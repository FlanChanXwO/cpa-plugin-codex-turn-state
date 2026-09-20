// Observation domain: what serving state the upstream actually handed back,
// per (account, model), and whether we had injected a template on that request.
//
// This exists because the plugin is the only thing that sees both halves. CPA's
// request logs record the response header but not which credential was chosen;
// the plugin knows the credential AND what it did to the outgoing request. The
// pairing is the whole point -- see observationKind below for why an
// unpaired count would mislead.
//
// Nothing here changes a request or a response. It is pure observation of
// traffic that was happening anyway, which is what makes it safe to run
// permanently while active probing stays off.
//
// It deliberately does NOT keep a time series. Counts are lifetime-since-Since,
// plus a bounded ring of recent events. Trends belong in the decision log,
// which any cron can roll up at no storage cost; a plugin that grows a metrics
// database has stopped being a plugin.

package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// observationsFileName sits at the top of store_dir, beside index.json and
	// runtime.json. scanStoreRecords only reads <auth_id>/<model>.json under the
	// account subdirectories, so a top-level file cannot be mistaken for a
	// bucket.
	observationsFileName = "observations.json"

	// observationsVersion guards the snapshot format. On a mismatch the file is
	// ignored and collection restarts from empty -- deliberately, and there will
	// never be migration code here. This is a discardable observation snapshot,
	// not contract data: the cost of dropping it is losing counts, and the cost
	// of carrying migrations for it forever is higher.
	observationsVersion = 1

	// observationsRecentMax bounds the live feed. It rides on the status
	// document, which the dashboard polls, so this is also a bound on that
	// document's size (~12 KB at 100).
	observationsRecentMax = 100

	// observationsBucketMax bounds the tally. A malformed or hostile model id
	// would otherwise grow the map without limit; past this the least recently
	// seen bucket is dropped.
	observationsBucketMax = 256
)

// observationsFlushInterval is the floor between disk writes. The snapshot is
// whole-state rather than append-only, so writing more often buys nothing but a
// shorter loss window on a hard kill; a crash loses at most this much.
//
// A var rather than a const so the tests can force a flush instead of waiting a
// minute. Nothing in production reassigns it.
var observationsFlushInterval = 60 * time.Second

// The three things the upstream can do with the turn-state on a response.
// Deliberately about the UPSTREAM's act, not about our interpretation of it:
// "limited" says a 312 was signed, nothing about model quality. See the
// dashboard copy for the operational reading laid on top.
const (
	observationNormal  = "normal"  // template_length: a reusable state was signed
	observationLimited = "limited" // replace_length: a degraded state was signed
	observationSilent  = "silent"  // no state signed at all
	observationOther   = "other"   // some other length; worth seeing, never stored
)

// bucketObservation is one (account, model) cell.
//
// Counts are split by whether WE had written a template onto the request,
// because a combined rate is not a degradation rate and reporting one would
// mislead. The mechanism: once a bucket holds a live template the request hook
// injects it, the upstream accepts it and signs nothing new, and this side goes
// quiet for up to a full TTL. So a healthy bucket is SILENT, and a throttled
// bucket -- whose 312s never become templates, leaving it permanently empty --
// is observed on every single request. Counting them together makes the
// throttled bucket look like the whole story and the healthy one look absent.
//
// Read them this way:
//
//	NaturalNormal    upstream signed a good state, unprompted -- it is serving us
//	NaturalLimited   upstream signed a degraded state, unprompted -- throttled
//	InjectedSilent   we supplied a template and the upstream signed nothing:
//	                 it accepted ours. Healthy, but inferred rather than seen.
//	InjectedLimited  we supplied a valid template and it degraded us anyway.
//	                 The one actionable alarm here: this bucket cannot be
//	                 rescued by the thing this plugin does.
//	InjectedNormal   we injected and it signed a fresh good state regardless.
type bucketObservation struct {
	AuthID string `json:"auth_id"`
	Model  string `json:"model"`

	NaturalNormal  int64 `json:"natural_normal"`
	NaturalLimited int64 `json:"natural_limited"`
	NaturalOther   int64 `json:"natural_other"`

	InjectedSilent  int64 `json:"injected_silent"`
	InjectedLimited int64 `json:"injected_limited"`
	InjectedNormal  int64 `json:"injected_normal"`

	// Last* describe the most recent observation of any kind.
	LastKind  string `json:"last_kind"`
	LastLen   int    `json:"last_len"`
	LastWrote bool   `json:"last_wrote"`
	LastAt    string `json:"last_at"`

	// LastNatural* describe the most recent observation we did NOT prompt. This
	// is the one the dashboard must age: a natural reading from an hour ago is
	// not evidence about now, and presenting it as current state is exactly the
	// blind spot this split exists to expose.
	LastNaturalKind string `json:"last_natural_kind,omitempty"`
	LastNaturalAt   string `json:"last_natural_at,omitempty"`
}

// observationEvent is one row of the live feed. Every field is structured --
// no free text. The status document is anonymously readable and a free-text
// channel on it is a leak waiting to be written; see probe_run.lines for the
// one that already exists and the constraint it carries.
type observationEvent struct {
	At     string `json:"at"`
	AuthID string `json:"auth_id"`
	Model  string `json:"model"`
	Len    int    `json:"len"`
	Wrote  bool   `json:"wrote"`
	Kind   string `json:"kind"`
}

// observationSnapshot is the whole persisted state, rewritten atomically. It is
// not appended to: a torn append would need recovery logic, and a 20 KB
// rewrite once a minute does not.
type observationSnapshot struct {
	Version   int                 `json:"version"`
	Since     string              `json:"since"`
	UpdatedAt string              `json:"updated_at"`
	Buckets   []bucketObservation `json:"buckets"`
	Recent    []observationEvent  `json:"recent"`
}

// observations holds the live tally.
//
// Its own mutex, deliberately NOT state.mu. handleStatus already avoids holding
// state.mu and the probe runner's lock at once, and adding a third lock under
// state.mu would reintroduce exactly that ordering hazard. Nothing in here ever
// takes state.mu, so it cannot participate in a cycle.
var observations = struct {
	mu      sync.Mutex
	since   time.Time
	byKey   map[string]*bucketObservation
	recent  []observationEvent
	dirty   bool
	lastOut time.Time // last successful flush
	writing bool      // a flush goroutine is in flight
	dir     string    // store_dir this tally was loaded for
}{byKey: make(map[string]*bucketObservation)}

// classifyObservation maps a response's turn-state length onto what the
// upstream did. Lengths come from config because they are observations about
// the upstream rather than protocol constants -- see README.
func classifyObservation(cfg pluginConfig, valueLen int) string {
	switch valueLen {
	case 0:
		return observationSilent
	case cfg.TemplateLength:
		return observationNormal
	case cfg.ReplaceLength:
		return observationLimited
	default:
		return observationOther
	}
}

// recordObservation notes one response.
//
// wrote reports whether the request hook actually put a template on the way out
// -- not whether it wanted to. A dry_run decision is not a write, and counting
// it as one would make the whole injected/natural split meaningless in the mode
// an operator uses precisely to watch without touching anything.
//
// Called before the harvest path's own checks, so a response that carries no
// state at all still lands here: "we injected and the upstream then signed
// nothing" is the reading that tells us our template was accepted, and it is
// invisible if silence is not recorded.
func recordObservation(cfg pluginConfig, authID, model string, valueLen int, wrote bool) {
	authID = strings.TrimSpace(authID)
	model = strings.TrimSpace(model)
	if authID == "" || model == "" {
		// Unattributable. Counting it against some placeholder bucket would put
		// one account's throttling on another's row.
		return
	}
	kind := classifyObservation(cfg, valueLen)
	if kind == observationSilent && !wrote {
		// Neither side did anything: no template went out, none came back. Most
		// traffic looks like this and it says nothing about serving state.
		return
	}

	now := time.Now()
	key := bucketKey(authID, model)

	observations.mu.Lock()
	cell := observations.byKey[key]
	if cell == nil {
		evictObservationBucketLocked()
		cell = &bucketObservation{AuthID: authID, Model: model}
		observations.byKey[key] = cell
	}

	switch {
	case wrote && kind == observationSilent:
		cell.InjectedSilent++
	case wrote && kind == observationLimited:
		cell.InjectedLimited++
	case wrote && kind == observationNormal:
		cell.InjectedNormal++
	case !wrote && kind == observationNormal:
		cell.NaturalNormal++
	case !wrote && kind == observationLimited:
		cell.NaturalLimited++
	default:
		cell.NaturalOther++
	}

	cell.LastKind = kind
	cell.LastLen = valueLen
	cell.LastWrote = wrote
	cell.LastAt = now.UTC().Format(time.RFC3339)
	if !wrote && kind != observationSilent {
		cell.LastNaturalKind = kind
		cell.LastNaturalAt = cell.LastAt
	}

	observations.recent = append(observations.recent, observationEvent{
		At:     cell.LastAt,
		AuthID: authID,
		Model:  model,
		Len:    valueLen,
		Wrote:  wrote,
		Kind:   kind,
	})
	if len(observations.recent) > observationsRecentMax {
		observations.recent = observations.recent[len(observations.recent)-observationsRecentMax:]
	}
	observations.dirty = true
	dir := observations.dir
	due := now.Sub(observations.lastOut) >= observationsFlushInterval && !observations.writing
	if due {
		observations.writing = true
	}
	observations.mu.Unlock()

	if due && dir != "" {
		// Off the response path. Holding a hook open for a disk write would put
		// file latency on every upstream response.
		go flushObservations(dir)
	}
}

// evictObservationBucketLocked drops the least recently seen cell once the map
// is full. The caller holds observations.mu.
func evictObservationBucketLocked() {
	if len(observations.byKey) < observationsBucketMax {
		return
	}
	oldestKey, oldestAt := "", ""
	for key, cell := range observations.byKey {
		if oldestKey == "" || cell.LastAt < oldestAt {
			oldestKey, oldestAt = key, cell.LastAt
		}
	}
	if oldestKey != "" {
		delete(observations.byKey, oldestKey)
	}
}

// observationsSnapshot copies the tally out for the status document, sorted so
// the dashboard's rows do not reshuffle between polls.
func observationsSnapshot() ([]bucketObservation, []observationEvent, string) {
	observations.mu.Lock()
	defer observations.mu.Unlock()

	buckets := make([]bucketObservation, 0, len(observations.byKey))
	for _, cell := range observations.byKey {
		buckets = append(buckets, *cell)
	}
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].AuthID != buckets[j].AuthID {
			return buckets[i].AuthID < buckets[j].AuthID
		}
		return buckets[i].Model < buckets[j].Model
	})

	// Newest first: the feed is read top-down by someone asking "what just
	// happened", not "what happened first".
	recent := make([]observationEvent, 0, len(observations.recent))
	for i := len(observations.recent) - 1; i >= 0; i-- {
		recent = append(recent, observations.recent[i])
	}

	since := ""
	if !observations.since.IsZero() {
		since = observations.since.UTC().Format(time.RFC3339)
	}
	return buckets, recent, since
}

// flushObservations writes the snapshot. The copy happens under the lock and
// the write outside it, so a slow disk cannot stall a response hook.
func flushObservations(dir string) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return
	}

	observations.mu.Lock()
	if !observations.dirty {
		observations.writing = false
		observations.mu.Unlock()
		return
	}
	snap := observationSnapshot{
		Version:   observationsVersion,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		Recent:    append([]observationEvent(nil), observations.recent...),
	}
	if !observations.since.IsZero() {
		snap.Since = observations.since.UTC().Format(time.RFC3339)
	}
	for _, cell := range observations.byKey {
		snap.Buckets = append(snap.Buckets, *cell)
	}
	observations.dirty = false
	observations.mu.Unlock()

	sort.Slice(snap.Buckets, func(i, j int) bool {
		if snap.Buckets[i].AuthID != snap.Buckets[j].AuthID {
			return snap.Buckets[i].AuthID < snap.Buckets[j].AuthID
		}
		return snap.Buckets[i].Model < snap.Buckets[j].Model
	})

	data, errMarshal := json.MarshalIndent(snap, "", "  ")
	if errMarshal == nil {
		errWrite := atomicWrite(filepath.Join(dir, observationsFileName), append(data, '\n'))
		if errWrite != nil {
			// Not fatal and not retried: the tally lives in memory and the next
			// flush rewrites the whole thing anyway.
			log.Printf(logPrefix+"could not write %s: %v", observationsFileName, errWrite)
			observations.mu.Lock()
			observations.dirty = true
			observations.mu.Unlock()
		}
	}

	observations.mu.Lock()
	observations.lastOut = time.Now()
	observations.writing = false
	observations.mu.Unlock()
}

// loadObservations restores the tally for dir, or starts a fresh one. Called
// from configure, so a reconfigure that changes store_dir moves the tally with
// it rather than mixing two stores' counts.
func loadObservations(dir string) {
	dir = strings.TrimSpace(dir)

	observations.mu.Lock()
	defer observations.mu.Unlock()

	if observations.dir == dir && !observations.since.IsZero() {
		// Same store, already loaded. A reconfigure must not reset counts the
		// operator is watching.
		return
	}

	observations.dir = dir
	observations.byKey = make(map[string]*bucketObservation)
	observations.recent = nil
	observations.since = time.Now()
	observations.dirty = false
	// Start the clock now rather than at the zero time, so the first
	// observation after a load does not trigger an immediate write. Batching
	// from the first tick is the point: a restart should not cost a disk write
	// per response until the interval catches up.
	observations.lastOut = time.Now()

	if dir == "" {
		return
	}

	raw, errRead := os.ReadFile(filepath.Join(dir, observationsFileName))
	if errRead != nil || len(raw) == 0 {
		// Absent on first run, and that is the normal case -- not worth a log
		// line every time a fresh store_dir is configured.
		return
	}
	var snap observationSnapshot
	if errUnmarshal := json.Unmarshal(raw, &snap); errUnmarshal != nil {
		log.Printf(logPrefix+"%s is unreadable, observation counts restart from empty: %v", observationsFileName, errUnmarshal)
		return
	}
	if snap.Version != observationsVersion {
		log.Printf(logPrefix+"%s is version %d, want %d; observation counts restart from empty",
			observationsFileName, snap.Version, observationsVersion)
		return
	}
	for i := range snap.Buckets {
		cell := snap.Buckets[i]
		if strings.TrimSpace(cell.AuthID) == "" || strings.TrimSpace(cell.Model) == "" {
			continue
		}
		observations.byKey[bucketKey(cell.AuthID, cell.Model)] = &cell
	}
	if len(snap.Recent) > observationsRecentMax {
		snap.Recent = snap.Recent[len(snap.Recent)-observationsRecentMax:]
	}
	observations.recent = snap.Recent
	if parsed, errParse := time.Parse(time.RFC3339, snap.Since); errParse == nil && !parsed.IsZero() {
		observations.since = parsed
	}
}

// flushObservationsNow writes synchronously, for shutdown and reconfigure where
// there is no later flush to rely on.
func flushObservationsNow() {
	observations.mu.Lock()
	dir := observations.dir
	observations.writing = true
	observations.mu.Unlock()
	flushObservations(dir)
}
