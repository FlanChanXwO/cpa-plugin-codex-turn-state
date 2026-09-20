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
// What it keeps is deliberately shallow: lifetime counts since Since, the same
// counts per hour for the last two days, and a bounded ring of recent events.
// The hourly ring exists because lifetime totals cannot answer "is this worse
// than yesterday", which is the actual question. Anything finer -- per-request
// history, retention past two days, arbitrary ranges -- belongs in the decision
// log, which any cron can roll up at no storage cost. A plugin that grows a
// metrics database has stopped being a plugin.

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
	//
	// 2: added the hourly history and split injected-other out of
	// natural_other, which changes what an existing natural_other means.
	observationsVersion = 2

	// observationsRecentMax bounds the live feed. It rides on the status
	// document, which the dashboard polls, so this is also a bound on that
	// document's size (~12 KB at 100).
	observationsRecentMax = 100

	// observationsBucketMax bounds the tally. A malformed or hostile model id
	// would otherwise grow the map without limit; past this the least recently
	// seen bucket is dropped.
	observationsBucketMax = 256

	// observationsHourlyMax bounds the history kept per bucket: two days, which
	// is enough to ask whether today is worse than yesterday and short enough
	// that the snapshot stays small. Only hours with traffic take a slot, so an
	// idle bucket costs nothing and the worst case is bounded by this times
	// observationsBucketMax.
	observationsHourlyMax = 48
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
//	InjectedOther    we injected and it signed something we do not recognise.
type bucketObservation struct {
	AuthID string `json:"auth_id"`
	Model  string `json:"model"`

	observationCounts

	// Last* describe the most recent observation of any kind, silent ones
	// included. So this answers "is traffic flowing through this bucket",
	// which is a different question from "has the upstream told us anything",
	// and the dashboard needs both to tell injection blindness apart from an
	// account nobody is using.
	LastKind  string `json:"last_kind"`
	LastLen   int    `json:"last_len"`
	LastWrote bool   `json:"last_wrote"`
	LastAt    string `json:"last_at"`

	// LastSigned* describe the most recent observation in which the upstream
	// actually put a state on the wire -- whether or not we had injected into
	// that request. This is what the dashboard ages.
	//
	// Not LastNatural*, and the difference is load bearing: a 292 signed on a
	// request we injected into is still the upstream saying it serves this
	// account normally. Ageing only the unprompted readings would file that
	// evidence away as "blind", which is the opposite of what it shows.
	LastSignedKind  string `json:"last_signed_kind,omitempty"`
	LastSignedAt    string `json:"last_signed_at,omitempty"`
	LastSignedWrote bool   `json:"last_signed_wrote,omitempty"`

	// LastNatural* narrow that to the unprompted readings. Kept because the
	// injected/natural split is the whole reason these counts mean anything,
	// and an operator reading one row has to know which side it came from.
	LastNaturalKind string `json:"last_natural_kind,omitempty"`
	LastNaturalAt   string `json:"last_natural_at,omitempty"`

	// Hourly is the history: one entry per hour that saw traffic, oldest
	// first, capped at observationsHourlyMax. An idle bucket carries none.
	Hourly []hourlyObservation `json:"hourly,omitempty"`
}

// observationCounts is the seven-way split of what happened, used for the
// lifetime tally and for each hour of history alike.
//
// One type and one add() for both, because the alternative -- two switches
// over the same cases -- fails by having a new kind wired into one and not the
// other, and that shows up only as history that quietly disagrees with the
// total. Embedded untagged, so these serialise flat: a caller reads
// observed.natural_normal, not observed.counts.natural_normal.
type observationCounts struct {
	NaturalNormal  int64 `json:"natural_normal"`
	NaturalLimited int64 `json:"natural_limited"`
	NaturalOther   int64 `json:"natural_other"`

	InjectedSilent  int64 `json:"injected_silent"`
	InjectedLimited int64 `json:"injected_limited"`
	InjectedNormal  int64 `json:"injected_normal"`
	InjectedOther   int64 `json:"injected_other"`
}

// add books one observation.
//
// The (silent, not-injected) pair never arrives -- recordObservation drops it
// as noise before this is reached -- so the last case is (other, not
// injected). Every injected case is named explicitly rather than falling
// through, because "we injected and got back something unrecognised" filed
// under NaturalOther would put our own traffic on the unprompted side of the
// split and corrupt the only counts that can be read as a rate.
func (c *observationCounts) add(wrote bool, kind string) {
	switch {
	case wrote && kind == observationSilent:
		c.InjectedSilent++
	case wrote && kind == observationLimited:
		c.InjectedLimited++
	case wrote && kind == observationNormal:
		c.InjectedNormal++
	case wrote:
		c.InjectedOther++
	case kind == observationNormal:
		c.NaturalNormal++
	case kind == observationLimited:
		c.NaturalLimited++
	default:
		c.NaturalOther++
	}
}

// addAll sums another set in. TestObservationCountsAddAllCoversEveryField walks
// the type to prove no counter is missed here.
func (c *observationCounts) addAll(o observationCounts) {
	c.NaturalNormal += o.NaturalNormal
	c.NaturalLimited += o.NaturalLimited
	c.NaturalOther += o.NaturalOther
	c.InjectedSilent += o.InjectedSilent
	c.InjectedLimited += o.InjectedLimited
	c.InjectedNormal += o.InjectedNormal
	c.InjectedOther += o.InjectedOther
}

// hourlyObservation is one hour of the same counts.
//
// The lifetime totals answer "how many since we started", which is the wrong
// shape for the question an operator actually has -- is this worse than it was
// yesterday. Only hours with traffic get an entry.
type hourlyObservation struct {
	Hour string `json:"hour"` // RFC3339, truncated to the hour, UTC
	observationCounts
}

// hourSlot returns the counters for now's hour, appending a slot if this is
// the first observation in it and dropping the oldest once the ring is full.
//
// The returned pointer aims into the slice, so it is only valid until the next
// append. Every caller uses it immediately, under the lock.
func (b *bucketObservation) hourSlot(now time.Time) *observationCounts {
	hour := now.UTC().Truncate(time.Hour).Format(time.RFC3339)

	// Observations arrive in time order, so the current hour is the last entry
	// essentially always. The scan behind it covers a clock stepping backwards,
	// which would otherwise open a second slot for an hour already present.
	for i := len(b.Hourly) - 1; i >= 0; i-- {
		if b.Hourly[i].Hour == hour {
			return &b.Hourly[i].observationCounts
		}
	}

	b.Hourly = append(b.Hourly, hourlyObservation{Hour: hour})
	if len(b.Hourly) > observationsHourlyMax {
		b.Hourly = b.Hourly[len(b.Hourly)-observationsHourlyMax:]
	}
	return &b.Hourly[len(b.Hourly)-1].observationCounts
}

// rollup sums the hours falling inside window. Hours are whole, so a 24h
// window covers the last 24 hour-slots rather than exactly 24 hours -- close
// enough for "is today worse than yesterday", and the alternative is keeping
// per-request timestamps this deliberately does not keep.
func (b bucketObservation) rollup(now time.Time, window time.Duration) observationCounts {
	cutoff := now.UTC().Add(-window)
	var out observationCounts
	for _, h := range b.Hourly {
		at, err := time.Parse(time.RFC3339, h.Hour)
		if err != nil || at.Before(cutoff) {
			continue
		}
		out.addAll(h.observationCounts)
	}
	return out
}

// observationSummary is a bucketObservation stripped of the key fields, for
// hanging off a status row that already carries auth_id and model. Repeating
// them there would put two sources of truth for the same key on one published
// document.
type observationSummary struct {
	observationCounts

	LastKind  string `json:"last_kind"`
	LastLen   int    `json:"last_len"`
	LastWrote bool   `json:"last_wrote"`
	LastAt    string `json:"last_at"`

	LastSignedKind  string `json:"last_signed_kind,omitempty"`
	LastSignedAt    string `json:"last_signed_at,omitempty"`
	LastSignedWrote bool   `json:"last_signed_wrote,omitempty"`

	LastNaturalKind string `json:"last_natural_kind,omitempty"`
	LastNaturalAt   string `json:"last_natural_at,omitempty"`

	// Recent24h is the hourly history rolled into one figure per counter. The
	// lifetime totals above can only answer "how many since we started", and
	// an operator comparing today with yesterday cannot get there from a pair
	// of numbers that only ever grow.
	//
	// The hours themselves are not published. They are on disk for whoever
	// wants to chart them; putting 48 slots per bucket on a document the
	// dashboard polls would grow it by more than the panel can use.
	Recent24h observationCounts `json:"recent_24h"`
}

func (b bucketObservation) summary(now time.Time) observationSummary {
	return observationSummary{
		observationCounts: b.observationCounts,
		LastKind:          b.LastKind,
		LastLen:           b.LastLen,
		LastWrote:         b.LastWrote,
		LastAt:            b.LastAt,
		LastSignedKind:    b.LastSignedKind,
		LastSignedAt:      b.LastSignedAt,
		LastSignedWrote:   b.LastSignedWrote,
		LastNaturalKind:   b.LastNaturalKind,
		LastNaturalAt:     b.LastNaturalAt,
		Recent24h:         b.rollup(now, 24*time.Hour),
	}
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

	cell.observationCounts.add(wrote, kind)
	cell.hourSlot(now).add(wrote, kind)

	cell.LastKind = kind
	cell.LastLen = valueLen
	cell.LastWrote = wrote
	cell.LastAt = now.UTC().Format(time.RFC3339)

	// Anything that is not silence is the upstream telling us something, and it
	// counts as current evidence whether or not we had injected into that
	// request. Silence is the one reading that says nothing on its own -- under
	// injection it means our template was taken, which is not a state anyone
	// signed.
	if kind != observationSilent {
		cell.LastSignedKind = kind
		cell.LastSignedAt = cell.LastAt
		cell.LastSignedWrote = wrote
		if !wrote {
			cell.LastNaturalKind = kind
			cell.LastNaturalAt = cell.LastAt
		}
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
