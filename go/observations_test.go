package main

// Tests for the observation tally: the natural/injected split, the blind spot
// it exists to expose, the bounded ring, and the snapshot round trip.
//
// The split is the part worth testing hardest. If injected observations leak
// into the natural counts, the dashboard reports a degradation rate computed
// over a sample the plugin itself selected -- which is the specific way this
// feature can lie to its operator.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func resetObservations(t *testing.T, dir string) pluginConfig {
	t.Helper()
	observations.mu.Lock()
	observations.byKey = make(map[string]*bucketObservation)
	observations.recent = nil
	observations.since = time.Now()
	observations.dirty = false
	observations.lastOut = time.Now()
	observations.writing = false
	observations.dir = dir
	observations.mu.Unlock()
	return pluginConfig{TemplateLength: 292, ReplaceLength: 312}
}

func observedBucket(t *testing.T, auth, model string) bucketObservation {
	t.Helper()
	buckets, _, _ := observationsSnapshot()
	for _, cell := range buckets {
		if cell.AuthID == auth && cell.Model == model {
			return cell
		}
	}
	t.Fatalf("no observation cell for (%s, %s)", auth, model)
	return bucketObservation{}
}

// The four readings the dashboard renders, each landing in its own counter.
func TestObservationSplitsNaturalFromInjected(t *testing.T) {
	cfg := resetObservations(t, "")

	recordObservation(cfg, "a.json", "gpt-5.5", 292, false) // unprompted, good
	recordObservation(cfg, "a.json", "gpt-5.5", 312, false) // unprompted, degraded
	recordObservation(cfg, "a.json", "gpt-5.5", 0, true)    // ours accepted
	recordObservation(cfg, "a.json", "gpt-5.5", 312, true)  // degraded despite ours
	recordObservation(cfg, "a.json", "gpt-5.5", 292, true)  // fresh good despite ours
	recordObservation(cfg, "a.json", "gpt-5.5", 99, false)  // unrecognised length

	cell := observedBucket(t, "a.json", "gpt-5.5")
	for _, want := range []struct {
		name string
		got  int64
		n    int64
	}{
		{"NaturalNormal", cell.NaturalNormal, 1},
		{"NaturalLimited", cell.NaturalLimited, 1},
		{"NaturalOther", cell.NaturalOther, 1},
		{"InjectedSilent", cell.InjectedSilent, 1},
		{"InjectedLimited", cell.InjectedLimited, 1},
		{"InjectedNormal", cell.InjectedNormal, 1},
	} {
		if want.got != want.n {
			t.Errorf("%s = %d, want %d", want.name, want.got, want.n)
		}
	}
}

// Silence with nothing injected is the majority of traffic and says nothing
// about serving state. Recording it would bury the readings that matter.
func TestObservationIgnoresUninterestingSilence(t *testing.T) {
	cfg := resetObservations(t, "")

	recordObservation(cfg, "a.json", "gpt-5.5", 0, false)
	if buckets, recent, _ := observationsSnapshot(); len(buckets) != 0 || len(recent) != 0 {
		t.Errorf("a silent, untouched response was recorded: %d bucket(s), %d event(s)", len(buckets), len(recent))
	}

	// But silence AFTER we injected is the signal that the template was taken.
	recordObservation(cfg, "a.json", "gpt-5.5", 0, true)
	if cell := observedBucket(t, "a.json", "gpt-5.5"); cell.InjectedSilent != 1 {
		t.Errorf("InjectedSilent = %d, want 1: silence after an injection is how acceptance is seen", cell.InjectedSilent)
	}
}

// An observation with no account cannot be filed. Bucketing it anywhere would
// put one customer's throttling on another customer's row.
func TestObservationRequiresAFullKey(t *testing.T) {
	cfg := resetObservations(t, "")

	recordObservation(cfg, "", "gpt-5.5", 312, false)
	recordObservation(cfg, "a.json", "", 312, false)
	recordObservation(cfg, "  ", "  ", 312, true)

	if buckets, _, _ := observationsSnapshot(); len(buckets) != 0 {
		t.Errorf("recorded %d cell(s) from observations with an incomplete key", len(buckets))
	}
}

// The blind spot, asserted directly. While a bucket holds a template every
// request is injected and the upstream signs nothing, so LastNatural stops
// advancing even though LastAt keeps moving. The dashboard ages the natural
// reading off these two fields; if injected traffic refreshed LastNatural,
// an hour-old "normal" would render as current.
func TestInjectedObservationsDoNotRefreshTheNaturalReading(t *testing.T) {
	cfg := resetObservations(t, "")

	recordObservation(cfg, "a.json", "gpt-5.5", 292, false)
	first := observedBucket(t, "a.json", "gpt-5.5")
	if first.LastNaturalKind != observationNormal || first.LastNaturalAt == "" {
		t.Fatalf("natural reading not recorded: kind=%q at=%q", first.LastNaturalKind, first.LastNaturalAt)
	}

	time.Sleep(1100 * time.Millisecond) // RFC3339 is second-resolution
	recordObservation(cfg, "a.json", "gpt-5.5", 0, true)
	recordObservation(cfg, "a.json", "gpt-5.5", 0, true)

	after := observedBucket(t, "a.json", "gpt-5.5")
	if after.LastNaturalAt != first.LastNaturalAt {
		t.Errorf("LastNaturalAt moved from %q to %q on injected traffic; the blind spot would be invisible",
			first.LastNaturalAt, after.LastNaturalAt)
	}
	if after.LastAt == first.LastAt {
		t.Error("LastAt did not move, so the two timestamps cannot be told apart")
	}
	if after.LastKind != observationSilent || !after.LastWrote {
		t.Errorf("last observation = (%s, wrote=%v), want (silent, true)", after.LastKind, after.LastWrote)
	}
}

// A 312 arriving while we hold a template is the one alarm here: the bucket
// cannot be rescued by what this plugin does.
func TestInjectedLimitedIsCountedSeparately(t *testing.T) {
	cfg := resetObservations(t, "")

	recordObservation(cfg, "a.json", "gpt-5.6-sol", 312, true)
	cell := observedBucket(t, "a.json", "gpt-5.6-sol")
	if cell.InjectedLimited != 1 {
		t.Errorf("InjectedLimited = %d, want 1", cell.InjectedLimited)
	}
	if cell.NaturalLimited != 0 {
		t.Errorf("NaturalLimited = %d, want 0: this one was prompted and must not enter the natural rate", cell.NaturalLimited)
	}
}

func TestObservationFeedIsBoundedAndNewestFirst(t *testing.T) {
	cfg := resetObservations(t, "")

	for i := 0; i < observationsRecentMax+40; i++ {
		length := 292
		if i%2 == 0 {
			length = 312
		}
		recordObservation(cfg, "a.json", "gpt-5.5", length, false)
	}

	_, recent, _ := observationsSnapshot()
	if len(recent) != observationsRecentMax {
		t.Fatalf("feed holds %d events, want the cap of %d", len(recent), observationsRecentMax)
	}
	// Last one in was i = max+39, which is odd, so a 292.
	if recent[0].Len != 292 {
		t.Errorf("feed[0].Len = %d, want the most recent event (292)", recent[0].Len)
	}
	if recent[0].Kind != observationNormal || recent[0].Wrote {
		t.Errorf("feed[0] = (%s, wrote=%v), want (normal, false)", recent[0].Kind, recent[0].Wrote)
	}
}

func TestObservationSnapshotRoundTrips(t *testing.T) {
	dir := t.TempDir()
	cfg := resetObservations(t, dir)

	recordObservation(cfg, "a.json", "gpt-5.5", 292, false)
	recordObservation(cfg, "b.json", "gpt-5.6-sol", 312, true)
	flushObservationsNow()

	path := filepath.Join(dir, observationsFileName)
	raw, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("no snapshot written: %v", errRead)
	}
	var snap observationSnapshot
	if errUnmarshal := json.Unmarshal(raw, &snap); errUnmarshal != nil {
		t.Fatalf("snapshot is not valid JSON: %v", errUnmarshal)
	}
	if snap.Version != observationsVersion {
		t.Errorf("snapshot version = %d, want %d", snap.Version, observationsVersion)
	}
	if len(snap.Buckets) != 2 || len(snap.Recent) != 2 {
		t.Fatalf("snapshot holds %d bucket(s) and %d event(s), want 2 and 2", len(snap.Buckets), len(snap.Recent))
	}

	// Reload into a cleared tally: the counts must come back.
	observations.mu.Lock()
	observations.dir = ""
	observations.since = time.Time{}
	observations.mu.Unlock()
	loadObservations(dir)

	if cell := observedBucket(t, "b.json", "gpt-5.6-sol"); cell.InjectedLimited != 1 {
		t.Errorf("after reload InjectedLimited = %d, want 1", cell.InjectedLimited)
	}
	if cell := observedBucket(t, "a.json", "gpt-5.5"); cell.NaturalNormal != 1 {
		t.Errorf("after reload NaturalNormal = %d, want 1", cell.NaturalNormal)
	}
}

// A snapshot from a future or unknown format is dropped, not guessed at. There
// is deliberately no migration path: these are discardable counts.
func TestObservationSnapshotRejectsForeignVersion(t *testing.T) {
	dir := t.TempDir()
	resetObservations(t, dir)

	raw, _ := json.Marshal(observationSnapshot{
		Version: observationsVersion + 1,
		Buckets: []bucketObservation{{AuthID: "a.json", Model: "gpt-5.5", NaturalLimited: 99}},
	})
	if errWrite := os.WriteFile(filepath.Join(dir, observationsFileName), raw, 0o600); errWrite != nil {
		t.Fatalf("seed snapshot: %v", errWrite)
	}

	observations.mu.Lock()
	observations.dir = ""
	observations.since = time.Time{}
	observations.mu.Unlock()
	loadObservations(dir)

	if buckets, _, _ := observationsSnapshot(); len(buckets) != 0 {
		t.Errorf("loaded %d cell(s) from a version-%d snapshot", len(buckets), observationsVersion+1)
	}
}

// A malformed model id must not be able to grow the map without bound.
func TestObservationBucketsAreCapped(t *testing.T) {
	cfg := resetObservations(t, "")

	for i := 0; i < observationsBucketMax+20; i++ {
		recordObservation(cfg, "a.json", "model-"+string(rune('a'+i%26))+string(rune('a'+i/26)), 312, false)
	}
	buckets, _, _ := observationsSnapshot()
	if len(buckets) > observationsBucketMax {
		t.Errorf("tally holds %d cells, above the cap of %d", len(buckets), observationsBucketMax)
	}
}

// The observation is taken before the harvest path's own checks, so it sees
// responses the harvester discards -- a 312 above all, which is the reading
// that matters most and is never stored as a template.
func TestHarvestPathRecordsTheDegradedObservation(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))
	resetHarvestState(t)
	resetObservations(t, "")

	state.mu.Lock()
	cfg := state.config
	state.mu.Unlock()

	issued := wallClock().Add(-time.Minute)
	meta := map[string]any{testAuthKey: "codex-alpha.json"}
	harvestFromResponse(cfg, harvestResponseHeaders(fakeToken(312, issued)), meta, "gpt-5.5", "")

	cell := observedBucket(t, "codex-alpha.json", "gpt-5.5")
	if cell.NaturalLimited != 1 {
		t.Errorf("NaturalLimited = %d, want 1: the 312 was seen even though it was never stored", cell.NaturalLimited)
	}
	if cell.LastNaturalKind != observationLimited {
		t.Errorf("LastNaturalKind = %q, want %q", cell.LastNaturalKind, observationLimited)
	}
}

// dry_run exists to watch without touching anything. If a dry-run decision
// counted as an injection, every observation taken in that mode would be filed
// under the wrong half of the split.
func TestDryRunDecisionIsNotCountedAsAnInjection(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))
	resetObservations(t, "")

	rememberRequestAuth("req-dry", "codex-alpha.json")
	// markRequestWrote is what interceptAfterAuth calls only past the dry_run
	// check; a dry run reaches its return without calling it.
	authID, wrote := recallRequestRecord("req-dry")
	if authID != "codex-alpha.json" {
		t.Fatalf("recall = %q, want the recorded account", authID)
	}
	if wrote {
		t.Fatal("a request that was never marked reports as injected")
	}

	state.mu.Lock()
	cfg := state.config
	state.mu.Unlock()
	recordObservation(cfg, authID, "gpt-5.5", 312, wrote)

	cell := observedBucket(t, "codex-alpha.json", "gpt-5.5")
	if cell.NaturalLimited != 1 || cell.InjectedLimited != 0 {
		t.Errorf("natural=%d injected=%d, want 1 and 0", cell.NaturalLimited, cell.InjectedLimited)
	}
}
