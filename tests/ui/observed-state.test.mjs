// Unit test for observedState, the one piece of dashboard logic that can lie.
//
// Everything else in ui.html renders a number the server sent. This function
// decides what a cell CLAIMS about the upstream right now, from observations
// that may be stale or may not exist -- and the wrong answer here is not a
// cosmetic bug, it is the page telling an operator "normal" about a bucket it
// has been blind to for the last hour.
//
// It is a pure function, so it can be tested without a DOM. The source is
// sliced out of ui.html between the section marker and renderMatrix rather
// than duplicated, so this cannot silently drift from what ships.
//
//   node tests/ui/observed-state.test.mjs

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const html = readFileSync(join(here, '..', '..', 'go', 'ui.html'), 'utf8');

const START = '/* ---------------------------- 服务态 ---------------------------- */';
const END = 'function renderMatrix(';
const from = html.indexOf(START);
const to = html.indexOf(END, from);
if (from < 0 || to < 0) {
  throw new Error('could not find the 服务态 section in ui.html; the markers moved');
}
const source = html.slice(from, to);
for (const name of ['OBS_UNKNOWN_MS', 'OBS_STALE_MS', 'fmtAgo', 'observedState']) {
  if (!source.includes(name)) throw new Error(`sliced section is missing ${name}`);
}
const observedState = new Function(source + '\nreturn observedState;')();

const MIN = 60 * 1000;
const ago = (m) => new Date(Date.now() - m * MIN).toISOString();

let failures = 0;
function check(name, got, wantCls, wantLabelPart) {
  const ok = got.cls === wantCls && (!wantLabelPart || got.label.includes(wantLabelPart));
  if (!ok) {
    failures++;
    console.error(`FAIL ${name}\n  got  cls=${got.cls} label=${JSON.stringify(got.label)}\n  want cls=${wantCls}${wantLabelPart ? ` label~${JSON.stringify(wantLabelPart)}` : ''}`);
  } else {
    console.log(`ok   ${name}`);
  }
}

// No observations at all is NOT normal. A bucket nobody has sent traffic to
// must never render green.
check('no observations', observedState({ ready: true }), 'obs-none', '无观测');
check('observed absent but ready', observedState({ ready: true, observed: null }), 'obs-none', '无观测');

// Older than the unknown window: the bucket is idle, not healthy.
check('idle for three hours', observedState({
  ready: true,
  observed: { last_at: ago(190), last_kind: 'normal', last_natural_kind: 'normal', last_natural_at: ago(190) },
}), 'obs-none', '未知');

// The alarm outranks everything: we supplied a valid template and were
// degraded anyway.
check('injected and still limited', observedState({
  ready: true,
  observed: {
    last_at: ago(0), last_kind: 'limited', last_wrote: true, injected_limited: 97,
    last_natural_kind: 'normal', last_natural_at: ago(1),
  },
}), 'obs-alarm', '模板失效');

// A fresh unprompted reading is the only thing that can speak for right now.
check('fresh natural normal', observedState({
  ready: true,
  observed: { last_at: ago(2), last_kind: 'normal', last_wrote: false, natural_normal: 161, last_natural_kind: 'normal', last_natural_at: ago(2) },
}), 'obs-ok', '正常 (292)');

check('fresh natural limited', observedState({
  ready: false,
  observed: { last_at: ago(1), last_kind: 'limited', last_wrote: false, natural_limited: 1076, last_natural_kind: 'limited', last_natural_at: ago(1) },
}), 'obs-bad', '受限 (312)');

// THE regression this file exists for. A bucket holding a template is injected
// on every request, so the upstream signs nothing and the last natural reading
// ages out. It must read as blind, never as the hour-old "normal".
check('blind while injecting', observedState({
  ready: true,
  observed: {
    last_at: ago(0), last_kind: 'silent', last_wrote: true, injected_silent: 806,
    natural_normal: 12, last_natural_kind: 'normal', last_natural_at: ago(47),
  },
}), 'obs-blind', '盲区');

// Empty bucket, stale natural reading: report it, but say it is stale. Not
// blind -- nothing is being injected, so the silence is not our doing.
check('stale with an empty bucket', observedState({
  ready: false,
  observed: {
    last_at: ago(38), last_kind: 'limited', last_wrote: false,
    natural_limited: 11, last_natural_kind: 'limited', last_natural_at: ago(38),
  },
}), 'obs-blind', '陈旧');

// The sample size rides along, because 3 observations and 4237 observations
// are not the same claim.
const withN = observedState({
  ready: true,
  observed: { last_at: ago(1), last_kind: 'normal', last_wrote: false, natural_normal: 40, natural_limited: 2, last_natural_kind: 'normal', last_natural_at: ago(1) },
});
if (!withN.label.includes('n=42')) {
  failures++;
  console.error(`FAIL sample size in label\n  got ${JSON.stringify(withN.label)}, want it to carry n=42`);
} else {
  console.log('ok   sample size in label');
}

// Every state must carry an explanation; a bare colour is not a finding.
for (const [name, cell] of [
  ['none', { ready: true }],
  ['alarm', { ready: true, observed: { last_at: ago(0), last_kind: 'limited', last_wrote: true } }],
  ['blind', { ready: true, observed: { last_at: ago(0), last_kind: 'silent', last_wrote: true, last_natural_at: ago(47), last_natural_kind: 'normal' } }],
]) {
  const got = observedState(cell);
  if (!got.title || got.title.length < 20) {
    failures++;
    console.error(`FAIL ${name} has no usable tooltip: ${JSON.stringify(got.title)}`);
  }
}

console.log(failures ? `\n${failures} failure(s)` : '\nall observedState checks passed');
process.exit(failures ? 1 : 0);
