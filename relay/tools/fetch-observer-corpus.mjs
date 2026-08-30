// Pulls ONE OBSERVER'S TRUST CHAIN off a live relay, for `ObserverTrustListIT`.
//
// Node 21+, no dependencies, same reasons as `fetch-corpus.mjs` beside it: a
// fixture that needs an npm install is a fixture nobody runs, and browser code
// this is not. Writes `corpus.jsonl` plus a `scenario.json` naming what the
// test should expect to find in it.
//
// WHAT A TRUST CHAIN IS, and why the fetch has to walk it rather than sweep:
// the whole question this corpus answers is whether a Trusted List unpacks FOR
// A READER WHO ASKED FOR IT, so every event has to be reachable from that one
// reader's kind-10040 by the same steps the relay itself takes.
//
//   1. the observer's Treasure Map (10040)
//   2. the publishers it delegates, per kind and in BOTH shapes — NIP-85's
//      `<kind>:<metric>` and the Tapestry ADR's generic bare `<kind>`
//   3. those publishers' declarations, per delegated kind
//   4. the kind-0 of every member those declarations point at
//
// Sweeping the kinds instead would fetch lists nobody asked for, which is the
// one thing the gate exists to refuse — and a corpus that cannot tell the two
// apart cannot test it.
//
//   node relay/tools/fetch-observer-corpus.mjs /tmp/obs <observer-hex> [relay]
//   ./gradlew :relay:test --tests '*ObserverTrustListIT*' \
//       -DitVespa=http://localhost:8080 -DitCorpus=/tmp/obs
import { mkdirSync, writeFileSync } from "node:fs";

const DIR = process.argv[2] || "/tmp/obs";
const OBSERVER = process.argv[3];
const RELAY = process.argv[4] || "wss://search-staging.brainstorm.world/";
if (!OBSERVER || !/^[0-9a-f]{64}$/.test(OBSERVER)) {
  console.error("usage: fetch-observer-corpus.mjs <dir> <observer-hex> [relay]");
  process.exit(1);
}

// The search relay requires an anonymous read to declare a lens
// (LensRequiredPolicy), and `include:spam` is the waiver — the whole corpus,
// ranked by nobody. Reading a reader's own statement of whom they trust
// THROUGH that trust would be circular, which is the same reason the relay's
// own enrolment lookup declares it.
const SPAM = "include:spam";

const ask = (filters, ms = 45_000) =>
  new Promise((resolve, reject) => {
    const ws = new WebSocket(RELAY);
    const out = [];
    let done = false;
    const fin = (err) => {
      if (done) return;
      done = true;
      clearTimeout(t);
      try { ws.close(); } catch {}
      err ? reject(err) : resolve(out);
    };
    const t = setTimeout(() => fin(), ms);
    ws.onopen = () => ws.send(JSON.stringify(["REQ", "fetch", ...filters]));
    ws.onerror = () => fin(new Error(`cannot reach ${RELAY}`));
    ws.onmessage = (e) => {
      const m = JSON.parse(e.data);
      if (m[0] === "EVENT") out.push(m[2]);
      else if (m[0] === "EOSE") fin();
      else if (m[0] === "CLOSED") fin(new Error(`CLOSED ${m[2]}`));
    };
  });

/** Chunked so one `authors` list never grows past what a relay will answer. */
const askChunked = async (keys, build, size = 200) => {
  const out = [];
  for (let i = 0; i < keys.length; i += size) out.push(...(await ask([build(keys.slice(i, i + size))])));
  return out;
};

const LIST_KINDS = [30392, 30393, 30394, 30395];
const ASSERTION_KINDS = [30382, 30383, 30384, 30385];

// 1. The Map. Kind 10040 is REPLACEABLE, so a relay hands back the one version
//    it kept — which is the one its own gate will read, and therefore the one
//    this corpus wants.
const maps = await ask([{ kinds: [10040], authors: [OBSERVER], search: SPAM }]);
if (maps.length === 0) {
  console.error(`no kind-10040 for ${OBSERVER.slice(0, 12)}… on ${RELAY} — nothing to walk`);
  process.exit(2);
}
const map = maps.sort((a, b) => b.created_at - a.created_at)[0];

// 2. Both delegation shapes, kept apart by the kind each entry names. The bare
//    `["30392", pk, relay]` form is the ADR's, one entry per kind; the
//    `["30382:rank", pk, relay]` form is NIP-85's, one per kind AND metric.
const delegated = new Map(); // kind -> Set(pubkey)
for (const tag of map.tags) {
  if (!Array.isArray(tag) || tag.length < 2 || !/^[0-9a-f]{64}$/.test(tag[1])) continue;
  const head = String(tag[0]);
  const kind = Number.parseInt(head.includes(":") ? head.slice(0, head.indexOf(":")) : head, 10);
  if (!Number.isInteger(kind)) continue;
  const named = head.includes(":");
  // A named `3039x:<name>` entry is RESERVED by the ADR and drives nothing —
  // the relay's gate refuses it, so a corpus built to test that gate must not
  // quietly follow it either.
  if (LIST_KINDS.includes(kind) && named) continue;
  if (!LIST_KINDS.includes(kind) && !ASSERTION_KINDS.includes(kind)) continue;
  if (!delegated.has(kind)) delegated.set(kind, new Set());
  delegated.get(kind).add(tag[1]);
}

// 3. Each publisher's declarations, asked for the kind it was delegated and no
//    other — the same scoping the gate applies on the way back out.
const declarations = [];
for (const [kind, keys] of delegated) {
  const rows = await ask([{ kinds: [kind], authors: [...keys], search: SPAM, limit: 200 }]);
  declarations.push(...rows);
  console.error(`kind ${kind}: ${rows.length} from ${keys.size} publisher(s)`);
}

// 4. Whom they point at. Only the pubkey members here: `e`/`a` members would
//    need their own events and this corpus is about the pubkey families.
const subjects = new Set();
for (const d of declarations) {
  const dTag = (d.tags.find((t) => t[0] === "d") || [])[1];
  if (d.kind === 30382 && /^[0-9a-f]{64}$/.test(dTag ?? "")) subjects.add(dTag);
  if (d.kind === 30392) for (const t of d.tags) if (t[0] === "p" && /^[0-9a-f]{64}$/.test(t[1] ?? "")) subjects.add(t[1]);
}
const profiles = await askChunked([...subjects], (authors) => ({ kinds: [0], authors, search: SPAM }));
console.error(`subjects: ${subjects.size} named, ${profiles.length} with a profile`);

const seen = new Set();
const corpus = [map, ...declarations, ...profiles].filter((e) => !seen.has(e.id) && seen.add(e.id));

mkdirSync(DIR, { recursive: true });
writeFileSync(`${DIR}/corpus.jsonl`, corpus.map((e) => JSON.stringify(e)).join("\n") + "\n");
writeFileSync(
  `${DIR}/scenario.json`,
  JSON.stringify(
    {
      relay: RELAY,
      observer: OBSERVER,
      map: map.id,
      delegated: [...delegated].map(([kind, keys]) => ({ kind, publishers: [...keys] })),
      declarations: declarations.length,
      profiles: profiles.length,
    },
    null,
    2,
  ) + "\n",
);
console.error(`wrote ${corpus.length} events to ${DIR}/corpus.jsonl`);
