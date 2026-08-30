// Pulls a PRODUCTION CORPUS off one or more real relays for `ProductionCorpusIT`.
//
// Node 21+ only, and deliberately no dependencies: the global `WebSocket` is
// enough, and a test fixture that needs an npm install is a test fixture nobody
// runs. Writes one JSONL file of events to the directory given as argv[2].
//
// It lives in `relay/tools/` rather than under `src/` because nothing loads it
// — no classpath read, no Gradle task, only a human typing `node`. That also
// keeps it clear of `NoBrowserFilesInEngineModulesTest`, which bans `.mjs`
// inside an engine's `src` tree by extension rather than by path. The ban is
// about BROWSER code, and this file never touches a DOM, so the honest fix is
// to put a shell tool where shell tools go, not to poke a hole in the rule.
//
// The corpus is NOT committed. These are other people's public events, they
// are megabytes, and AGENTS.md's rule is to reach for staging rather than
// invent a fixture — so the repo carries the fetch and staging carries the
// data. Everything here is an anonymous read declaring `include:spam`, which
// is what LensRequiredPolicy requires of one.
//
// TWO relays, because no single one has the whole picture: the search relay
// has the labels, the contact cards and 55M profiles; the TAPESTRY relay is
// where the Trusted List family is actually published, along with the kind-10040
// that enrols its signer — which is what lets the list assertions run on a real
// trust chain instead of a synthesized one.
//
//   node fetch-corpus.mjs /tmp/corpus \
//       wss://search-staging.brainstorm.world/ wss://tapestry.brainstorm.world/relay
//   ./gradlew :relay:test --tests '*ProductionCorpusIT*' \
//       -DitVespa=http://localhost:8080 -DitCorpus=/tmp/corpus
import { mkdirSync, writeFileSync } from "node:fs";

const DIR = process.argv[2] || "/tmp/corpus";
const RELAYS = process.argv.slice(3);
if (RELAYS.length === 0) RELAYS.push("wss://search-staging.brainstorm.world/", "wss://tapestry.brainstorm.world/relay");
// `include:spam` waives the ranking lens the search relay requires of an
// anonymous read (LensRequiredPolicy). NIP-50 says a relay SHOULD ignore
// extensions it does not support — but "ignore the token" and "ignore the
// field" are different things, and a relay that treats a non-empty `search` as
// a constraint it cannot satisfy answers a lensed filter with silence rather
// than a refusal. The tapestry relay does exactly that, which is how a corpus
// that looked complete arrived with none of its Trusted Lists in it. So the
// token is not a constant: each relay is ASKED, once, which kind of reader it
// wants, and the answer decides every filter sent to it.
const SPAM = "include:spam";

/** Does this relay answer a read that declares no lens? One cheap ask, once. */
const wantsLens = (relay) =>
  new Promise((resolve) => {
    const ws = new WebSocket(relay);
    let answered = false;
    const fin = (lens) => { if (answered) return; answered = true; clearTimeout(t); try { ws.close(); } catch {} resolve(lens); };
    const t = setTimeout(() => fin(true), 20_000);
    ws.onopen = () => ws.send(JSON.stringify(["REQ", "probe", { kinds: [0], limit: 1 }]));
    ws.onerror = () => fin(true);
    ws.onmessage = (e) => {
      const m = JSON.parse(e.data);
      // An event back means a lensless read is answered here: send no token.
      if (m[0] === "EVENT") fin(false);
      else if (m[0] === "EOSE") fin(true);
      else if (m[0] === "CLOSED") fin(true);
    };
  });

// The kinds this feature is about, plus enough of what they POINT AT that the
// expansion has something real to splice. 10040 is taken whole — 337 of them
// at the time of writing — because the enrolment assertions read the real
// service dimensions out of it.
const PAGES = [
  { id: "p10040", filter: { kinds: [10040], limit: 500, search: SPAM } },
  { id: "p1985", filter: { kinds: [1985], limit: 400, search: SPAM } },
  { id: "p30382", filter: { kinds: [30382], limit: 400, search: SPAM } },
  { id: "p30392", filter: { kinds: [30392, 30393, 30394, 30395], limit: 500, search: SPAM } },
  // The enrolment side of the list family, and the reason the list assertions
  // can run on a real chain: a 10040 that names the list signer as a service.
  { id: "p10040b", filter: { kinds: [10040], limit: 500 } },
  { id: "pprofiles", filter: { kinds: [0], limit: 200, search: SPAM } },
];

// Merged by id: the same event is on several relays and the corpus wants one.
const byId = new Map();
const out = { get length() { return byId.size; } };
const keep = (e) => byId.set(e.id, e);

const lensOf = new Map();
const lensed = (filter, relay) => {
  if (!lensOf.get(relay)) { const { search, ...rest } = filter; return rest; }
  return { ...filter, search: SPAM };
};

const run = (relay, asks) =>
  new Promise((resolve) => {
    const ws = new WebSocket(relay);
    let i = 0;
    let done = false;
    // A relay that stalls or refuses costs its share of the corpus and nothing
    // else — the others still answer, and the test says what it could not find.
    const fin = (why) => { if (done) return; done = true; clearTimeout(timer); try { ws.close(); } catch {} console.error(`  ${relay}: ${why}`); resolve(); };
    const timer = setTimeout(() => fin("timed out on " + (asks[i] || {}).id), 300_000);
    const ask = () => {
      if (i >= asks.length) { fin(`${asks.length} asks done`); return; }
      ws.send(JSON.stringify(["REQ", asks[i].id, lensed(asks[i].filter, relay)]));
    };
    ws.onopen = ask;
    ws.onerror = (e) => fin("socket: " + (e.message || e));
    ws.onmessage = (e) => {
      const m = JSON.parse(e.data);
      if (m[0] === "EVENT") keep(m[2]);
      else if (m[0] === "EOSE") { ws.send(JSON.stringify(["CLOSE", asks[i].id])); i++; ask(); }
      // A CLOSED is a refusal, not a crash — say which ask lost and carry on.
      else if (m[0] === "CLOSED") { console.error("  refused:", JSON.stringify(m).slice(0, 120)); i++; ask(); }
    };
  });

const runAll = async (asks) => {
  for (const relay of RELAYS) {
    if (!lensOf.has(relay)) {
      lensOf.set(relay, await wantsLens(relay));
      console.error(`  ${relay}: ${lensOf.get(relay) ? "wants a lens token" : "answers lensless reads"}`);
    }
    await run(relay, asks);
  }
};

await runAll(PAGES);
console.error(`pages: ${out.length} events`);

// Second pass: whatever the pointers point AT. This is the half that makes the
// expansion assertions mean anything — a real label pointing at a real note,
// and a real 30392's `p` resolving to a real profile — so the corpus has to be
// closed under "what this event names" rather than just a sample of kinds.
const targets = new Set();
const people = new Set();
for (const e of byId.values()) {
  const pointer = e.kind === 1985 || (e.kind >= 30392 && e.kind <= 30395) || e.kind === 30382;
  if (!pointer) continue;
  for (const t of e.tags) {
    if (typeof t[1] !== "string" || t[1].length !== 64) continue;
    if (t[0] === "e") targets.add(t[1]);
    if (t[0] === "p") people.add(t[1]);
  }
  // A 30382's subject is its `d`, not a `p`.
  if (e.kind === 30382) {
    const d = e.tags.find((t) => t[0] === "d");
    if (d && typeof d[1] === "string" && d[1].length === 64) people.add(d[1]);
  }
}
const chunks = [];
const all = [...targets];
for (let i = 0; i < all.length; i += 200) chunks.push(all.slice(i, i + 200));
const asks = chunks.map((ids, n) => ({ id: "t" + n, filter: { ids, search: SPAM } }));

// The profiles of everyone a pointer names, so a subject lookup has something
// real to find. Capped: a 30382 page names hundreds of subjects and the point
// is a corpus that closes, not a mirror of the relay.
const who = [...people].slice(0, 600);
for (let i = 0; i < who.length; i += 200) {
  asks.push({ id: "w" + i, filter: { kinds: [0], authors: who.slice(i, i + 200), search: SPAM } });
}
await runAll(asks);
console.error(`+ targets and profiles: ${out.length} events total`);

mkdirSync(DIR, { recursive: true });
writeFileSync(`${DIR}/corpus.jsonl`, [...byId.values()].map((e) => JSON.stringify(e)).join("\n") + "\n");
console.error(`wrote ${DIR}/corpus.jsonl`);
