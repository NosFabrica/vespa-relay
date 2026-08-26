// Pulls a PRODUCTION CORPUS off the staging relay for `ProductionCorpusIT`.
//
// Node 21+ only, and deliberately no dependencies: the global `WebSocket` is
// enough, and a test fixture that needs an npm install is a test fixture nobody
// runs. Writes one JSONL file of events to the directory given as argv[2].
//
// The corpus is NOT committed. These are other people's public events, they
// are megabytes, and AGENTS.md's rule is to reach for staging rather than
// invent a fixture — so the repo carries the fetch and staging carries the
// data. Everything here is an anonymous read declaring `include:spam`, which
// is what LensRequiredPolicy requires of one.
//
//   node fetch-corpus.mjs wss://search-staging.brainstorm.world/ /tmp/corpus
//   ./gradlew :relay:test --tests '*ProductionCorpusIT*' \
//       -DitVespa=http://localhost:8080 -DitCorpus=/tmp/corpus
import { mkdirSync, writeFileSync } from "node:fs";

const RELAY = process.argv[2] || "wss://search-staging.brainstorm.world/";
const DIR = process.argv[3] || "/tmp/corpus";
const SPAM = "include:spam";

// The kinds this feature is about, plus enough of what they POINT AT that the
// expansion has something real to splice. 10040 is taken whole — 337 of them
// at the time of writing — because the enrolment assertions read the real
// service dimensions out of it.
const PAGES = [
  { id: "p10040", filter: { kinds: [10040], limit: 500, search: SPAM } },
  { id: "p1985", filter: { kinds: [1985], limit: 400, search: SPAM } },
  { id: "p30382", filter: { kinds: [30382], limit: 400, search: SPAM } },
  { id: "p30392", filter: { kinds: [30392, 30393, 30394, 30395], limit: 400, search: SPAM } },
  { id: "pprofiles", filter: { kinds: [0], limit: 200, search: SPAM } },
];

const out = [];
const run = (asks) =>
  new Promise((resolve, reject) => {
    const ws = new WebSocket(RELAY);
    let i = 0;
    const timer = setTimeout(() => { ws.close(); reject(new Error("timed out on " + (asks[i] || {}).id)); }, 300_000);
    const ask = () => {
      if (i >= asks.length) { clearTimeout(timer); ws.close(); resolve(); return; }
      ws.send(JSON.stringify(["REQ", asks[i].id, asks[i].filter]));
    };
    ws.onopen = ask;
    ws.onerror = (e) => { clearTimeout(timer); reject(new Error("socket: " + (e.message || e))); };
    ws.onmessage = (e) => {
      const m = JSON.parse(e.data);
      if (m[0] === "EVENT") out.push(m[2]);
      else if (m[0] === "EOSE") { ws.send(JSON.stringify(["CLOSE", asks[i].id])); i++; ask(); }
      // A CLOSED is a refusal, not a crash — say which ask lost and carry on.
      else if (m[0] === "CLOSED") { console.error("refused:", JSON.stringify(m)); i++; ask(); }
    };
  });

await run(PAGES);
console.error(`pages: ${out.length} events`);

// Second pass: whatever the labels point at, by id. This is the half that
// makes the expansion assertions mean anything — a real label pointing at a
// real note this relay actually holds.
const targets = new Set();
for (const e of out) {
  if (e.kind !== 1985) continue;
  for (const t of e.tags) if (t[0] === "e" && typeof t[1] === "string" && t[1].length === 64) targets.add(t[1]);
}
const chunks = [];
const all = [...targets];
for (let i = 0; i < all.length; i += 200) chunks.push(all.slice(i, i + 200));
await run(chunks.map((ids, n) => ({ id: "t" + n, filter: { ids, search: SPAM } })));
console.error(`+ targets: ${out.length} events total`);

mkdirSync(DIR, { recursive: true });
writeFileSync(`${DIR}/corpus.jsonl`, out.map((e) => JSON.stringify(e)).join("\n") + "\n");
console.error(`wrote ${DIR}/corpus.jsonl`);
