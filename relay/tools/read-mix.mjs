// A client-shaped READ MIX against a relay, for measuring what serving costs: the
// allocation and GC profile, latency per shape. Node 21+, no dependencies, like
// fetch-corpus.mjs beside it, and for the same reasons.
//
// Every filter is lensed through one observer, as an anonymous client must
// (LensRequiredPolicy). The shapes are drawn from every *.jsonl in a corpus
// directory (the same events the relay was fed: kind 0 profiles, kind 1 notes,
// kind 3 follow lists, as fetch-slice.mjs pulls them) with a seeded generator, so
// two builds can be fed the same sequence: SEED=42 is the default.
//
//   node relay/tools/read-mix.mjs ws://localhost:7777 /tmp/corpus 16 20000
//
// Prints one line per shape: queries, events per query, milliseconds per query.
// Pair it with `jcmd <pid> JFR.start settings=profile` on the relay; docs/instrumentation.md
// has the rest of the method.
import { readdirSync, readFileSync } from "node:fs";

const [url, dir, socksS, countS, mix = "all"] = process.argv.slice(2);
const OBS = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c";
const L = `observer:${OBS}`;
const byKind = new Map();
for (const f of readdirSync(dir).filter((f) => f.endsWith(".jsonl")).sort()) {
  for (const line of readFileSync(`${dir}/${f}`, "utf8").split("\n")) {
    if (!line) continue;
    const e = JSON.parse(line);
    if (!byKind.has(e.kind)) byKind.set(e.kind, []);
    byKind.get(e.kind).push(e);
  }
}
const of = (kind, max) => (byKind.get(kind) || []).slice(0, max);
const k3 = of(3, Infinity), k1 = of(1, 40000), k0 = of(0, 20000);
if (!k3.length || !k1.length || !k0.length) throw new Error(`${dir} needs kind 0, 1 and 3 events`);
const follows = k3.map((e) => e.tags.filter((t) => t[0] === "p" && /^[0-9a-f]{64}$/.test(t[1] || "")).map((t) => t[1])).filter((a) => a.length > 20);
const noteIds = k1.map((e) => e.id), authors = k0.map((e) => e.pubkey);
const words = ["bitcoin", "nostr", "coffee", "music", "art", "zap", "freedom", "lightning", "photography", "news", "jack", "vitor", "amethyst", "relay", "gm", "privacy", "meme", "cat", "dog", "food"];
let seed = Number(process.env.SEED || 42) >>> 0;
const rand = () => { seed = (seed + 0x6d2b79f5) >>> 0; let t = seed; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
const pick = (a) => a[Math.floor(rand() * a.length)];
const some = (a, n) => Array.from({ length: n }, () => pick(a));
const shapes = {
  feed: () => [{ kinds: [1, 6], authors: pick(follows).slice(0, 500), limit: 200, search: L }],
  search: () => [{ kinds: [1], search: `${pick(words)} ${L}`, limit: 50 }],
  people: () => [{ kinds: [0], search: `${pick(words)} ${L}`, limit: 30 }],
  thread: () => [{ ids: [pick(noteIds)], search: L }, { kinds: [1, 7, 9735], "#e": [pick(noteIds)], limit: 500, search: L }],
  profiles: () => [{ kinds: [0, 3, 10002], authors: some(authors, 100), search: L }],
  notif: () => [{ kinds: [1, 7, 6, 9735], "#p": [pick(authors)], limit: 100, search: L }],
  global: () => [{ kinds: [1], limit: 500, search: L }],
  count: () => [{ kinds: [7], "#e": some(noteIds, 20), search: L }],
};
const names = mix === "all" ? Object.keys(shapes) : mix.split(",");
const stat = Object.fromEntries(names.map((n) => [n, { n: 0, ev: 0, ms: 0, closed: 0 }]));
const total = Number(countS); let sub = 0, started = 0; const t0 = Date.now();
const worker = () => new Promise((resolve) => {
  const ws = new WebSocket(url); let cur;
  const go = () => {
    if (started >= total) { ws.close(); resolve(); return; }
    started++;
    const name = pick(names), id = "s" + sub++; const filters = shapes[name]();
    cur = { name, id, t: Date.now(), ev: 0 };
    ws.send(JSON.stringify(name === "count" ? ["COUNT", id, ...filters] : ["REQ", id, ...filters]));
  };
  const done = (closed) => { const s = stat[cur.name]; s.n++; s.ev += cur.ev; s.ms += Date.now() - cur.t; if (closed) s.closed++; if (!closed && cur.name !== "count") ws.send(JSON.stringify(["CLOSE", cur.id])); go(); };
  ws.onopen = go; ws.onerror = () => resolve();
  ws.onmessage = (e) => { const m = JSON.parse(e.data);
    if (m[0] === "EVENT") cur.ev++; else if (m[0] === "EOSE" || m[0] === "COUNT") done(false); else if (m[0] === "CLOSED") { if (stat[cur.name].closed < 3) console.error("CLOSED", cur.name, m[2]); done(true); } };
});
await Promise.all(Array.from({ length: Number(socksS) }, worker));
console.log(`${total} queries in ${((Date.now() - t0) / 1000).toFixed(1)}s`);
for (const [n, s] of Object.entries(stat)) console.log(`${n.padEnd(9)} n=${s.n} ev/q=${(s.ev / Math.max(1, s.n)).toFixed(1)} ms/q=${(s.ms / Math.max(1, s.n)).toFixed(1)} closed=${s.closed}`);
