// Variants of the store's OWN production query (yql-all.txt, printed by
// searchTrace) against a local Vespa: baseline `search` vs the same query with
// a match-phase cut sent as query parameters. Prints p50 latency (Vespa's own
// searchtime), totalCount, degraded flag, and top-K overlap with the baseline.
const fs = await import("node:fs");
const KEY = process.env.OBSERVER || "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c";
const URL = process.env.VESPA_URL || "http://localhost:8080";
const K = Number(process.env.HITS || 40), REPS = Number(process.env.REPS || 5);
const words = process.argv.slice(2);
const yqls = new Map();
{ let cur = null; for (const line of fs.readFileSync("yql-all.txt", "utf8").split("\n")) { const m = /^=== "(.+?)" /.exec(line); if (m) cur = m[1]; else if (cur && line.startsWith("  yql: ")) yqls.set(cur, line.slice(7).replace(/ limit \d+$/, ` limit ${K}`)); } }
const fold = (s) => s.normalize("NFKD").replace(/\p{M}/gu, "").toLowerCase();
const now = Math.floor(Date.now() / 1000);
function base(word, extra = {}) {
  return { yql: yqls.get(word), hits: String(K), ranking: "search", w0: word, fw0: fold(word),
    "ranking.features.query(user_q)": `{${KEY}:1.0}`, "ranking.features.query(min_rank)": process.env.MIN_RANK || "2.0",
    "ranking.features.query(w_gram)": word.length <= 3 ? "8.0" : "2.0", "ranking.features.query(n_words)": "1",
    "ranking.features.query(now_secs)": String(now), "presentation.timing": true, "timeout": "120s",
    "presentation.summary": "dedup", ...extra };
}
async function run(body) {
  const r = await fetch(URL + "/search/", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
  if (!r.ok) throw new Error(r.status + " " + (await r.text()).slice(0, 300));
  const d = await r.json();
  return { ms: (d.timing?.searchtime ?? 0) * 1000, total: d.root.fields.totalCount, degraded: !!d.root.coverage.degraded?.["match-phase"], ids: (d.root.children || []).map((c) => c.fields.id) };
}
async function measure(body) {
  await run(body); const t = []; let last;
  for (let i = 0; i < REPS; i++) { last = await run(body); t.push(last.ms); }
  t.sort((a, b) => a - b); return { ...last, p50: t[Math.floor(t.length / 2)] };
}
const overlap = (a, b) => { const s = new Set(a); return b.filter((x) => s.has(x)).length; };
const variants = (process.env.VARIANTS || "5000,20000,100000").split(",").map(Number);
const threads = (process.env.THREADS || "4").split(",");
console.log(`corpus: ${(await run({ yql: "select * from event where true", hits: "0", ranking: "unranked" })).total} docs; K=${K}, reps=${REPS}`);
for (const w of words) {
  if (!yqls.has(w)) { console.log(`no yql for ${w}`); continue; }
  for (const th of threads) {
    const t = { "ranking.matching.numThreadsPerSearch": th };
    const b = await measure(base(w, t));
    console.log(`\n"${w}" threads=${th}  baseline search: ${b.p50.toFixed(0)}ms  matches=${b.total}`);
    for (const mh of variants) {
      const v = await measure(base(w, { ...t, "ranking.matchPhase.attribute": process.env.ATTR || "created_at", "ranking.matchPhase.ascending": "false", "ranking.matchPhase.maxHits": String(mh) }));
      console.log(`   maxHits=${String(mh).padStart(6)}: ${v.p50.toFixed(0).padStart(5)}ms  count=${String(v.total).padStart(7)} degraded=${v.degraded}  top${K} overlap=${overlap(b.ids, v.ids)}/${b.ids.length}  top10 overlap=${overlap(b.ids.slice(0,10), v.ids.slice(0,10))}/10`);
    }
  }
}
