// Trust-keyed cut + an exactness bound: after a cut at threshold T (the least
// author_max_rank among kept docs), every excluded doc scores at most
// CEIL * wot(T) * 1.1, so the page is exact when its K-th hit beats that.
const fs = await import("node:fs");
const KEY = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c";
const K = Number(process.env.HITS || 40), CEIL = Number(process.env.CEIL || 5000), MIN_RANK = 2;
const wot = (r) => 1 + Math.pow(Math.min(Math.max(0, r - MIN_RANK), 100), 2.7);
const words = process.argv.slice(2);
let cur = null; const yqls = new Map();
for (const line of fs.readFileSync("yql-all.txt", "utf8").split("\n")) { const m = /^=== "(.+?)" /.exec(line); if (m) cur = m[1]; else if (cur && line.startsWith("  yql: ")) yqls.set(cur, line.slice(7).replace(/ limit \d+$/, ` limit ${K}`).replace("select id, pubkey, created_at, kind, tags, content, sig, owner", "select id")); }
const now = Math.floor(Date.now() / 1000);
async function run(word, extra = {}) {
  const body = { yql: yqls.get(word) + " | all(output(min(author_max_rank)))", hits: String(K), ranking: "search", w0: word, fw0: word,
    "ranking.features.query(user_q)": `{${KEY}:1.0}`, "ranking.features.query(min_rank)": String(MIN_RANK), "ranking.features.query(w_gram)": word.length <= 3 ? "8.0" : "2.0",
    "ranking.features.query(n_words)": "1", "ranking.features.query(now_secs)": String(now), "presentation.timing": true, "timeout": "120s", ...extra };
  const t = []; let d;
  for (let i = 0; i < 4; i++) { d = await (await fetch("http://localhost:8080/search/", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) })).json(); if (i) t.push(d.timing.searchtime * 1000); }
  t.sort((a, b) => a - b);
  const hits = d.root.children.filter((c) => c.fields?.id).map((c) => ({ id: c.fields.id, rel: c.relevance }));
  const group = d.root.children.find((c) => c.id?.startsWith("group"));
  const gf = group?.fields?.["min(author_max_rank)"] ?? group?.children?.[0]?.fields?.["min(author_max_rank)"];
  if (process.env.DEBUG) console.log(JSON.stringify(group).slice(0, 300));
  const minKept = Number(gf ?? NaN);
  return { ms: t[1], total: d.root.fields.totalCount, degraded: !!d.root.coverage.degraded?.["match-phase"], hits, minKept };
}
const overlap = (a, b) => { const s = new Set(a.map((h) => h.id)); return b.filter((h) => s.has(h.id)).length; };
for (const w of words) {
  const exact = await run(w);
  console.log(`\n"${w}" exact: ${exact.ms.toFixed(0)}ms served=${exact.total} K-th rel=${exact.hits[K - 1]?.rel.toExponential(2)}`);
  for (const mh of (process.env.VARIANTS || "200,500,1000,2000,5000").split(",")) {
    const c = await run(w, { "ranking.matchPhase.attribute": "author_max_rank", "ranking.matchPhase.ascending": "false", "ranking.matchPhase.maxHits": mh });
    const kth = c.hits[K - 1]?.rel ?? 0; const bound = CEIL * wot(c.minKept) * 1.1; const proven = c.hits.length >= K && kth >= bound;
    console.log(`  cut ${String(mh).padStart(5)}: ${c.ms.toFixed(0).padStart(4)}ms kept=${String(c.total).padStart(6)} T=${String(c.minKept).padStart(3)} bound=${bound.toExponential(2)} K-th=${kth.toExponential(2)} proven=${proven} degraded=${c.degraded} top10=${overlap(exact.hits.slice(0, 10), c.hits.slice(0, 10))}/10 top${K}=${overlap(exact.hits, c.hits)}/${K}`);
  }
}
