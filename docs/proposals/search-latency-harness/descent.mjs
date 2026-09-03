// Exact trust descent: an explicit `author_max_rank >= T` clause (a clean
// threshold, unlike the match phase's estimate), T falling until the page's
// K-th hit beats what any excluded doc could score.
const fs = await import("node:fs");
const KEY = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c";
const K = Number(process.env.HITS || 40), CEIL = Number(process.env.CEIL || 131100), MIN_RANK = 2;
const wot = (r) => r < MIN_RANK ? 0 : 1 + Math.pow(Math.min(Math.max(0, r - MIN_RANK), 100), 2.7);
let cur = null; const yqls = new Map();
for (const line of fs.readFileSync("yql-all.txt", "utf8").split("\n")) { const m = /^=== "(.+?)" /.exec(line); if (m) cur = m[1]; else if (cur && line.startsWith("  yql: ")) yqls.set(cur, line.slice(7).replace(/ limit \d+$/, ` limit ${K}`).replace("select id, pubkey, created_at, kind, tags, content, sig, owner", "select id")); }
const now = Math.floor(Date.now() / 1000);
async function run(word, T) {
  const yql = T == null ? yqls.get(word) : yqls.get(word).replace("where kind in (1) and", `where kind in (1) and author_max_rank >= ${T} and`);
  const body = { yql, hits: String(K), ranking: "search", w0: word, fw0: word, "ranking.features.query(user_q)": `{${KEY}:1.0}`, "ranking.features.query(min_rank)": String(MIN_RANK), "ranking.features.query(w_gram)": word.length <= 3 ? "8.0" : "2.0", "ranking.features.query(n_words)": "1", "ranking.features.query(now_secs)": String(now), "presentation.timing": true, "timeout": "120s" };
  const t = []; let d;
  for (let i = 0; i < 4; i++) { d = await (await fetch("http://localhost:8080/search/", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) })).json(); if (d.errors) throw new Error(JSON.stringify(d.errors).slice(0, 300)); if (i) t.push(d.timing.searchtime * 1000); }
  t.sort((a, b) => a - b);
  return { ms: t[1], total: d.root.fields.totalCount, hits: (d.root.children || []).map((c) => ({ id: c.fields.id, rel: c.relevance })) };
}
const overlap = (a, b) => { const s = new Set(a.map((h) => h.id)); return b.filter((h) => s.has(h.id)).length; };
for (const w of process.argv.slice(2)) {
  const exact = await run(w, null);
  console.log(`\n"${w}" exact: ${exact.ms.toFixed(0)}ms served=${exact.total} K-th rel=${exact.hits[K - 1]?.rel.toExponential(2)}`);
  let spent = 0;
  for (const T of [90, 70, 50, 30, 20, 10, 5, 2]) {
    const c = await run(w, T); spent += c.ms;
    const kth = c.hits[K - 1]?.rel ?? 0, bound = CEIL * wot(T - 1) * 1.1, proven = c.hits.length >= K && kth >= bound;
    console.log(`  rank>=${String(T).padStart(2)}: ${c.ms.toFixed(0).padStart(4)}ms kept=${String(c.total).padStart(6)} K-th=${kth.toExponential(2)} bound=${bound.toExponential(2)} proven=${proven} top10=${overlap(exact.hits.slice(0, 10), c.hits.slice(0, 10))}/10 top${K}=${overlap(exact.hits, c.hits)}/${K}  (ladder so far ${spent.toFixed(0)}ms)`);
    if (proven) break;
  }
}
