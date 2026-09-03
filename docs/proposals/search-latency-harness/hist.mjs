const base = 'select * from event where kind in (1) and search_text contains "bitcoin" limit 0 | all(group(author_max_rank) each(output(count())))';
async function run(extra) {
  const body = { yql: base, hits: "0", ranking: "unranked", "grouping.defaultMaxGroups": "-1", "presentation.timing": true, ...extra };
  const d = await (await fetch("http://localhost:8080/search/", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) })).json();
  const groups = d.root.children?.[0]?.children?.[0]?.children || [];
  const h = groups.map((g) => [Number(g.value), g.fields["count()"]]).sort((a, b) => a[0] - b[0]);
  const total = h.reduce((s, [, c]) => s + c, 0);
  const bucket = (lo, hi) => h.filter(([v]) => v >= lo && v < hi).reduce((s, [, c]) => s + c, 0);
  return { total, degraded: !!d.root.coverage.degraded?.["match-phase"], ms: d.timing.searchtime * 1000, b0: bucket(0, 1), b1_19: bucket(1, 20), b20_49: bucket(20, 50), b50_79: bucket(50, 80), b80: bucket(80, 101) };
}
console.log("uncut       ", await run({}));
for (const mh of [100, 500, 1000, 2000, 5000]) console.log(`cut ${String(mh).padStart(5)}   `, await run({ "ranking.matchPhase.attribute": "author_max_rank", "ranking.matchPhase.ascending": "false", "ranking.matchPhase.maxHits": String(mh) }));
