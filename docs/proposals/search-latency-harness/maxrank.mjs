// Writes max_rank = max over influence_scores cells into every reputation doc.
const base = "http://localhost:8080/document/v1/reputation/reputation/docid/";
let cont = null, n = 0, updated = 0, t0 = Date.now();
do {
  const u = base + "?wantedDocumentCount=40&fieldSet=reputation:influence_scores" + (cont ? "&continuation=" + cont : "");
  const d = await (await fetch(u)).json();
  const jobs = [];
  for (const doc of d.documents || []) {
    n++;
    const cells = doc.fields?.influence_scores?.cells ?? doc.fields?.influence_scores ?? {};
    const vals = Array.isArray(cells) ? cells.map((c) => c.value) : Object.values(cells);
    const max = vals.length ? Math.max(...vals.map(Number)) : 0;
    jobs.push(fetch("http://localhost:8080/document/v1/reputation/reputation/docid/" + doc.id.split("::")[1], { method: "PUT", headers: { "content-type": "application/json" }, body: JSON.stringify({ fields: { max_rank: { assign: max } } }) }).then((r) => { if (r.ok) updated++; else r.text().then((t) => console.error(r.status, t.slice(0, 200))); }));
  }
  await Promise.all(jobs);
  cont = d.continuation;
} while (cont);
console.log(`${n} reputation docs, ${updated} updated in ${(Date.now() - t0) / 1000}s`);
