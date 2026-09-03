// The exact top-10 of a word under the observer, with the signals that placed each hit.
const fs = await import("node:fs");
const KEY = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c";
const word = process.argv[2] || "the"; const extra = process.argv[3] ? JSON.parse(process.argv[3]) : {};
let cur=null; const yqls=new Map();
for (const line of fs.readFileSync("yql-all.txt","utf8").split("\n")) { const m=/^=== "(.+?)" /.exec(line); if(m) cur=m[1]; else if(cur&&line.startsWith("  yql: ")) yqls.set(cur,line.slice(7).replace(/ limit \d+$/," limit 10").replace("select id, pubkey, created_at, kind, tags, content, sig, owner","select id, pubkey, kind")); }
const body={ yql: yqls.get(word), hits:"10", ranking:"search", w0:word, fw0:word, "ranking.features.query(user_q)":`{${KEY}:1.0}`, "ranking.features.query(min_rank)":"2.0","ranking.features.query(w_gram)": word.length<=3?"8.0":"2.0","ranking.features.query(n_words)":"1","ranking.features.query(now_secs)":String(Math.floor(Date.now()/1000)),"presentation.timing":true, ...extra};
const d=await (await fetch("http://localhost:8080/search/",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(body)})).json();
console.log(word, "served", d.root.fields.totalCount, "degraded", !!d.root.coverage.degraded?.["match-phase"], d.timing.searchtime*1000+"ms");
for (const c of d.root.children) {
  const f=c.fields, m=f.matchfeatures||{};
  const r = await (await fetch(`http://localhost:8080/document/v1/reputation/reputation/docid/${f.pubkey}?fieldSet=reputation:max_rank`)).json();
  console.log(` rel=${c.relevance.toExponential(2)} kind=${f.kind} trust=${m.user_score} max_rank=${r.fields?.max_rank} text=${Number(m.text_score).toFixed(0)} wot=${Number(m.wot_mult).toFixed(0)} tok=${m.any_token_match} near=${m.any_near_match} weak=${m.weak_match} body=${m.tier_body_match} affil=${m.affiliation_match}`);
}
