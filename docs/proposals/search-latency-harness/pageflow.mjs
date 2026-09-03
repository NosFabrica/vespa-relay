// Emulates what app.js sends for one typed search: a debounced type-ahead per
// keystroke gap > 150ms (limit 8), then Enter (limit 40), then the pager's
// preload (limit 160) once the first page lands. Every ask is a full ranked search.
const KEY = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c";
const word = process.argv[2] || "bitcoin"; const gapMs = Number(process.argv[3] || 200);
const ws = new WebSocket("wss://search-staging.brainstorm.world/");
const T0 = performance.now(); const t = () => (performance.now() - T0).toFixed(0).padStart(6) + "ms";
const open = new Map(); let n = 0;
function req(label, filter) { const id = "s" + n++; open.set(id, { label, t0: performance.now(), count: 0 }); ws.send(JSON.stringify(["REQ", id, filter])); console.log(t(), "REQ ", label); return id; }
const f = (text, limit) => ({ kinds: [1, 11, 1111], search: `${text} observer:${KEY}`, limit });
let done;
const finished = new Promise((r) => (done = r));
ws.onmessage = (e) => {
  const m = JSON.parse(e.data); const o = open.get(m[1]);
  if (!o) return;
  if (m[0] === "EVENT") { o.count++; return; }
  if (m[0] === "EOSE" || m[0] === "CLOSED") {
    console.log(t(), "EOSE", o.label, `${o.count} ev, ${(performance.now() - o.t0).toFixed(0)}ms`); ws.send(JSON.stringify(["CLOSE", m[1]])); open.delete(m[1]);
    if (o.label === "full(40)") req("preload(160)", f(word, 160));
    if (o.label === "preload(160)") done();
  }
};
ws.onopen = async () => {
  // type it: each keystroke gap exceeds DEBOUNCE_MS, so every prefix fires a popup search
  let typed = "";
  for (const ch of word) { typed += ch; await new Promise((r) => setTimeout(r, gapMs)); req(`popup "${typed}"(8)`, f(typed, 8)); }
  await new Promise((r) => setTimeout(r, 700)); // the reader presses Enter
  req("full(40)", f(word, 40));
  await finished; console.log(t(), "sequence done"); ws.close();
};
