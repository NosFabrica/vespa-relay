// The page as changed: a debounce of 250ms, one type-ahead in flight, the text
// that queued behind it re-debounced before it runs, Enter reusing an ask for
// the same text and CLOSING one for any other. Same word and cadence as before.
const KEY = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c";
const word = process.argv[2] || "bitcoin"; const gapMs = Number(process.argv[3] || 200); const DEBOUNCE = 250;
const ws = new WebSocket("wss://search-staging.brainstorm.world/");
const T0 = performance.now(); const t = () => (performance.now() - T0).toFixed(0).padStart(6) + "ms";
const open = new Map(); let n = 0, inflight = null, queued = null, typed = "", entered = false, timer = null;
const f = (text, limit) => ({ kinds: [1, 11, 1111], search: `${text} observer:${KEY}`, limit });
const box = () => typed;
function req(text) { const id = "s" + n++; open.set(id, { t0: performance.now(), count: 0, text }); ws.send(JSON.stringify(["REQ", id, f(text, 160)])); console.log(t(), `REQ  popup "${text}"(160)`); inflight = id; }
function runPopup(text) { if (inflight) { queued = text; return; } queued = null; req(text); }
function arm(text) { clearTimeout(timer); timer = setTimeout(() => { if (box() === text) runPopup(text); }, DEBOUNCE); }
ws.onmessage = (e) => {
  const m = JSON.parse(e.data); const o = open.get(m[1]); if (!o) return;
  if (m[0] === "EVENT") { o.count++; return; }
  if (m[0] === "EOSE" || m[0] === "CLOSED") {
    console.log(t(), `EOSE popup "${o.text}"`, `${o.count} ev, ${(performance.now() - o.t0).toFixed(0)}ms`); ws.send(JSON.stringify(["CLOSE", m[1]])); open.delete(m[1]); inflight = null;
    if (entered && o.text === word) { console.log(t(), "results view drawn from the reused ask"); ws.close(); return; }
    if (!entered && queued && queued !== o.text && box() === queued) arm(queued);
  }
};
ws.onopen = async () => {
  for (const ch of word) { typed += ch; arm(typed); await new Promise((r) => setTimeout(r, gapMs)); }
  await new Promise((r) => setTimeout(r, 700)); // the reader presses Enter
  entered = true; clearTimeout(timer);
  const cur = inflight && open.get(inflight);
  if (cur && cur.text === word) console.log(t(), "Enter: the in-flight ask IS this text -> reused");
  else {
    if (cur) { console.log(t(), `Enter: closing the in-flight "${cur.text}"`); ws.send(JSON.stringify(["CLOSE", inflight])); open.delete(inflight); inflight = null; }
    req(word);
  }
};
