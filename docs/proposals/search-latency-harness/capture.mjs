// Read-only capture from staging: pages a kind newest-first on `until`,
// writes NDJSON. usage: node capture.mjs <kinds csv> <target count> <outfile> [extra filter json]
const [kindsCsv, targetStr, out, extraJson] = process.argv.slice(2);
const kinds = kindsCsv.split(",").map(Number);
const target = Number(targetStr);
const extra = extraJson ? JSON.parse(extraJson) : {};
const fs = await import("node:fs");
const ws = new WebSocket("wss://search-staging.brainstorm.world/");
const seen = new Set();
let until = process.env.START_UNTIL ? Number(process.env.START_UNTIL) : null, total = 0, page = 0, t0 = Date.now(), buf = [], cur = [];
const fd = fs.openSync(out, "w");
function ask() {
  const f = { kinds, limit: Number(process.env.LIMIT || 5000), search: "include:spam", ...extra };
  if (until != null) f.until = until;
  cur = [];
  ws.send(JSON.stringify(["REQ", "p" + page, f]));
}
ws.onopen = ask;
ws.onmessage = (e) => {
  const m = JSON.parse(e.data);
  if (m[0] === "EVENT") { cur.push(m[2]); return; }
  if (m[0] === "NOTICE") { console.error("NOTICE", m[1]); return; }
  if (m[0] !== "EOSE" && m[0] !== "CLOSED") return;
  ws.send(JSON.stringify(["CLOSE", "p" + page]));
  let fresh = 0, minT = Infinity;
  for (const ev of cur) {
    if (ev.created_at < minT) minT = ev.created_at;
    if (seen.has(ev.id)) continue;
    seen.add(ev.id); fresh++; total++;
    buf.push(JSON.stringify(ev));
  }
  if (buf.length) { fs.writeSync(fd, buf.join("\n") + "\n"); buf = []; }
  page++;
  if (page % 20 === 0) console.error(`${out}: page ${page} total ${total} until ${until} ${(Date.now()-t0)/1000}s`);
  if (m[0] === "CLOSED" || cur.length === 0 || fresh === 0 || total >= target) { console.error(`${out}: done ${total} events in ${(Date.now()-t0)/1000}s (${m[0]} ${m[2]||""})`); fs.closeSync(fd); ws.close(); return; }
  until = minT;
  ask();
};
ws.onerror = (e) => console.error("error", e.message); ws.onclose = (e) => console.error("closed", e.code, e.reason);
