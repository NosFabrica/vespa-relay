// Pulls a SLICE of one kind mix off a relay, newest first, paging backwards with
// `until`, into one JSONL file. Node 21+, no dependencies. A read-only client: it
// only sends REQ and CLOSE. The slice feeds read-mix.mjs and the ingest
// benchmark, so the costs measured are those of real events.
//
//   node relay/tools/fetch-slice.mjs /tmp/corpus/k1.jsonl 150000 '{"kinds":[1],"search":"include:spam","until":1792800000}'
//
// `include:spam` is the lens waiver an anonymous read needs on the search relay.
// Set `until` below the present: the live corpus holds notes dated far in the
// future, which sort ahead of everything.
import { createWriteStream } from "node:fs";

const [out, maxS, filterS, relay = "wss://search.brainstorm.world/"] = process.argv.slice(2);
if (!out || !maxS || !filterS) {
  console.error("usage: fetch-slice.mjs <out.jsonl> <max> <filter json> [relay]");
  process.exit(1);
}
const MAX = Number(maxS); const base = JSON.parse(filterS);
const w = createWriteStream(out);
let n = 0, until = base.until ?? Math.floor(Date.now() / 1000), seen = new Set();
const page = () => new Promise((resolve, reject) => {
  const ws = new WebSocket(relay);
  let got = 0, received = 0, oldest = until, t = setTimeout(() => { try { ws.close(); } catch {} ; resolve({ got, received, oldest, timeout: true }); }, 120000);
  ws.onopen = () => ws.send(JSON.stringify(["REQ", "p", { ...base, until, limit: Math.min(5000, MAX - n) }]));
  ws.onerror = (e) => { clearTimeout(t); reject(e); };
  ws.onmessage = (e) => {
    const m = JSON.parse(e.data);
    if (m[0] === "EVENT") { const ev = m[2]; received++; if (!seen.has(ev.id)) { seen.add(ev.id); w.write(JSON.stringify(ev) + "\n"); n++; got++; } if (ev.created_at < oldest) oldest = ev.created_at; }
    else if (m[0] === "EOSE" || m[0] === "CLOSED") { if (m[0] === "CLOSED") console.error("CLOSED", m[2]); clearTimeout(t); ws.close(); resolve({ got, received, oldest }); }
  };
});
while (n < MAX) {
  let r; try { r = await page(); } catch (e) { console.error("err, retry", e.message); await new Promise(r => setTimeout(r, 3000)); continue; }
  console.error(`${out}: +${r.got} = ${n} until=${until}`);
  // A full page that never left `until` is a second holding more than a page: `until` alone
  // cannot reach the rest of it, so say so rather than call the slice complete.
  if (r.oldest >= until && r.received >= Math.min(5000, MAX - n + r.got)) console.error(`${out}: more than a page at created_at=${until}; the rest of that second is skipped`);
  if (r.got === 0 && r.oldest >= until) break;
  until = r.oldest === until ? until - 1 : r.oldest;
  if (r.got === 0) until -= 1;
}
w.end(); console.error(`${out}: done ${n}`);
