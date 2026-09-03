// Fetch the provider's 30382 cards for exactly the authors in a captured corpus:
// usage: node capture_cards.mjs <provider hex> <out.ndjson> <corpus.ndjson...>
const fs = await import("node:fs");
const readline = await import("node:readline");
const [provider, out, ...inputs] = process.argv.slice(2);
const authors = new Set();
for (const f of inputs) {
  const rl = readline.createInterface({ input: fs.createReadStream(f) });
  for await (const line of rl) { if (!line) continue; const i = line.indexOf('"pubkey":"'); if (i >= 0) authors.add(line.substr(i + 10, 64)); }
}
const list = [...authors];
console.error(`${list.length} distinct authors`);
const fd = fs.openSync(out, "w");
const ws = new WebSocket("wss://search-staging.brainstorm.world/");
let i = 0, got = 0, cur = [], t0 = Date.now();
const BATCH = 500, PAR = 4;
let inflight = 0, nextId = 0; const pending = new Map();
function pump() {
  while (inflight < PAR && i < list.length) {
    const batch = list.slice(i, i + BATCH); i += BATCH;
    const id = "c" + nextId++; pending.set(id, []); inflight++;
    ws.send(JSON.stringify(["REQ", id, { kinds: [30382], authors: [provider], "#d": batch, limit: batch.length, search: "include:spam" }]));
  }
  if (inflight === 0 && i >= list.length) { console.error(`done: ${got} cards for ${list.length} authors in ${(Date.now()-t0)/1000}s`); fs.closeSync(fd); ws.close(); }
}
ws.onopen = pump;
ws.onmessage = (e) => {
  const m = JSON.parse(e.data);
  if (m[0] === "EVENT") { pending.get(m[1])?.push(JSON.stringify(m[2])); return; }
  if (m[0] === "EOSE" || m[0] === "CLOSED") {
    if (m[0] === "CLOSED") console.error("CLOSED", m[2]);
    const evs = pending.get(m[1]) || []; pending.delete(m[1]); inflight--;
    if (evs.length) fs.writeSync(fd, evs.join("\n") + "\n"); got += evs.length;
    ws.send(JSON.stringify(["CLOSE", m[1]]));
    if ((i / BATCH) % 40 === 0) console.error(`${i}/${list.length} authors, ${got} cards, ${(Date.now()-t0)/1000}s`);
    pump();
  }
};
