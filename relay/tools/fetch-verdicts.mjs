// Pulls a relay's own MONITOR CORPUS, every kind-30166 record its monitor signed, for
// `MonitorProfileProbe`. One JSONL file of events, one record per relay url.
//
// Node 21+ and no dependencies, like fetch-corpus.mjs beside it, and like it the output is
// NOT committed: these are public claims about other people's servers, and the relay
// carries them. The signer is read off the relay's NIP-11 `self` unless named, and the
// read declares `include:spam`, which is what LensRequiredPolicy asks of an anonymous one.
//
//   node relay/tools/fetch-verdicts.mjs wss://search.brainstorm.world/ /tmp/v30166.jsonl
//   ./gradlew :monitor:test --tests '*MonitorProfileProbe*' -DprofileProbe=true \
//       -DprofileRecords=/tmp/v30166.jsonl -DprofileOut=/tmp/profile -DprofileHosts=1500 -PtestHeap=6g --rerun -i
import { createWriteStream } from "node:fs";

const [RELAY, OUT, AUTHOR] = process.argv.slice(2);
if (!RELAY || !OUT) {
  console.error("usage: node fetch-verdicts.mjs <relay ws url> <out.jsonl> [monitor pubkey hex]");
  process.exit(2);
}
const PAGE = 5000;

/** The monitor's pubkey: NIP-11's `self`, which is the key RELAY_NSEC signs as. */
const monitorOf = async (relay) => {
  const res = await fetch(relay.replace(/^ws/, "http"), { headers: { Accept: "application/nostr+json" } });
  const self = (await res.json()).self;
  if (!/^[0-9a-f]{64}$/.test(self ?? "")) throw new Error(`${relay} names no NIP-11 \`self\`; pass the monitor's pubkey`);
  return self;
};

/** One page below `until`, inclusive, as the events it holds. */
const page = (filter) =>
  new Promise((resolve, reject) => {
    const ws = new WebSocket(RELAY);
    const got = [];
    const timer = setTimeout(() => { try { ws.close(); } catch {} reject(new Error("no EOSE in 120s")); }, 120_000);
    ws.onopen = () => ws.send(JSON.stringify(["REQ", "v", filter]));
    ws.onerror = () => { clearTimeout(timer); reject(new Error("socket error")); };
    ws.onmessage = (e) => {
      const m = JSON.parse(e.data);
      if (m[0] === "EVENT") got.push(m[2]);
      else if (m[0] === "EOSE" || m[0] === "CLOSED") {
        clearTimeout(timer);
        if (m[0] === "CLOSED") console.error(`CLOSED: ${m[2]}`);
        try { ws.close(); } catch {}
        resolve(got);
      }
    };
  });

const author = AUTHOR ?? (await monitorOf(RELAY));
const out = createWriteStream(OUT);
// Records are replaceable by `d`, so the newest per url is the one the monitor stands behind.
const byUrl = new Map();
let until = Math.floor(Date.now() / 1000) + 86400;
for (;;) {
  let events;
  for (let attempt = 0; ; attempt++) {
    try {
      events = await page({ kinds: [30166], authors: [author], until, limit: PAGE, search: "include:spam" });
      break;
    } catch (e) {
      if (attempt >= 4) throw e;
      await new Promise((r) => setTimeout(r, 2000 << attempt));
    }
  }
  let fresh = 0;
  let oldest = until;
  for (const e of events) {
    oldest = Math.min(oldest, e.created_at);
    const d = e.tags.find((t) => t[0] === "d")?.[1];
    if (d === undefined) continue;
    const held = byUrl.get(d);
    if (held === undefined) fresh++;
    if (held === undefined || held.created_at < e.created_at) byUrl.set(d, e);
  }
  console.error(`page of ${events.length}, ${fresh} new url(s), ${byUrl.size} in all, until ${until}`);
  if (events.length === 0 || fresh === 0) break;
  // `until` is inclusive: a full page that ends on one second steps below it, any other re-reads it.
  until = events.length >= PAGE && oldest === until ? oldest - 1 : oldest;
}
for (const e of byUrl.values()) out.write(JSON.stringify(e) + "\n");
out.end();
console.error(`wrote ${byUrl.size} record(s) signed by ${author.slice(0, 12)}… to ${OUT}`);
