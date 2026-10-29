// How many LIVE FEEDS a relay sustains: sockets each holding REQs for {kinds:[1], limit:1}, the
// shape a client keeps open for "anything new", ramped step by step while one publisher writes at a
// fixed rate. Each step reports how fast the replay's EOSE came back, then, while publishing, what
// share of the frames every feed was owed arrived and how late (publish sent to frame received), and
// the relay's CPU, RSS and /pressure when RELAY_PID names its process. Node 22+, no dependencies.
//
//   RELAY_PID=$(pgrep -f vespa-relay) node relay/tools/live-feeds.mjs ws://localhost:7777
//
// STEPS     feeds per step, comma-separated                       2000,4000,8000,16000
// PER_SOCKET feeds per socket (the relay allows MAX_SUBSCRIPTIONS)  1
// RATE      events a second the publisher writes                  5
// HOLD      seconds each step publishes                           20
// LENS      the search token every feed carries                   include:spam
// CHILDREN  load processes; each is one process's open-file limit 4
// OPEN_RATE sockets opened a second, over all children            1000
// URLS      extra comma-separated ws:// addresses of the same relay (127.0.0.2…): one address
//           gives ~28k local ports
//
// It WRITES kind-1 notes under a throwaway key: point it at a local relay, never at staging or
// production. Feeds, sockets and the load share one clock, so run it on the relay's own host.
import { fork } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const env = (k, d) => process.env[k] ?? d;
const BUCKETS = [...Array.from({ length: 1000 }, (_, i) => i), ...Array.from({ length: 900 }, (_, i) => 1000 + i * 10), ...Array.from({ length: 500 }, (_, i) => 10000 + i * 100)];
const bucketOf = (ms) => (ms < 1000 ? Math.max(0, Math.floor(ms)) : ms < 10000 ? 1000 + Math.floor((ms - 1000) / 10) : Math.min(BUCKETS.length - 1, 1900 + Math.floor((ms - 10000) / 100)));
const quantile = (hist, q) => {
  const total = hist.reduce((a, b) => a + b, 0); if (!total) return NaN;
  let seen = 0; for (let i = 0; i < hist.length; i++) { seen += hist[i]; if (seen >= q * total) return BUCKETS[i]; }
  return BUCKETS.at(-1);
};

if (process.argv[2] === "--feeds") feedsChild();
else if (process.argv[2] === "--publisher") publisherChild();
else await main();

// ---------------------------------------------------------------- the feeds: sockets and their REQs
function feedsChild() {
  const urls = JSON.parse(process.argv[3]); const perSocket = Number(process.argv[4]); const lens = process.argv[5];
  const filter = { kinds: [1], limit: 1, search: lens };
  const sockets = []; let live = 0, failed = 0, dropped = 0, nextUrl = 0;
  const closedWhy = {}; const eose = [];
  let pubAt = new Map(), early = new Map(), hist = new Array(BUCKETS.length).fill(0), received = 0, windowOpen = false;

  const record = (id, at) => {
    const t = pubAt.get(id);
    if (t === undefined) { if (!early.has(id)) early.set(id, []); early.get(id).push(at); return; }
    hist[bucketOf(at - t)]++; received++;
  };
  const open = () => new Promise((resolve) => {
    const ws = new WebSocket(urls[nextUrl++ % urls.length]); const s = { ws, subs: perSocket, alive: false, gotEose: 0 };
    let settled = false; const settle = (ok) => { if (!settled) { settled = true; if (ok) { s.alive = true; live++; } else failed++; resolve(); } };
    ws.onopen = () => {
      const sent = performance.now();
      s.sent = sent;
      for (let i = 0; i < perSocket; i++) ws.send(JSON.stringify(["REQ", "f" + i, filter]));
    };
    ws.onmessage = (e) => {
      const m = JSON.parse(e.data);
      if (m[0] === "EVENT") { if (windowOpen) record(m[2].id, Date.now()); }
      else if (m[0] === "EOSE") { if (++s.gotEose === perSocket) { eose.push(performance.now() - s.sent); settle(true); } }
      else if (m[0] === "CLOSED" || m[0] === "NOTICE") { const why = String(m.at(-1)).slice(0, 80); closedWhy[why] = (closedWhy[why] || 0) + 1; if (m[0] === "CLOSED") s.subs--; }
    };
    ws.onerror = () => {};
    ws.onclose = () => { if (s.alive) { s.alive = false; live--; dropped++; } settle(false); };
    sockets.push(s);
  });

  process.on("message", async (msg) => {
    if (msg.op === "grow") {
      eose.length = 0; failed = 0;
      const want = msg.sockets - sockets.length; const gap = 1000 / msg.openRate; const pending = [];
      for (let i = 0; i < want; i++) { pending.push(open()); await new Promise((r) => setTimeout(r, gap)); }
      await Promise.race([Promise.all(pending), new Promise((r) => setTimeout(r, 60000))]);
      process.send({ op: "grown", live, failed, eose: [...eose], closedWhy: { ...closedWhy }, feeds: sockets.filter((s) => s.alive).reduce((a, s) => a + s.subs, 0) });
    } else if (msg.op === "window") {
      pubAt = new Map(); early = new Map(); hist.fill(0); received = 0; dropped = 0; windowOpen = true;
      process.send({ op: "windowed", feeds: sockets.filter((s) => s.alive).reduce((a, s) => a + s.subs, 0) });
    } else if (msg.op === "pub") {
      pubAt.set(msg.id, msg.t);
      for (const at of early.get(msg.id) || []) record(msg.id, at); early.delete(msg.id);
    } else if (msg.op === "report") {
      windowOpen = false;
      process.send({ op: "reported", received, hist, dropped, feeds: sockets.filter((s) => s.alive).reduce((a, s) => a + s.subs, 0) });
    } else if (msg.op === "exit") { for (const s of sockets) s.ws.close(); setTimeout(() => process.exit(0), 500); }
  });
  process.send({ op: "ready" });
}

// ---------------------------------------------------------------- the publisher
async function publisherChild() {
  const { event, keypair } = await import("./nostr-sign.mjs");
  const url = process.argv[3]; const key = keypair(); const run = Math.random().toString(36).slice(2, 8);
  const ws = new WebSocket(url); await new Promise((r) => (ws.onopen = r));
  const okAt = new Map(); const sentAt = new Map(); const refused = [];
  ws.onmessage = (e) => { const m = JSON.parse(e.data); if (m[0] === "OK") { okAt.set(m[1], Date.now() - (sentAt.get(m[1]) ?? Date.now())); if (!m[2]) refused.push(m[3]); } };
  let seq = 0;
  process.on("message", async (msg) => {
    if (msg.op === "sign") {
      const signed = Array.from({ length: msg.n }, () => event(key, 1, [], `live-feeds ${run} ${seq++}`));
      process.send({ op: "signed" });
      process.once("message", async (go) => {
        const gap = 1000 / go.rate; const start = performance.now();
        for (let i = 0; i < signed.length; i++) {
          const wait = start + i * gap - performance.now(); if (wait > 0) await new Promise((r) => setTimeout(r, wait));
          const t = Date.now(); sentAt.set(signed[i].id, t); ws.send(JSON.stringify(["EVENT", signed[i]]));
          process.send({ op: "pub", id: signed[i].id, t });
        }
        await new Promise((r) => setTimeout(r, 3000));
        const oks = signed.map((e) => okAt.get(e.id)).filter((x) => x !== undefined).sort((a, b) => a - b);
        process.send({ op: "published", n: signed.length, oks: oks.length, okP50: oks[Math.floor(oks.length / 2)], okMax: oks.at(-1), refused: refused.splice(0) });
      });
    } else if (msg.op === "exit") { ws.close(); process.exit(0); }
  });
  process.send({ op: "ready" });
}

// ---------------------------------------------------------------- the ramp
async function main() {
  const url = process.argv[2] || "ws://localhost:7777";
  const urls = [url, ...env("URLS", "").split(",").filter(Boolean)];
  const steps = env("STEPS", "2000,4000,8000,16000").split(",").map(Number);
  const perSocket = Number(env("PER_SOCKET", 1)), rate = Number(env("RATE", 5)), hold = Number(env("HOLD", 20));
  const lens = env("LENS", "include:spam"), nChildren = Number(env("CHILDREN", 4)), openRate = Number(env("OPEN_RATE", 1000));
  const pid = env("RELAY_PID", ""); const httpBase = url.replace(/^ws/, "http");
  const self = fileURLToPath(import.meta.url);

  const spawn = (args) => { const c = fork(self, args, { stdio: ["ignore", "inherit", "inherit", "ipc"] }); return c; };
  const next = (c, op) => new Promise((r) => { const h = (m) => { if (m.op === op) { c.off("message", h); r(m); } }; c.on("message", h); });
  const children = Array.from({ length: nChildren }, () => spawn(["--feeds", JSON.stringify(urls), String(perSocket), lens]));
  const publisher = spawn(["--publisher", url]);
  await Promise.all([...children, publisher].map((c) => next(c, "ready")));
  publisher.on("message", (m) => { if (m.op === "pub") for (const c of children) c.send(m); });

  const cpuTicks = () => { try { const f = readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1].split(" "); return Number(f[11]) + Number(f[12]); } catch { return NaN; } };
  const rssMb = () => { try { return Math.round(Number(/VmRSS:\s+(\d+)/.exec(readFileSync(`/proc/${pid}/status`, "utf8"))[1]) / 1024); } catch { return NaN; } };
  const pressure = async () => { try { return await (await fetch(`${httpBase}/pressure`)).json(); } catch { return null; } };
  const fmt = (x) => (Number.isFinite(x) ? String(Math.round(x)) : "-");

  console.log(`lens=${lens} per_socket=${perSocket} rate=${rate}/s hold=${hold}s children=${nChildren} urls=${urls.length}`);
  console.log("feeds  sockets fail  eose_p50 eose_p99 | owed      got%    p50   p90   p99   max ms | dropped relay_cpu% rss_mb pressure_ms liveGate");
  for (const feeds of steps) {
    const sockets = Math.ceil(feeds / perSocket);
    const per = children.map((_, i) => Math.floor(sockets / nChildren) + (i < sockets % nChildren ? 1 : 0));
    const grown = await Promise.all(children.map((c, i) => { const p = next(c, "grown"); c.send({ op: "grow", sockets: per[i], openRate: openRate / nChildren }); return p; }));
    const live = grown.reduce((a, g) => a + g.live, 0), failed = grown.reduce((a, g) => a + g.failed, 0);
    const eose = grown.flatMap((g) => g.eose).sort((a, b) => a - b);
    const why = {}; for (const g of grown) for (const [k, v] of Object.entries(g.closedWhy)) why[k] = (why[k] || 0) + v;

    const n = Math.round(rate * hold);
    const signed = next(publisher, "signed"); publisher.send({ op: "sign", n }); await signed;
    const windowed = await Promise.all(children.map((c) => { const p = next(c, "windowed"); c.send({ op: "window" }); return p; }));
    const liveFeeds = windowed.reduce((a, w) => a + w.feeds, 0);
    console.error(`  ${feeds} feeds open, publishing for ${hold}s`);
    const t0 = performance.now(), c0 = cpuTicks();
    const published = next(publisher, "published"); publisher.send({ op: "go", rate }); const pub = await published;
    const secs = (performance.now() - t0) / 1000, c1 = cpuTicks();
    const rep = await Promise.all(children.map((c) => { const p = next(c, "reported"); c.send({ op: "report" }); return p; }));
    const hist = new Array(BUCKETS.length).fill(0); for (const r of rep) r.hist.forEach((v, i) => (hist[i] += v));
    const got = rep.reduce((a, r) => a + r.received, 0), dropped = rep.reduce((a, r) => a + r.dropped, 0);
    const owed = liveFeeds * pub.n; const p = await pressure();
    const cpu = ((c1 - c0) / 100 / secs) * 100;
    console.log(
      `${String(feeds).padEnd(6)} ${String(live).padEnd(7)} ${String(failed).padEnd(5)} ${fmt(eose[Math.floor(eose.length / 2)]).padStart(8)} ${fmt(eose[Math.floor(eose.length * 0.99)]).padStart(8)} | ` +
        `${String(owed).padEnd(9)} ${((100 * got) / Math.max(1, owed)).toFixed(2).padStart(6)} ${fmt(quantile(hist, 0.5)).padStart(5)} ${fmt(quantile(hist, 0.9)).padStart(5)} ${fmt(quantile(hist, 0.99)).padStart(5)} ${fmt(quantile(hist, 1)).padStart(5)}    | ` +
        `${String(dropped).padEnd(7)} ${fmt(cpu).padStart(10)} ${fmt(rssMb()).padStart(6)} ${fmt(p?.meanMs).padStart(11)} ${p?.liveGate ? JSON.stringify(p.liveGate) : "-"}`,
    );
    if (pub.oks < pub.n || pub.refused.length) console.log(`  publisher: ${pub.oks}/${pub.n} OK, refused ${JSON.stringify(pub.refused.slice(0, 3))}`);
    else console.log(`  publisher: ${pub.n} OK, ok p50 ${pub.okP50}ms max ${pub.okMax}ms`);
    if (Object.keys(why).length) console.log(`  relay said: ${JSON.stringify(why)}`);
  }
  for (const c of [...children, publisher]) c.send({ op: "exit" });
}
