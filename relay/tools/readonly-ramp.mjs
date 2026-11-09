// A READ-ONLY connection ramp, safe to aim at a production relay: sockets each hold PER_SOCKET live
// feeds ({kinds:[1],limit:1,search:LENS}), added step by step, and nothing is ever published. Each
// step reports how fast sockets opened and replays came back (EOSE), what dropped, the relay's
// /pressure, and, for the live events the relay's own traffic brings during HOLD, how many of the
// held feeds each reached, how many frames repeated an event a feed already had, and the spread
// between the first and last socket to see it. Node 22+, no dependencies. The live-feeds.mjs beside
// it measures delivery under its own publisher; this one only reads, so it is the one for a
// deployment that serves people.
//
//   STEPS=500,1000,2000 OPEN_RATE=40 node relay/tools/readonly-ramp.mjs wss://relay.example
//
// STEPS            sockets held at each step, comma-separated               100,500,1000
// PER_SOCKET       feeds per socket (up to the relay's max_subscriptions)    1
// HOLD             seconds each step holds its feeds before reporting        30
// OPEN_RATE        sockets opened a second, over all children; every one     40
//                  is a store query, and new feeds arriving is what costs
// OPEN_TIMEOUT     seconds a socket may take to open and answer its REQs     90
// CHILDREN         load processes                                            4
// LENS             the search token every feed carries; `none` omits it      include:spam
// SETTLE_MS        wait for /pressure under this before the first step       1000
// ABORT_EOSE_MS    stop after a step whose EOSE p99 reaches this             5000
// ABORT_PRESSURE_MS stop after a step whose /pressure peak reaches this      30000
// ABORT_FAIL_PCT   stop after a step where this share of new sockets failed  5
//
// The ramp stops at the first step past an abort line: the replay load it adds lands on the store
// every reader shares, and an overloaded store keeps draining for minutes after the load is gone.
// One load machine is one address, and a proxy between it and the relay can be the bound first.
import { fork } from "node:child_process";
import { fileURLToPath } from "node:url";

const env = (k, d) => process.env[k] ?? d;
const quantile = (sorted, p) => (sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))] : NaN);
const bump = (o, k) => { o[k] = (o[k] || 0) + 1; };

if (process.argv[2] === "--child") child();
else await main();

// ---------------------------------------------------------------- one load process: its sockets and their feeds
function child() {
  const [url, perSocket, lens] = [process.argv[3], Number(process.argv[4]), process.argv[5]];
  const openTimeoutMs = Number(env("OPEN_TIMEOUT", 90)) * 1000;
  const filter = lens === "none" ? { kinds: [1], limit: 1 } : { kinds: [1], limit: 1, search: lens };
  const sockets = [], holds = [], eose = [], openMs = [], why = {}, seen = new Map();
  let live = 0, failed = 0, dropped = 0, duplicates = 0, holding = false, closing = false;

  const open = () => new Promise((resolve) => {
    const t0 = performance.now(); const ws = new WebSocket(url); sockets.push(ws);
    let settled = false, answered = 0, sentAt = 0, alive = false;
    const heard = new Set(); // ids this socket got during the current hold
    holds.push(heard);
    const settle = (ok) => {
      if (settled) return; settled = true; clearTimeout(timer);
      if (ok) { alive = true; live++; } else { failed++; try { ws.close(); } catch {} }
      resolve();
    };
    // A socket that gave up must also close: one that connects late would still queue its replays.
    const timer = setTimeout(() => { bump(why, "timeout"); settle(false); }, openTimeoutMs);
    ws.onopen = () => {
      openMs.push(performance.now() - t0); sentAt = performance.now();
      for (let i = 0; i < perSocket; i++) ws.send(JSON.stringify(["REQ", "f" + i, filter]));
    };
    ws.onmessage = (e) => {
      const m = JSON.parse(e.data);
      if (m[0] === "EOSE" || m[0] === "CLOSED") {
        if (m[0] === "CLOSED") bump(why, "CLOSED: " + String(m[2]).slice(0, 60));
        if (++answered === perSocket) { eose.push(performance.now() - sentAt); settle(m[0] === "EOSE"); }
      } else if (m[0] === "EVENT" && holding && alive) {
        const key = m[1] + " " + m[2].id;
        if (heard.has(key)) { duplicates++; return; }
        heard.add(key);
        const s = seen.get(m[2].id), t = Date.now();
        if (s) { s.count++; s.last = t; } else seen.set(m[2].id, { count: 1, first: t, last: t });
      }
    };
    ws.onerror = () => {};
    ws.onclose = (e) => {
      if (!settled) { bump(why, "close " + e.code); settle(false); }
      else if (alive && !closing) { alive = false; live--; dropped++; }
    };
  });

  process.on("message", async (m) => {
    if (m.op === "open") {
      eose.length = 0; openMs.length = 0; failed = 0; for (const k in why) delete why[k];
      const opens = [];
      for (let i = 0; i < m.n; i++) { opens.push(open()); await new Promise((r) => setTimeout(r, 1000 / m.rate)); }
      await Promise.all(opens);
      process.send({ op: "opened", failed, eose, openMs, why });
    } else if (m.op === "hold") { seen.clear(); for (const h of holds) h.clear(); dropped = 0; duplicates = 0; holding = true; }
    else if (m.op === "report") { holding = false; process.send({ op: "held", live, dropped, duplicates, seen: [...seen.entries()] }); }
    else if (m.op === "exit") { closing = true; for (const s of sockets) try { s.close(); } catch {} setTimeout(() => process.exit(0), 500); }
  });
  process.send({ op: "ready" });
}

// ---------------------------------------------------------------- the ramp
async function main() {
  const url = process.argv[2];
  if (!url) { console.error("usage: node readonly-ramp.mjs wss://relay.example"); process.exit(2); }
  const http = url.replace(/^ws/, "http").replace(/\/$/, "");
  const steps = env("STEPS", "100,500,1000").split(",").map(Number);
  const perSocket = Number(env("PER_SOCKET", 1)), hold = Number(env("HOLD", 30));
  const nChildren = Number(env("CHILDREN", 4)), openRate = Number(env("OPEN_RATE", 40)), lens = env("LENS", "include:spam");
  const settleMs = Number(env("SETTLE_MS", 1000)), abortEose = Number(env("ABORT_EOSE_MS", 5000));
  const abortPressure = Number(env("ABORT_PRESSURE_MS", 30000)), abortFailPct = Number(env("ABORT_FAIL_PCT", 5));

  const pressure = async () => {
    try { return (await (await fetch(http + "/pressure", { signal: AbortSignal.timeout(10000) })).json()).meanMs; } catch { return NaN; }
  };
  // A store still draining an earlier run reads as this run's load.
  for (let waited = 0, p = await pressure(); p >= settleMs && waited < 600; waited += 10, p = await pressure()) {
    if (waited % 60 === 0) console.log(`waiting for /pressure to settle under ${settleMs} ms (now ${p} ms)`);
    await new Promise((r) => setTimeout(r, 10000));
  }

  const kids = Array.from({ length: nChildren }, () => fork(fileURLToPath(import.meta.url), ["--child", url, perSocket, lens], { stdio: "inherit" }));
  const next = (c, op) => new Promise((r) => { const h = (m) => { if (m.op === op) { c.off("message", h); r(m); } }; c.on("message", h); });
  await Promise.all(kids.map((c) => next(c, "ready")));

  const f = (x) => (Number.isNaN(x) || x === undefined ? "-" : Math.round(x));
  console.log(`${url}  per_socket ${perSocket}  hold ${hold}s  open_rate ${openRate}/s  lens ${lens}  started ${new Date().toISOString()}`);
  console.log("sockets feeds fail | open p50/p99 ms | eose p50/p99 ms | live dropped | events reach% dup spread p50/p99/max ms | pressure before/peak ms");
  let held = 0;
  for (const target of steps) {
    const add = target - held; if (add <= 0) continue;
    const before = await pressure(); let peak = before;
    const poll = setInterval(async () => { const p = await pressure(); if (!(p <= peak)) peak = p; }, 2000);

    const share = kids.map((_, i) => Math.floor(add / nChildren) + (i < add % nChildren ? 1 : 0));
    const opened = await Promise.all(kids.map((c, i) => { const w = next(c, "opened"); c.send({ op: "open", n: share[i], rate: openRate / nChildren }); return w; }));
    held = target;
    for (const c of kids) c.send({ op: "hold" });
    await new Promise((r) => setTimeout(r, hold * 1000));
    const reports = await Promise.all(kids.map((c) => { const w = next(c, "held"); c.send({ op: "report" }); return w; }));
    clearInterval(poll);

    const failed = opened.reduce((a, o) => a + o.failed, 0);
    const eose = opened.flatMap((o) => o.eose).sort((a, b) => a - b), openMs = opened.flatMap((o) => o.openMs).sort((a, b) => a - b);
    const why = {}; for (const o of opened) for (const [k, v] of Object.entries(o.why)) why[k] = (why[k] || 0) + v;
    const live = reports.reduce((a, r) => a + r.live, 0), dropped = reports.reduce((a, r) => a + r.dropped, 0);
    const duplicates = reports.reduce((a, r) => a + r.duplicates, 0);
    // The same event seen by sockets in several children: merge its count and its first and last arrival.
    const byId = new Map();
    for (const r of reports) for (const [id, s] of r.seen) {
      const e = byId.get(id);
      if (e) { e.count += s.count; e.first = Math.min(e.first, s.first); e.last = Math.max(e.last, s.last); } else byId.set(id, { ...s });
    }
    const spread = [...byId.values()].map((e) => e.last - e.first).sort((a, b) => a - b);
    const reach = byId.size && live ? (100 * [...byId.values()].reduce((a, e) => a + e.count, 0)) / byId.size / (live * perSocket) : NaN;

    console.log(`${String(target).padStart(7)} ${String(target * perSocket).padStart(5)} ${String(failed).padStart(4)} | ${f(quantile(openMs, 0.5))}/${f(quantile(openMs, 0.99))} | ${f(quantile(eose, 0.5))}/${f(quantile(eose, 0.99))} | ${live} ${dropped} | ${byId.size} ${Number.isNaN(reach) ? "-" : reach.toFixed(0)} ${duplicates} ${f(quantile(spread, 0.5))}/${f(quantile(spread, 0.99))}/${f(spread.at(-1))} | ${f(before)}/${f(peak)}`);
    if (Object.keys(why).length) console.log("   failures:", JSON.stringify(why));
    const reasons = [];
    if (quantile(eose, 0.99) >= abortEose) reasons.push(`eose p99 ${f(quantile(eose, 0.99))} ms`);
    if (peak >= abortPressure) reasons.push(`/pressure ${f(peak)} ms`);
    if ((100 * failed) / add >= abortFailPct) reasons.push(`${failed}/${add} sockets failed`);
    if (dropped) reasons.push(`${dropped} dropped`);
    if (reasons.length) { console.log(`stopped at ${target} sockets: ${reasons.join(", ")}`); break; }
  }
  console.log(`ended ${new Date().toISOString()}`);
  for (const c of kids) c.send({ op: "exit" });
  await new Promise((r) => setTimeout(r, 1500));
  process.exit(0);
}
