// The relay's live panels on stats.html: what the page asks its own socket, how often, and what it
// keeps when an ask fails. The asks are the load this public page puts on the relay, so they are
// asserted as properties, not as today's list.
import assert from "node:assert/strict";
import { LiveCounts, PEOPLE_KINDS, socketUrlOf, wantedAsks } from "../../main/resources/web/relay/live.js";

const ok = (name) => console.log(`  ✓ ${name}`);

/** 2026-10-16 20:41:07 UTC. */
const NOW = 1792183267;

/** A relay that answers every COUNT with [answer] and every REQ with [events], recording what it was asked. */
function fakeRelay({ answer = () => 7, events = () => [] } = {}) {
  return {
    ws: { readyState: 1 },
    asked: [],
    async count(filter) {
      this.asked.push({ verb: "count", filter });
      return answer(filter);
    },
    async req(filter) {
      this.asked.push({ verb: "req", filter });
      const evs = events(filter);
      evs.complete = true;
      return evs;
    },
  };
}

/** Let every pending microtask and chained tick run. */
const settle = () => new Promise((r) => setTimeout(r, 0));

/** Drive [live] until nothing is due at its current clock. */
async function drain(live) {
  for (let i = 0; i < 200 && (live.due().length || live.inFlight.size); i++) {
    live.tick();
    await settle();
  }
}

{
  for (const a of wantedAsks(NOW)) {
    assert.ok(a.filter.until != null || a.key.startsWith("people."), `${a.key} reaches past the present`);
    if (a.filter.until != null) assert.ok(a.filter.until <= NOW, `${a.key} counts the future-dated`);
  }
  ok("every window is bounded at now, so future-dated spam counts in none of them");
}

{
  // A COUNT with neither a window nor a one-per-author kind walks the whole store; a REQ is bounded by its limit.
  for (const a of wantedAsks(NOW)) {
    if (a.verb === "req") {
      assert.ok(a.filter.limit > 0 && a.filter.limit <= 100, `${a.key} reads an unbounded page`);
      continue;
    }
    const bounded = a.filter.since != null || (a.filter.kinds && a.filter.kinds.every((k) => PEOPLE_KINDS.includes(k)));
    assert.ok(bounded, `${a.key} counts with neither a window nor a one-per-author kind: ${JSON.stringify(a.filter)}`);
  }
  ok("nothing the page asks counts the whole store");
}

{
  assert.equal(socketUrlOf("https://relay.example/stats.html"), "wss://relay.example/");
  assert.equal(socketUrlOf("http://localhost:7777/stats.html?x=1#y"), "ws://localhost:7777/");
  assert.equal(socketUrlOf("https://host.example/relay/stats.html"), "wss://host.example/relay/");
  ok("the socket is the one this page was served beside, behind any prefix");
}

{
  let clock = NOW;
  const relay = fakeRelay({ events: () => [{ created_at: NOW - 5, kind: 1 }] });
  const live = new LiveCounts(relay, { now: () => clock });
  await drain(live);

  assert.equal(live.value("newest"), NOW - 5);
  assert.equal(live.value("last.60"), 7);
  assert.equal(live.value("last.3600"), 7);
  for (const k of PEOPLE_KINDS) assert.equal(live.value(`people.${k}`), 7);
  assert.ok(relay.asked.every((a) => Object.keys(a.filter).length > 0), "an empty filter went out");

  const before = relay.asked.length;
  clock += 20;
  await drain(live);
  const reasked = relay.asked.slice(before).map((a) => JSON.stringify(a.filter));
  assert.equal(reasked.length, 3, `20 seconds on, ${reasked.length} asks were repeated; only the newest event and the two windows are due`);
  assert.ok(!reasked.some((f) => /"kinds"/.test(f)), "a people count was re-asked inside its cadence");
  ok("a first pass fills every tile and a later one re-asks only what has gone stale");
}

{
  let clock = NOW;
  let refuse = false;
  const relay = fakeRelay({ answer: () => (refuse ? { unanswered: "refused" } : 7) });
  const live = new LiveCounts(relay, { now: () => clock });
  await drain(live);
  refuse = true;
  clock += 20;
  await drain(live);
  const a = live.get("last.3600");
  assert.equal(a.value, 7, "a failed refresh threw away the last answer");
  assert.equal(a.error, "refused");
  // Retried, but not on every tick.
  const before = relay.asked.length;
  clock += 1;
  await drain(live);
  assert.equal(relay.asked.length, before, "a refused ask was retried at once");
  ok("a failed refresh keeps the previous answer and says why");
}

{
  const relay = fakeRelay();
  relay.ws = null;
  relay.count = async () => { throw new Error("relay connection failed"); };
  relay.req = relay.count;
  const live = new LiveCounts(relay, { now: () => NOW });
  live.tick();
  await settle();
  assert.equal(live.connError, "relay connection failed");
  const calls = live.inFlight.size;
  live.tick();
  assert.equal(live.inFlight.size, calls, "a dead socket is dialled again on the next tick");
  ok("a socket that cannot connect is retried after a pause, not in a loop");
}

{
  const relay = fakeRelay();
  const live = new LiveCounts(relay, { now: () => NOW });
  live.paused = true;
  live.tick();
  await settle();
  assert.equal(relay.asked.length, 0);
  ok("a hidden tab asks nothing");
}

{
  // The whole-kind counts are the expensive ones: one at a time, and never in place of a cheap ask.
  const pending = [];
  const relay = fakeRelay({ answer: (f) => (f.kinds ? new Promise((r) => pending.push(r)) : 7) });
  relay.count = async function (filter) {
    this.asked.push({ verb: "count", filter });
    return filter.kinds ? new Promise((r) => pending.push(r)) : 7;
  };
  const live = new LiveCounts(relay, { now: () => NOW });
  live.tick();
  await settle();
  const heavyAsked = () => relay.asked.filter((a) => a.filter.kinds).length;
  assert.equal(heavyAsked(), 1, "two whole-kind counts were in flight at once");
  assert.equal(live.value("last.60"), 7, "a cheap ask waited behind a whole-kind count");
  pending.shift()(60547435);
  await settle();
  live.tick();
  await settle();
  assert.equal(heavyAsked(), 2, "the next whole-kind count did not start when the first answered");
  // Answer the rest, so no deadline timer outlives the test.
  for (let i = 0; i < 5 && pending.length; i++) {
    while (pending.length) pending.shift()(1);
    await settle();
  }
  assert.equal(heavyAsked(), 3);
  ok("at most one whole-kind count is in flight, and the cheap asks never wait for it");
}

{
  // A count the engine is too busy for is retried later each time, never every 30s for good.
  let clock = NOW;
  const relay = fakeRelay({ answer: (f) => (f.kinds ? { unanswered: "timeout" } : 7) });
  const live = new LiveCounts(relay, { now: () => clock });
  const kind0 = () => relay.asked.filter((a) => a.filter.kinds && a.filter.kinds[0] === 0).length;
  await drain(live);
  assert.equal(kind0(), 1);
  const at = [];
  for (let s = 0; s < 1800 && at.length < 4; s++) {
    clock++;
    const before = kind0();
    await drain(live);
    if (kind0() > before) at.push(clock - NOW);
  }
  assert.deepEqual(at, [30, 90, 210, 450], "the waits between retries do not double");
  assert.equal(live.get("people.0").failures, 5);
  ok("a failing count backs off, doubling up to its own cadence");
}

{
  // The socket's own watchdog is an idle window the cheap asks keep resetting; this deadline is not.
  const relay = fakeRelay();
  relay.count = async (filter) => (filter.kinds ? new Promise(() => {}) : 7);
  const live = new LiveCounts(relay, { now: () => NOW, countDeadlineMs: 5 });
  live.tick();
  await new Promise((r) => setTimeout(r, 30));
  assert.equal(live.get("people.0").error, "timed out");
  assert.ok(!live.inFlight.has("people.0"), "an unanswered count held its slot");
  ok("a count the relay never answers gives its slot back");
}
