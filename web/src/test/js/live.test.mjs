// The relay's live statistics: what the page asks its own socket, how often, and what it keeps
// when an ask fails. The asks are the load this public page puts on the relay, so they are
// asserted as properties, not as today's list.
import assert from "node:assert/strict";
import { DAY, DAYS, HOUR, HOURS, LiveCounts, PEOPLE_KINDS, socketUrlOf, wantedAsks } from "../../main/resources/web/relay/live.js";

const ok = (name) => console.log(`  ✓ ${name}`);

/** 2026-10-16 20:41:07 UTC: inside an hour and a day, never on a boundary. */
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
  const asks = wantedAsks(NOW, [1, 7]);
  for (const a of asks) {
    assert.ok(a.filter.until != null || a.key.startsWith("people."), `${a.key} reaches past the present`);
    if (a.filter.until != null) assert.ok(a.filter.until <= NOW, `${a.key} counts the future-dated`);
  }
  ok("every window is bounded at now, so future-dated spam counts in none of them");
}

{
  // A COUNT with neither a window nor a sparse kind walks the whole store; a REQ is bounded by its limit.
  const asks = wantedAsks(NOW, [1, 7, 30382]);
  for (const a of asks) {
    if (a.verb === "req") {
      assert.ok(a.filter.limit > 0 && a.filter.limit <= 100, `${a.key} reads an unbounded page`);
      continue;
    }
    const bounded = a.filter.since != null || (a.filter.kinds && a.filter.kinds.every((k) => PEOPLE_KINDS.includes(k)));
    assert.ok(bounded, `${a.key} counts with neither a window nor a one-per-author kind: ${JSON.stringify(a.filter)}`);
  }
  ok("nothing the page asks unprompted walks the store");
}

{
  const asks = wantedAsks(NOW, []);
  const hours = asks.filter((a) => a.key.startsWith("hour.")).map((a) => a.filter);
  const days = asks.filter((a) => a.key.startsWith("day.")).map((a) => a.filter);
  assert.equal(hours.length, HOURS);
  assert.equal(days.length, DAYS);
  for (const [buckets, width] of [[hours, HOUR], [days, DAY]]) {
    for (const f of buckets) assert.equal(f.since % width, 0, "a bucket starts off its UTC boundary");
    // Newest first, each closing one second before the next opens: no event counts twice or never.
    for (let i = 1; i < buckets.length; i++) assert.equal(buckets[i].until, buckets[i - 1].since - 1);
    assert.equal(buckets[0].until, NOW, "the open bucket counts up to now");
  }
  ok("hourly and daily buckets are UTC-aligned, adjacent and disjoint");
}

{
  assert.equal(socketUrlOf("https://relay.example/stats.html"), "wss://relay.example/");
  assert.equal(socketUrlOf("http://localhost:7777/stats.html?x=1#y"), "ws://localhost:7777/");
  assert.equal(socketUrlOf("https://host.example/relay/stats.html"), "wss://host.example/relay/");
  ok("the socket is the one this page was served beside, behind any prefix");
}

{
  let clock = NOW;
  const relay = fakeRelay({ events: (f) => (f.limit === 1 ? [{ created_at: NOW - 5, kind: 1 }] : [{ kind: 1 }, { kind: 9041 }]) });
  const live = new LiveCounts(relay, { now: () => clock });
  live.setMirrored([1, 7]);
  await drain(live);

  assert.equal(live.value("newest"), NOW - 5);
  assert.equal(live.value("last.3600"), 7);
  assert.deepEqual(live.kinds(), [1, 7, 9041], "a kind seen among the newest events and not mirrored is still counted");
  assert.ok(live.get("kind.9041.86400"));
  assert.equal(live.get("store"), undefined, "the whole store was counted without being asked");
  assert.ok(relay.asked.every((a) => Object.keys(a.filter).length > 0), "an empty filter went out unprompted");

  const before = relay.asked.length;
  clock += 20;
  await drain(live);
  const reasked = relay.asked.slice(before).length;
  assert.ok(reasked > 0 && reasked < 10, `20 seconds on, ${reasked} asks were repeated; only the open windows are due`);
  ok("a first pass fills every panel and a later one re-asks only what has gone stale");
}

{
  const relay = fakeRelay();
  const live = new LiveCounts(relay, { now: () => NOW });
  await drain(live);
  live.countStore();
  await drain(live);
  assert.equal(live.value("store"), 7);
  assert.equal(relay.asked.filter((a) => Object.keys(a.filter).length === 0).length, 1);
  ok("the whole-store count is asked once per press");
}

{
  let clock = NOW;
  const relay = fakeRelay({ answer: (f) => (Object.keys(f).length ? 7 : { unanswered: "timeout" }) });
  const live = new LiveCounts(relay, { now: () => clock });
  live.countStore();
  await drain(live);
  clock += 3600;
  await drain(live);
  assert.equal(relay.asked.filter((a) => Object.keys(a.filter).length === 0).length, 1, "a failed walk was retried on a timer");
  assert.equal(live.get("store").error, "timed out");
  assert.equal(live.countingStore, false);
  ok("a whole-store count that fails is not retried until the next press");
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
  let clock = NOW;
  const live = new LiveCounts(fakeRelay(), { now: () => clock });
  await drain(live);
  clock += 2 * HOUR;
  await drain(live);
  const hourKeys = [...live.answers.keys()].filter((k) => k.startsWith("hour."));
  assert.equal(hourKeys.length, HOURS, "buckets that rolled off the chart are still held");
  ok("the answers held stay bounded as the clock moves");
}
