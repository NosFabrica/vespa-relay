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
