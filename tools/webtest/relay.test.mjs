import assert from 'assert';
const { Relay } = await import(new URL("../../relay/src/main/resources/web/shared/relay.js", import.meta.url));

// Drive req()'s retry logic through a stubbed reqOnce — the wiring under
// test is exactly: CLOSED auth-required -> onAuthRequired -> resend once.
function stubbed(failures, reason) {
  const r = new Relay("ws://unused/");
  let calls = 0, hookCalls = 0;
  r.reqOnce = async () => {
    calls++;
    if (calls <= failures) throw new Error(reason);
    return [{ id: "ev1" }];
  };
  r.onAuthRequired = async () => { hookCalls++; r.authed = true; };
  return { r, get calls() { return calls; }, get hookCalls() { return hookCalls; } };
}

// auth-required with a hook: authenticate, resend, deliver.
let t = stubbed(1, "auth-required: this relay ranks per reader");
assert.deepStrictEqual(await t.r.req({}), [{ id: "ev1" }]);
assert.strictEqual(t.calls, 2, "resent exactly once");
assert.strictEqual(t.hookCalls, 1, "authenticated exactly once");

// auth-required again even after auth: the refusal is the answer.
t = stubbed(2, "auth-required: still no");
await assert.rejects(() => t.r.req({}), /auth-required/);
assert.strictEqual(t.calls, 2, "one retry only, never a loop");

// no hook installed (the anonymous reference connection): error surfaces.
t = stubbed(1, "auth-required: nope");
t.r.onAuthRequired = null;
await assert.rejects(() => t.r.req({}), /auth-required/);
assert.strictEqual(t.calls, 1, "no silent auth on an anonymous connection");

// a different CLOSED reason: not an auth problem, no retry.
t = stubbed(1, "restricted: not for you");
await assert.rejects(() => t.r.req({}), /restricted/);
assert.strictEqual(t.calls, 1);
assert.strictEqual(t.hookCalls, 0, "restricted is not auth-required");

// already authed and still auth-required: retry cannot help, don't.
t = stubbed(1, "auth-required: confused relay");
t.r.authed = true;
await assert.rejects(() => t.r.req({}), /auth-required/);
assert.strictEqual(t.calls, 1);

// publish(): the relay's OK verdict decides, and both verdicts surface.
{
  const r = new Relay("ws://unused/");
  r.connect = async () => {};
  const sent = [];
  r.ws = { send: (m) => sent.push(m) };
  const accepted = r.publish({ id: "e1" });
  await new Promise((res) => setTimeout(res, 0)); // publish awaits connect() before arming its waiter
  r.handle(["OK", "e1", true, ""]);
  await accepted;
  assert.strictEqual(sent.length, 1, "publish sends once");
  assert(sent[0].startsWith('["EVENT"'), "publish sends EVENT");
}
{
  const r = new Relay("ws://unused/");
  r.connect = async () => {};
  r.ws = { send: () => {} };
  const rejected = r.publish({ id: "e2" });
  await new Promise((res) => setTimeout(res, 0));
  r.handle(["OK", "e2", false, "invalid: bad signature"]);
  await assert.rejects(() => rejected, /bad signature/, "a rejection carries the relay's reason");
}

// reqOnce(): EOSE marks the result complete; a timeout marks it partial.
// enrichProfiles caches "no profile" only off complete reads — a timed-out
// read is not the relay stating absence.
{
  const r = new Relay("ws://unused/");
  r.connect = async () => {};
  r.ws = { send: () => {} };
  const p = r.reqOnce({}, 1000);
  await new Promise((res) => setTimeout(res, 0));
  r.handle(["EVENT", "sot1", { id: "x" }]);
  r.handle(["EOSE", "sot1"]);
  const evs = await p;
  assert.strictEqual(evs.length, 1);
  assert.strictEqual(evs.complete, true, "EOSE -> complete");

  const partial = await r.reqOnce({}, 20); // nothing answers; the timer fires
  assert.strictEqual(partial.complete, false, "timeout -> partial");
}

// A REQ carries as many filters as it was given. NIP-01 ORs the filters of one
// subscription, and that is how a hashtag search asks its union question —
// `#t`, plus the NIP-22 comments naming the same topic in `i`/`I` — in one
// round trip with one EOSE. Sent as a nested array they would be one malformed
// filter, and the relay's answer to that is nothing at all.
{
  const r = new Relay("ws://unused/");
  r.connect = async () => {};
  const sent = [];
  r.ws = { send: (m) => sent.push(JSON.parse(m)) };

  r.reqOnce({ "#t": ["nostr"] }, 20);
  await new Promise((res) => setTimeout(res, 0));
  assert.deepStrictEqual(sent[0], ["REQ", "sot1", { "#t": ["nostr"] }], "one filter is still one filter");

  r.reqOnce([{ "#t": ["nostr"] }, { kinds: [1111], "#I": ["#nostr"] }], 20);
  await new Promise((res) => setTimeout(res, 0));
  assert.deepStrictEqual(
    sent[1],
    ["REQ", "sot2", { "#t": ["nostr"] }, { kinds: [1111], "#I": ["#nostr"] }],
    "several filters are spread into the REQ, not nested inside one",
  );
}

// waitForChallenge(): the NIP-42 challenge is DELIVERED, never polled for.
//
// The property that matters is the latency, so it is what is asserted: a
// challenge arriving on the socket must wake the waiter in that turn, not on
// the next tick of some interval. This replaced a `while (!challenge) await
// sleep(100)` in the page, which sat on the critical path of every load —
// sign-in gates the first REQ — and averaged 50ms of nothing.
{
  const r = new Relay("ws://unused/");
  let resolved = false;
  const p = r.waitForChallenge(5000).then((c) => { resolved = true; return c; });
  await new Promise((res) => setTimeout(res, 0));
  assert.strictEqual(resolved, false, "nothing to hand over yet");
  r.handle(["AUTH", "chal-1"]);
  assert.strictEqual(await p, "chal-1", "the AUTH message itself wakes the waiter");

  // Already known: answered without a trip through the event loop's timers.
  assert.strictEqual(await r.waitForChallenge(5000), "chal-1", "a known challenge is immediate");

  // No challenge inside the budget: null, and the waiter is not left behind.
  const r2 = new Relay("ws://unused/");
  assert.strictEqual(await r2.waitForChallenge(10), null, "timeout -> null");
  assert.strictEqual(r2.challengeWaiters.length, 0, "a timed-out waiter unregisters itself");

  // A socket that closes will never deliver one. Say so now rather than
  // spending the caller's whole budget waiting on a dead connection.
  const r3 = new Relay("ws://unused/");
  const dead = r3.waitForChallenge(60000);
  await new Promise((res) => setTimeout(res, 0));
  r3.wakeChallengeWaiters();
  assert.strictEqual(await dead, null, "close wakes waiters with the absence");
}

// count(): every way a COUNT can end is an ANSWER the caller has to tell apart.
//
// The three non-answers are the point. A relay that does not implement NIP-45
// declines in one of two dialects — it closes the subscription, or it sends a
// NOTICE, which carries no subscription id and so cannot be addressed to the
// ask it is about — and a socket that dies is a third. Every one of them must
// SETTLE the promise: the readiness panel awaits these before it can say
// anything, and a count that never settles is a panel that never appears.
{
  const { REFUSED, TIMED_OUT } = await import(new URL("../../relay/src/main/resources/web/shared/relay.js", import.meta.url));
  const armed = () => {
    const r = new Relay("ws://unused/");
    r.connect = async () => {};
    const sent = [];
    r.ws = { send: (m) => sent.push(JSON.parse(m)) };
    return { r, sent };
  };

  // The ordinary answer.
  {
    const { r, sent } = armed();
    const p = r.count({ kinds: [30382] });
    await new Promise((res) => setTimeout(res, 0));
    assert.strictEqual(sent[0][0], "COUNT", "count sends COUNT");
    r.handle(["COUNT", sent[0][1], { count: 145968 }]);
    assert.strictEqual(await p, 145968);
    assert(sent.some((m) => m[0] === "CLOSE"), "the subscription is closed however it ends");
  }

  // CLOSED: the relay declined. Its answer, not our budget.
  {
    const { r, sent } = armed();
    const p = r.count({});
    await new Promise((res) => setTimeout(res, 0));
    r.handle(["CLOSED", sent[0][1], "unsupported"]);
    assert.strictEqual(await p, REFUSED);
  }

  // A COUNT frame with no number in it is the same refusal in a third dialect.
  {
    const { r, sent } = armed();
    const p = r.count({});
    await new Promise((res) => setTimeout(res, 0));
    r.handle(["COUNT", sent[0][1], {}]);
    assert.strictEqual(await p, REFUSED);
  }

  // NOTICE: unaddressable, so it is attributed to what is outstanding.
  //
  // This is the regression. failCounts() used to clear the map before calling
  // the resolvers, and each resolver begins by deleting its own entry to stay
  // idempotent — so every one of them returned early without resolving, and
  // the caller awaited a promise nothing would ever settle. Measured against a
  // stubbed nip85.brainstorm.world, which answers exactly this way: the panel
  // never appeared at all.
  {
    const { r } = armed();
    const p = r.count({});
    await new Promise((res) => setTimeout(res, 0));
    r.handle(["NOTICE", "COUNT not supported"]);
    assert.strictEqual(await p, REFUSED, "a NOTICE settles the outstanding count");
    assert.strictEqual(r.counts.size, 0, "and leaves nothing behind in the map");
  }

  // Two outstanding at once: one NOTICE settles both, and neither is dropped.
  {
    const { r } = armed();
    const both = Promise.all([r.count({}), r.count({})]);
    await new Promise((res) => setTimeout(res, 0));
    r.handle(["NOTICE", "no"]);
    assert.deepStrictEqual(await both, [REFUSED, REFUSED]);
  }

  // The idle watchdog is an idle window, not a deadline: a message on the
  // connection about something else entirely is proof the relay is still
  // working through its queue, so it postpones the give-up.
  {
    const { r, sent } = armed();
    r.countIdle = null;
    const p = r.count({});
    await new Promise((res) => setTimeout(res, 0));
    const first = r.countIdle;
    r.handle(["EVENT", "somebody-elses-sub", { id: "x" }]);
    assert.notStrictEqual(r.countIdle, first, "traffic re-arms the window");
    r.handle(["COUNT", sent[0][1], { count: 1 }]);
    assert.strictEqual(await p, 1);
    assert.strictEqual(r.countIdle, null, "and it disarms when nothing is outstanding");
  }

  // TIMED_OUT is reachable and distinct — it is what "we stopped waiting"
  // resolves to, and the panel draws it differently from a refusal.
  {
    const { r } = armed();
    const p = r.count({});
    await new Promise((res) => setTimeout(res, 0));
    r.failCounts(TIMED_OUT);
    assert.strictEqual(await p, TIMED_OUT);
    assert.notStrictEqual(TIMED_OUT, REFUSED, "the two non-answers are never the same value");
  }
}

console.log("auth-required resend + publish + complete flag + multi-filter REQ + challenge delivery + COUNT non-answers: all assertions passed");
