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

console.log("auth-required resend + publish + complete flag + challenge delivery: all assertions passed");
