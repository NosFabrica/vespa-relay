import assert from 'assert';
const { Relay } = await import(new URL("../../main/resources/web/shared/relay.js", import.meta.url));

// req()'s retry wiring through a stubbed reqOnce: CLOSED auth-required -> onAuthRequired -> resend once.
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

let t = stubbed(1, "auth-required: this relay ranks per reader");
assert.deepStrictEqual(await t.r.req({}), [{ id: "ev1" }]);
assert.strictEqual(t.calls, 2, "resent exactly once");
assert.strictEqual(t.hookCalls, 1, "authenticated exactly once");

t = stubbed(2, "auth-required: still no");
await assert.rejects(() => t.r.req({}), /auth-required/);
assert.strictEqual(t.calls, 2, "one retry only, never a loop");

t = stubbed(1, "auth-required: nope");
t.r.onAuthRequired = null;
await assert.rejects(() => t.r.req({}), /auth-required/);
assert.strictEqual(t.calls, 1, "no silent auth on an anonymous connection");

t = stubbed(1, "restricted: not for you");
await assert.rejects(() => t.r.req({}), /restricted/);
assert.strictEqual(t.calls, 1);
assert.strictEqual(t.hookCalls, 0, "restricted is not auth-required");

t = stubbed(1, "auth-required: confused relay");
t.r.authed = true;
await assert.rejects(() => t.r.req({}), /auth-required/);
assert.strictEqual(t.calls, 1);

// An abort ends a REQ the way its timeout does: CLOSE to the relay, what arrived resolved,
// `complete` false.
{
  const r = new Relay("ws://unused/");
  r.connect = async () => {};
  const sent = [];
  r.ws = { send: (m) => sent.push(m) };
  const ctl = new AbortController();
  const asked = r.req({ kinds: [1], search: "bitco include:spam" }, 5000, { signal: ctl.signal });
  await new Promise((res) => setTimeout(res, 0));
  const id = JSON.parse(sent[0])[1];
  r.handle(["EVENT", id, { id: "partial" }]);
  ctl.abort();
  const got = await asked;
  assert.deepStrictEqual([...got], [{ id: "partial" }], "what arrived before the abort is handed back");
  assert.strictEqual(got.complete, false, "…marked incomplete, so nothing caches it as the answer");
  assert.strictEqual(sent.length, 2, "one REQ, one CLOSE");
  assert(sent[1].startsWith(`["CLOSE","${id}"`), "the relay is told to stop");
  r.handle(["EOSE", id]);
  assert.strictEqual(sent.length, 2, "a late EOSE for a closed ask does nothing");
  const dead = new AbortController(); dead.abort();
  const never = await r.req({ kinds: [1] }, 5000, { signal: dead.signal });
  assert.strictEqual(never.complete, false);
  assert.strictEqual(sent.length, 2, "an ask aborted before it was sent sends nothing");
}
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

// NIP-01 ORs the filters of one subscription; nested, they would be one malformed filter.
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

// waitForChallenge(): the NIP-42 challenge wakes the waiter in the turn it arrives, never on a poll.
{
  const r = new Relay("ws://unused/");
  let resolved = false;
  const p = r.waitForChallenge(5000).then((c) => { resolved = true; return c; });
  await new Promise((res) => setTimeout(res, 0));
  assert.strictEqual(resolved, false, "nothing to hand over yet");
  r.handle(["AUTH", "chal-1"]);
  assert.strictEqual(await p, "chal-1", "the AUTH message itself wakes the waiter");

  assert.strictEqual(await r.waitForChallenge(5000), "chal-1", "a known challenge is immediate");

  const r2 = new Relay("ws://unused/");
  assert.strictEqual(await r2.waitForChallenge(10), null, "timeout -> null");
  assert.strictEqual(r2.challengeWaiters.length, 0, "a timed-out waiter unregisters itself");

  const r3 = new Relay("ws://unused/");
  const dead = r3.waitForChallenge(60000);
  await new Promise((res) => setTimeout(res, 0));
  r3.wakeChallengeWaiters();
  assert.strictEqual(await dead, null, "close wakes waiters with the absence");
}

// count(): every way a COUNT can end must settle the promise, a NOTICE without a subscription
// id and a dead socket included.
{
  const { REFUSED, TIMED_OUT } = await import(new URL("../../main/resources/web/shared/relay.js", import.meta.url));
  const armed = () => {
    const r = new Relay("ws://unused/");
    r.connect = async () => {};
    const sent = [];
    r.ws = { send: (m) => sent.push(JSON.parse(m)) };
    return { r, sent };
  };

  {
    const { r, sent } = armed();
    const p = r.count({ kinds: [30382] });
    await new Promise((res) => setTimeout(res, 0));
    assert.strictEqual(sent[0][0], "COUNT", "count sends COUNT");
    r.handle(["COUNT", sent[0][1], { count: 145968 }]);
    assert.strictEqual(await p, 145968);
    assert(sent.some((m) => m[0] === "CLOSE"), "the subscription is closed however it ends");
  }

  {
    const { r, sent } = armed();
    const p = r.count({});
    await new Promise((res) => setTimeout(res, 0));
    r.handle(["CLOSED", sent[0][1], "unsupported"]);
    assert.strictEqual(await p, REFUSED);
  }

  {
    const { r, sent } = armed();
    const p = r.count({});
    await new Promise((res) => setTimeout(res, 0));
    r.handle(["COUNT", sent[0][1], {}]);
    assert.strictEqual(await p, REFUSED);
  }

  // A NOTICE is unaddressable, so it is attributed to every outstanding count.
  {
    const { r } = armed();
    const p = r.count({});
    await new Promise((res) => setTimeout(res, 0));
    r.handle(["NOTICE", "COUNT not supported"]);
    assert.strictEqual(await p, REFUSED, "a NOTICE settles the outstanding count");
    assert.strictEqual(r.counts.size, 0, "and leaves nothing behind in the map");
  }

  {
    const { r } = armed();
    const both = Promise.all([r.count({}), r.count({})]);
    await new Promise((res) => setTimeout(res, 0));
    r.handle(["NOTICE", "no"]);
    assert.deepStrictEqual(await both, [REFUSED, REFUSED]);
  }

  // The watchdog is an idle window, not a deadline: any traffic re-arms it.
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
