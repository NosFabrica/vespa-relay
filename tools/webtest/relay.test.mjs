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

console.log("auth-required resend: all assertions passed");
