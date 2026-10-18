// The reader's own group list: read once per reader and only off a finished answer, and its
// private half costs one prompt, ever, until the reader asks for the dialog back.
import assert from "node:assert/strict";

const { OwnGroups, canDecrypt } = await import(new URL("../../main/resources/web/owngroups.js", import.meta.url));

const ok = (name) => console.log(`  ✓ ${name}`);
const A = "a".repeat(64), B = "b".repeat(64);
const list = (complete = true) => {
  const evs = [{ kind: 10009, content: "AgSealedNip44", tags: [["group", "pub", "wss://g.example", "Public"]] }];
  evs.complete = complete;
  return evs;
};
const PLAIN = JSON.stringify([["group", "hidden", "wss://g.example", "Hidden"]]);

function rig({ decrypt = async () => PLAIN, answer = () => list() } = {}) {
  const t = { who: A, reads: 0, prompts: 0, unlocked: 0 };
  t.groups = new OwnGroups({
    who: () => t.who,
    read: async (filter) => { t.reads++; t.filter = filter; return answer(); },
    nostr: () => ({ nip44: { decrypt: async (...a) => { t.prompts++; return decrypt(...a); } } }),
    unlocked: () => { t.unlocked++; },
  });
  return t;
}

{
  const t = rig();
  const rows = await t.groups.candidates();
  assert.deepEqual(rows.map((g) => g.id), ["pub"]);
  assert.deepEqual(t.filter, { kinds: [10009], authors: [A], limit: 1 });
  await t.groups.candidates();
  assert.equal(t.reads, 1, "read once per reader");
  t.who = B;
  await t.groups.candidates();
  assert.equal(t.reads, 2, "another reader is another list");
  t.who = null;
  assert.deepEqual(await t.groups.candidates(), [], "signed out, nobody's list");
  assert.equal(t.groups.lockState(), null);
  ok("the list is read once per reader, and dropped when the reader changes");
}

{
  const t = rig({ answer: () => list(false) });
  assert.deepEqual(await t.groups.candidates(), [], "an unfinished answer is not the list");
  await t.groups.candidates();
  assert.equal(t.reads, 2, "…and is not cached as one");
  ok("only a finished answer is cached");
}

{
  const t = rig();
  await t.groups.candidates();
  assert.deepEqual(t.groups.lockState(), { state: "asking" });
  assert.ok(t.groups.shouldAsk());
  const [a, b] = await Promise.all([t.groups.unlock(), t.groups.unlock()]);
  assert.equal(t.prompts, 1, "two keystrokes, one prompt");
  assert.deepEqual(a.map((g) => g.id), ["hidden"]);
  assert.equal(a, b);
  assert.deepEqual(t.groups.opened().map((g) => g.secret), [true]);
  assert.equal(t.groups.lockState(), null, "opened, nothing left to say");
  await t.groups.unlock();
  assert.equal(t.prompts, 1, "…and never again once opened");
  assert.equal(t.unlocked, 1, "the picker is told when the dialog was answered");
  ok("the private half costs one prompt");
}

{
  const t = rig({ decrypt: async () => { throw new Error("denied"); } });
  await t.groups.candidates();
  assert.deepEqual(await t.groups.unlock(), []);
  assert.deepEqual(t.groups.lockState(), { state: "denied" });
  assert.ok(!t.groups.shouldAsk(), "a refusal is final until the reader asks");
  await t.groups.retry();
  assert.equal(t.prompts, 2, "the retry row asks again");
  ok("a refusal is not asked again unasked");
}

{
  const t = rig();
  t.groups.nostr = () => ({});
  await t.groups.candidates();
  assert.deepEqual(t.groups.lockState(), { state: "unsupported", scheme: "nip44" });
  assert.ok(!t.groups.shouldAsk());
  assert.equal(canDecrypt({ nip04: { decrypt: () => {} } }, "nip04"), true);
  assert.equal(canDecrypt(undefined, "nip44"), false);
  ok("an extension that cannot open the scheme is told apart from one that said no");
}

console.log("owngroups: one read per reader, one prompt per lock");
