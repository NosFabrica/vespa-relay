// What a git permalink shows under its card: the ask and the shape, both
// pure functions in web/related.js with one `conn.req` between them.
import assert from 'assert';
globalThis.location = { protocol: "http:", host: "localhost:7787" };
globalThis.window = { addEventListener: () => {} };

const { relatedAsk, relatedShape, relatedHtml, relatedPeople } =
  await import(new URL("../../main/resources/web/related.js", import.meta.url));
const { seedProfiles } = await import(new URL("../../main/resources/web/shared/profiles.js", import.meta.url));

const pk = "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2";
const pk2 = "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d";
const id = (c) => c.repeat(64);
const now = Math.floor(Date.now() / 1000);
const ev = (kind, tags = [], content = "", eid = id("0"), who = pk, ago = 0) =>
  ({ id: eid, pubkey: who, kind, created_at: now - ago, tags, content });

const REPO = ev(30617, [["d", "vespa-relay"], ["name", "vespa-relay"]]);
const ADDR = `30617:${pk}:vespa-relay`;

// The state is addressed by `d` and carries no `a`, so an `#a` ask alone
// would never reach it; hence two filters in one req.
const repoAsk = relatedAsk(REPO);
assert.strictEqual(repoAsk.length, 2, "a repository asks its items and its state");
assert.deepStrictEqual(repoAsk[0]["#a"], [ADDR], "items are tagged with the repo's address");
for (const k of [1617, 1618, 1621, 30063]) {
  assert(repoAsk[0].kinds.includes(k), `the repo ask must reach kind ${k}`);
}
assert.deepStrictEqual(repoAsk[1], { kinds: [30618], authors: [pk], "#d": ["vespa-relay"], limit: 1 },
  "the state is asked by author and `d`, because it carries no `a` at all");

for (const kind of [1617, 1618, 1619, 1621]) {
  const ask = relatedAsk(ev(kind));
  assert.deepStrictEqual(ask[0]["#e"], [id("0")], `kind ${kind} asks what tags it`);
  for (const k of [1630, 1631, 1632, 1633, 1622]) {
    assert(ask[0].kinds.includes(k), `kind ${kind}'s thread must reach kind ${k}`);
  }
}

for (const kind of [1, 0, 30023, 1622, 1630, 30618, 30063]) {
  assert.strictEqual(relatedAsk(ev(kind)), null, `kind ${kind} has no related question`);
}
assert.strictEqual(relatedAsk(ev(30617)), null, "no `d`, no address, no ask");
assert.strictEqual(relatedAsk(null), null, "no event, no ask");

const state = ev(30618, [["d", "vespa-relay"], ["refs/heads/main", "abc1234"]], "", id("a"), pk, 60);
const items = [
  state,
  ev(1621, [["a", ADDR], ["subject", "older issue"]], "", id("b"), pk2, 9000),
  ev(1621, [["a", ADDR], ["subject", "newer issue"]], "", id("c"), pk2, 100),
  ev(1617, [["a", ADDR]], "diff", id("d"), pk, 500),
  ev(1618, [["a", ADDR], ["subject", "a pr"]], "", id("e"), pk, 700),
  ev(30063, [["a", ADDR], ["d", "vespa-relay@v1"]], "", id("f"), pk, 800),
];
const repoShape = relatedShape(REPO, items);
assert.deepStrictEqual(repoShape.sections.map((s) => s.head),
  ["state", "2 issues", "1 patch", "1 pull request", "1 release"],
  "a repository's page is its state, then each family it holds, counted and singular when it is one");
assert.deepStrictEqual(repoShape.sections[1].events.map((e) => e.id), [id("c"), id("b")],
  "a list is newest first — the newest issue is the live one");

const ISSUE = ev(1621, [["a", ADDR], ["subject", "it breaks"]], "", id("9"));
const thread = relatedShape(ISSUE, [
  ev(1622, [["e", id("9")]], "second", id("2"), pk2, 100),
  ev(1622, [["e", id("9")]], "first", id("1"), pk2, 9000),
  ev(1630, [["e", id("9")]], "reopened", id("3"), pk, 5000),
  ev(1632, [["e", id("9")]], "closed for good", id("4"), pk, 50),
]);
assert.deepStrictEqual(thread.sections[0].events.map((e) => e.content), ["first", "second"],
  "replies read oldest first");
assert.strictEqual(thread.status.id, id("4"), "the NEWEST status is the current verdict");
assert(!thread.sections[0].events.some((e) => e.kind === 1630 || e.kind === 1632),
  "a status is the verdict, not another reply in the list");

const selfEchoed = relatedShape(ISSUE, [ISSUE, ev(1622, [["e", id("9")]], "hi", id("5"))]);
assert.deepStrictEqual(selfEchoed.events.map((e) => e.id), [id("5")], "the subject is not related to itself");
const dupes = relatedShape(ISSUE, [ev(1622, [], "hi", id("5")), ev(1622, [], "hi", id("5"))]);
assert.strictEqual(dupes.events.length, 1, "one event, one card");

// A `#e` ask returns citations too; NIP-10 marks those `mention`.
const quoted = relatedShape(ISSUE, [
  ev(1, [["e", id("9"), "", "mention"]], "look at this issue", id("7"), pk2, 10),
  ev(1622, [["e", id("9")]], "a real answer", id("8"), pk2, 20),
]);
assert.deepStrictEqual(quoted.sections[0].events.map((e) => e.content), ["a real answer"],
  "a mention is not a reply");
assert.strictEqual(relatedShape(ISSUE, [
  ev(1622, [["e", id("9"), "", "mention"], ["e", id("9"), "", "root"]], "both", id("7")),
]).sections[0].events.length, 1, "one marker of many being `mention` does not disqualify it");

// The ask stops at a limit, so a full answer is a floor, not a total.
const many = Array.from({ length: 120 }, (_, n) =>
  ev(1621, [["a", ADDR], ["subject", `issue ${n}`]], "", id("0").slice(0, 63) + (n % 10), pk2, n));
const cappedRead = relatedShape(REPO, many.map((e, n) => ({ ...e, id: `${n}`.padStart(64, "0") })));
assert(cappedRead.sections[0].head.includes("+"),
  "a full answer is a floor, not a total — the head says so");
const timedOut = Object.assign([ev(1621, [["a", ADDR]], "", id("b"), pk2, 5)], { complete: false });
assert(relatedShape(REPO, timedOut).sections[0].head.includes("+"),
  "…and so is an answer the relay never finished");
assert.strictEqual(relatedShape(REPO, Object.assign([...items], { complete: true })).sections[1].head, "2 issues",
  "a complete answer under the cap is an exact count");

assert.strictEqual(relatedHtml(relatedShape(REPO, [])), "", "an empty answer draws nothing at all");
assert.strictEqual(relatedHtml(null), "", "…and so does no answer");

seedProfiles([{ kind: 0, pubkey: pk, content: JSON.stringify({ name: "alice" }) }]);
const html = relatedHtml(thread);
assert(html.includes("status-pill lead closed"), "the verdict leads, as the pill the cards already speak in");
assert(html.includes(">alice<"), "and says who reached it, by name");
assert(html.includes("2 replies") && html.includes("<article"), "the thread is drawn as cards");

const repoHtml = relatedHtml(repoShape);
assert(!repoHtml.includes("repo-line"),
  "a repo's page must not print `in vespa-relay` once per card under its own title");
assert(relatedHtml(relatedShape(ev(1621, [["a", ADDR]], "", id("9")), [
  ev(1622, [["e", id("9")], ["a", `30617:${pk}:other-repo`]], "hi", id("5")),
])).includes("repo-line"),
  "…but a card from a DIFFERENT repository still says which one");

const people = relatedPeople(repoShape);
assert(people.includes(pk2) && people.includes(pk), "every author of a drawn card is a name the page owes itself");
assert(people.every((p) => /^[0-9a-f]{64}$/.test(p)), "and nothing that is not a pubkey");

console.log("related: the ask, the two orders, the verdict, and the line a repo's own page does not repeat");
