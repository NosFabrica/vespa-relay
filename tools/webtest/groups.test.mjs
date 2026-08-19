// Which NIP-29 group the picker offers for a half-typed `group:` — the
// ordering, and the one thing it must never do.
//
// The rule under test is that a group's identity is the pair (id, host relay)
// and this page holds that pair in TWO incompatible spellings: a 10009 `group`
// tag names the host as a url, a 39000 names it as the pubkey that signed it,
// and nothing joins the two. So the assertions below are mostly about what
// does NOT happen — no merge, no invented url, no silent collapse of two
// relays' groups into one row.
import assert from "assert";
import { readFileSync } from "node:fs";

const { ownGroups, metaGroup, rank, where, relayLabel, isNip04, sealed, privateGroups } =
  await import(new URL("../../web/src/main/resources/web/shared/groups.js", import.meta.url));

const HOST_A = "a".repeat(64);
const HOST_B = "b".repeat(64);

const meta = (id, pubkey, name, about = "") => ({
  kind: 39000,
  pubkey,
  tags: [["d", id], ["name", name], ["about", about]],
});

// ---- ownGroups: the reader's own list -------------------------------------

const list = {
  kind: 10009,
  tags: [
    ["group", "chachi", "wss://relay.groups.nip29.com/", "Chachi"],
    ["group", "zaps", "wss://groups.0xchat.com"],
    ["group", "broken"],                                  // no host
    ["group", "", "wss://x.example"],                     // no id
    ["r", "wss://not.a.group"],
    ["group", "chachi", "wss://relay.groups.nip29.com/"], // the same group twice
  ],
};

let own = ownGroups(list);
assert.strictEqual(own.length, 2, "only the tags carrying BOTH an id and a host are groups");
assert.deepStrictEqual(own.map((g) => g.id), ["chachi", "zaps"], "…in list order");
assert.strictEqual(own[0].name, "Chachi", "the optional name is read when it is there");
assert.strictEqual(own[1].name, "", "…and is empty, not undefined, when it is not");
assert.strictEqual(own[0].relayUrl, "wss://relay.groups.nip29.com/", "the url is kept EXACTLY as tagged");
assert.strictEqual(own[0].host, null, "a list entry names its host as a url and never as a key");
assert(own.every((g) => g.mine), "everything here is the reader's own");

assert.deepStrictEqual(ownGroups(null), [], "no event is no groups");
assert.deepStrictEqual(ownGroups({ tags: [] }), [], "…and neither is an empty one");

// ---- the LOCKED half of a group list --------------------------------------
//
// A 10009 is a NIP-51 private-tag event: items may sit encrypted in `.content`
// instead of in the tags. Opening it costs a permission prompt from the
// reader's extension, so the two questions this has to get right are which
// scheme the payload uses (a wrong guess is a wasted prompt) and when there is
// anything to ask about at all.

// The scheme is told apart by SHAPE. NIP-04 appends `?iv=` plus 24 base64
// characters, so the marker sits exactly 28 from the end; NIP-44 is one blob.
const IV = "?iv=" + "a".repeat(24);
assert.strictEqual(isNip04("ciphertextbase64" + IV), true, "the `?iv=` tail is NIP-04");
assert.strictEqual(isNip04("AglkjhasdlkjhasdlkjhasdlkjhasdIkjh"), false, "a bare base64 blob is NIP-44");
assert.strictEqual(isNip04("ciphertextbase64" + IV + "-null"), true, "…and quartz's `-null` bug is stripped first");
assert.strictEqual(isNip04("short"), false, "something too short to carry the marker is not NIP-04");
// The marker has to be AT that offset, not merely present — otherwise a NIP-44
// payload whose base64 happens to contain the sequence would be misrouted.
assert.strictEqual(isNip04(IV + "trailingtrailingtrailingtrailing"), false, "the offset is the test, not the substring");

assert.strictEqual(sealed({ content: "" }), null, "no payload, nothing to unlock, nothing to ask");
assert.strictEqual(sealed({ content: "   " }), null, "…and whitespace is no payload");
assert.strictEqual(sealed({}), null, "…nor is a missing content");
assert.deepStrictEqual(
  sealed({ content: "AgSomeNip44Payload" }),
  { content: "AgSomeNip44Payload", scheme: "nip44" },
  "a payload says which scheme to ask the extension for",
);
assert.strictEqual(sealed({ content: "abc" + IV }).scheme, "nip04", "…including the older one");

// What comes back once the reader has unlocked it: the same tag array shape as
// the public half, so every rule ownGroups states applies unchanged.
let secret = privateGroups(JSON.stringify([
  ["group", "hidden", "wss://groups.example", "Hidden"],
  ["group", "nohost"],
  ["p", "not a group"],
]));
assert.strictEqual(secret.length, 1, "the private half is parsed by the same rules as the public one");
assert.deepStrictEqual(
  [secret[0].id, secret[0].relayUrl, secret[0].name, secret[0].mine, secret[0].secret],
  ["hidden", "wss://groups.example", "Hidden", true, true],
  "…and marks where it came from, so a reader can see which of theirs the network can read",
);

// THE CASE THAT COSTS A PROMPT FOR NOTHING, and is ordinary rather than
// corrupt: an EMPTY private list encrypts the empty STRING, not `[]`. So a
// reader who removed their last private group still carries a valid payload,
// and the only way to learn it is empty is to ask. That is a real answer and
// gets cached like one — what must not happen is an error.
assert.deepStrictEqual(privateGroups(""), [], "the empty string an empty private list encrypts to");
assert.deepStrictEqual(privateGroups("[]"), [], "…and an explicitly empty array");
assert.deepStrictEqual(privateGroups("not json at all"), [], "a wrong-scheme decrypt is gibberish, not a crash");
assert.deepStrictEqual(privateGroups('{"tags":[]}'), [], "…and JSON that is not a tag array is no groups");
assert.deepStrictEqual(privateGroups(null), [], "…nor is nothing");

// Unlocked rows join the reader's own: rank() dedupes them on (id, host), so a
// group in BOTH halves collapses to one row — the PUBLIC one, which is right.
// It is not a secret if the tag is in the clear.
const bothHalves = { kind: 10009, tags: [["group", "hidden", "wss://groups.example", "Hidden"]] };
let merged = rank("hidden", { own: [...ownGroups(bothHalves), ...secret], meta: [] });
assert.strictEqual(merged.length, 1, "a group listed publicly AND privately is one row");
assert(!merged[0].secret, "…and it is the public one: a tag in the clear is not a secret");
assert.strictEqual(merged[0].ambiguous, false, "…so it is not ambiguous either");

// ---- metaGroup: what the corpus knows -------------------------------------

const m = metaGroup(meta("chachi", HOST_A, "Chachi", "a group about groups"));
assert.deepStrictEqual(
  [m.id, m.host, m.name, m.about, m.relayUrl, m.mine],
  ["chachi", HOST_A, "Chachi", "a group about groups", null, false],
  "a 39000 gives an id, a host KEY and no url at all",
);
assert.strictEqual(metaGroup({ kind: 39000, pubkey: HOST_A, tags: [] }), null, "no `d` is no group");
assert.strictEqual(metaGroup({ kind: 1, pubkey: HOST_A, tags: [["d", "x"]] }), null, "…and neither is another kind");

// ---- rank: the order, and the refusal to merge ----------------------------

// Nothing typed opens on the reader's own groups — the whole reason the
// picker exists is that nobody remembers an id to type.
let rows = rank("", { own, meta: [] });
assert.deepStrictEqual(rows.map((r) => r.id), ["chachi", "zaps"], "`group:` alone offers your groups");

// A name match reaches both halves; yours come first because you chose them.
const found = [metaGroup(meta("chat-abc", HOST_A, "Chachi Fans")), metaGroup(meta("zz", HOST_B, "Chachi Talk"))];
rows = rank("chachi", { own, meta: found });
assert.deepStrictEqual(rows.map((r) => r.id), ["chachi", "chat-abc", "zz"], "your group, then the relay's answers");
assert.deepStrictEqual(rows.map((r) => r.mine), [true, false, false], "and each says which it is");

// The relay's ORDER is kept. It ranked those two against the corpus and the
// reader's lens; re-sorting them on where a substring landed would throw a
// measurement away for a guess.
rows = rank("chachi", { own: [], meta: [...found].reverse() });
assert.deepStrictEqual(rows.map((r) => r.id), ["zz", "chat-abc"], "the relay's ranking survives");

// …and so does every ROW. This is the one that was wrong, and the fixtures
// above could not see it because "chachi" happens to be a literal substring of
// "Chachi Fans". The relay matches through a real index — `name` in the primary
// tier, `about` in the secondary, both reachable through the prefix/fuzzy `near`
// column — and re-testing its answers with `includes` here silently discarded
// every hit that index can make and a substring cannot, reporting them to the
// reader as "No group matches".
const byAbout = metaGroup(meta("g-about", HOST_A, "Nostr Talk", "all about bitcoin"));
assert.deepStrictEqual(
  rank("bitcoin", { own: [], meta: [byAbout] }).map((r) => r.id), ["g-about"],
  "a hit the relay made on `about` is not thrown away by a name/id substring test",
);
const byNear = metaGroup(meta("g-near", HOST_A, "Alice's Club", ""));
assert.deepStrictEqual(
  rank("alices", { own: [], meta: [byNear] }).map((r) => r.id), ["g-near"],
  "…nor is one the near tier matched but `includes` cannot",
);
assert.strictEqual(rank("zzzz", { own: [], meta: [byAbout, byNear] }).length, 2,
  "the relay decided what matches; this module's job is the ORDER between bands, not a second opinion");

// The reader's OWN list is the other way round, and has to be: it arrives whole
// and unsearched, so nothing has decided which entries were meant.
assert.strictEqual(rank("zaps", { own, meta: [] }).length, 1, "an own-list row is matched here, because nothing else did");
assert.strictEqual(rank("nothinglikethis", { own, meta: [] }).length, 0, "…and a non-match is dropped");

// An id typed in FULL is somebody naming one group: that row goes under Enter,
// ahead of every name match, so the picker gets out of the way.
rows = rank("zz", { own, meta: found });
assert.strictEqual(rows[0].id, "zz", "an exact id wins");

// Case-sensitively, because that is how the store matches an `h` tag and how
// the filter asks for one. `General` and `general` are two groups.
const cased = [metaGroup(meta("General", HOST_A, "General")), metaGroup(meta("general", HOST_B, "general"))];
rows = rank("general", { own: [], meta: cased });
assert.strictEqual(rows[0].id, "general", "the exact-id band compares bytes, not letters");

// THE ONE THAT MATTERS. Your list says `general` lives on relay A; the corpus
// holds a `general` signed by relay B's key. Those are two statements about
// possibly-different groups and nothing here can join them, so they stay two
// rows — and both are flagged, because picking either writes the same `#h`.
const clash = { kind: 10009, tags: [["group", "general", "wss://a.example", "General"]] };
rows = rank("general", { own: ownGroups(clash), meta: [metaGroup(meta("general", HOST_B, "General"))] });
assert.strictEqual(rows.length, 2, "one id on two hosts is two rows, never one merged row");
assert(rows.every((r) => r.ambiguous), "…and both say so");
assert.strictEqual(rows[0].relayUrl, "wss://a.example", "your row keeps YOUR url");
assert.strictEqual(rows[0].host, null, "…and does not acquire the other row's key");
assert.strictEqual(rows[1].host, HOST_B, "the corpus row keeps its key");
assert.strictEqual(rows[1].relayUrl, null, "…and never borrows a url it was not given");

// An unambiguous row says so too, since the flag is what the UI draws on.
rows = rank("zaps", { own, meta: [] });
assert.strictEqual(rows[0].ambiguous, false, "one row for an id is not ambiguous");

// The same group offered by both inputs is still deduped WITHIN a provenance:
// a list naming it twice, or a repeat between the exact band and the name
// band, must not draw two identical rows.
rows = rank("chachi", { own, meta: [] });
assert.strictEqual(rows.filter((r) => r.id === "chachi").length, 1, "the exact band does not double a row it also matches");

// ---- where: how sure the row is about its host ----------------------------

assert.deepStrictEqual(
  where({ relayUrl: "wss://relay.groups.nip29.com/", host: null }),
  { text: "relay.groups.nip29.com", exact: true },
  "a tagged url is the one exact answer, and draws bare",
);
assert.deepStrictEqual(
  where({ relayUrl: null, host: HOST_A }, "Groups Relay"),
  { text: "Groups Relay", exact: false },
  "a host's own kind 0 name is a claim it made about itself, not the same fact",
);
assert.deepStrictEqual(
  where({ relayUrl: null, host: HOST_A }),
  { text: `relay ${HOST_A.slice(0, 8)}…`, exact: false },
  "with no name at all the key is shown — never a guessed url",
);
assert.strictEqual(where({ relayUrl: null, host: null }).text, "unknown relay", "and nothing invents one");

assert.strictEqual(relayLabel("wss://x.example/"), "x.example", "the label drops the scheme and the trailing slash");
assert.strictEqual(relayLabel("wss://x.example/inbox"), "x.example/inbox", "…and keeps a real path");

// ---- WHICH CONNECTION the picker asks on -----------------------------------
//
// A source assertion, because the rule lives in app.js and nothing else can
// see it — the same shape cards.test.mjs uses to keep entity paths out of
// app.js. Both reads go down the AUTHENTICATED socket, and the one worth
// pinning is the reader's own list, because there is a plausible-looking
// change that breaks it: the store applies the observer as a FILTER, so a
// reader with no scores and no 10040 mirrored here reads back nothing at all —
// their own events included. Measured against a real Vespa, signed in on a
// store with no scores, `{kinds:[10009],authors:[me]}` returned 0, and
// returned 1 the moment a provider that reader trusts scored them.
//
// Moving this read to the anonymous connection makes it answer, which is
// exactly why it must not: the picker would become the one place on the page
// showing a reader content the relay has otherwise decided it cannot rank for
// them. No chain here, no personal groups — deliberately, with readiness.js
// explaining it rather than a special case in the search box.
const appSrc = readFileSync(new URL("../../web/src/main/resources/web/app.js", import.meta.url), "utf8");
const ownRead = /(\w+)\.req\(\{ kinds: \[10009\]/.exec(appSrc);
assert(ownRead, "app.js must read the reader's own kind 10009 somewhere");
assert.strictEqual(ownRead[1], "relay",
  "the reader's own group list is read on the AUTHENTICATED socket, behind the observer gate like " +
  "everything else served to them — an unmirrored reader is meant to see no personal groups");
assert(/relay\.req\(\{ kinds: \[39000\], search:/.test(appSrc),
  "…and so is the 39000 name search, because which groups to offer first IS a ranked question");

console.log("groups: your list and the corpus stay two answers, and one id on two relays stays two rows");
