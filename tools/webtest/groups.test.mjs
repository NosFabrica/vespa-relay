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

const { ownGroups, metaGroup, rank, where, relayLabel } =
  await import(new URL("../../relay/src/main/resources/web/shared/groups.js", import.meta.url));

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

console.log("groups: your list and the corpus stay two answers, and one id on two relays stays two rows");
