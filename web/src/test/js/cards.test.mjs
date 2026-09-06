import assert from 'assert';
import { readFileSync } from 'node:fs';
globalThis.location = { protocol: "http:", host: "localhost:7787" };
globalThis.window = { addEventListener: () => {} };

const { card, rowOf, popupRow, namedPubkeys } = await import(new URL("../../main/resources/web/cards.js", import.meta.url));
const { pubkeyParam, nip19Parse, npub, noteId, naddr, shortNpub } = await import(new URL("../../main/resources/web/shared/nip19.js", import.meta.url));
const { buildFilters } = await import(new URL("../../main/resources/web/shared/query.js", import.meta.url));
const { renderers, rows, safeUrl, PEOPLE_GRID, PEOPLE_GRID_KINDS } = await import(new URL("../../main/resources/web/cards/base.js", import.meta.url));
const { parsePatch } = await import(new URL("../../main/resources/web/cards/code.js", import.meta.url));
const { kindLabel, kindTone, KNOWN_KINDS } = await import(new URL("../../main/resources/web/shared/kinds.js", import.meta.url));
const { seedProfiles } = await import(new URL("../../main/resources/web/shared/profiles.js", import.meta.url));
const { seedGroupEvents } = await import(new URL("../../main/resources/web/shared/groupnames.js", import.meta.url));
const { REPLY_KINDS } = await import(new URL("../../main/resources/web/shared/parents.js", import.meta.url));

const pk = "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2";
const pk2 = "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d";
const eid = "0123456789abcdef".repeat(4);
const now = Math.floor(Date.now() / 1000);
const ev = (kind, tags = [], content = "") => ({ id: eid, pubkey: pk, kind, created_at: now - 3600, tags, content });

// One fixture per kind, each asserting the renderer's distinctive output.
// [kind, event, substring the full render must contain]
const FIXTURES = [
  [0,     ev(0, [], JSON.stringify({ name: "alice", about: "hi", picture: "https://x/p.png", nip05: "a@b.co", website: "https://a.co" })), "nip05"],
  [1,     ev(1, [], "hello world"), "hello world"],
  [11,    ev(11, [], "thread post"), "thread post"],
  [9802,  ev(9802, [["r", "https://src.example/page"]], "the quoted passage"), "quote"],
  [40,    ev(40, [], JSON.stringify({ name: "my channel", about: "chat", picture: "https://x/c.png" })), "my channel"],
  [41,    ev(41, [], JSON.stringify({ name: "renamed channel" })), "renamed channel"],
  [39000, ev(39000, [["d", "chachi"], ["name", "Chachi"], ["about", "a group about groups"], ["private"], ["closed"]]), "a group about groups"],
  [3,     ev(3, [["p", pk2], ["p", pk]]), "follows <b>2</b>"],
  [30000, ev(30000, [["d", "friends"], ["title", "Friends"], ["p", pk2]]), "Friends"],
  [10002, ev(10002, [["r", "wss://relay.a.com"], ["r", "wss://relay.b.com", "read"]]), "relay.a.com"],
  [30002, ev(30002, [["title", "My relays"], ["relay", "wss://relay.c.com"]]), "relay.c.com"],
  [10040, ev(10040, [["30382:rank", pk2, "wss://nip85.example.com"]]), "30382:rank"],
  [30382, ev(30382, [["d", pk2], ["rank", "87"], ["followers", "1234"]]), "rank-big"],
  [30023, ev(30023, [["title", "My Article"], ["summary", "sum"], ["image", "https://x/i.jpg"], ["published_at", String(now)]], "full body text"), "full body text"],
  [30024, ev(30024, [["title", "Draft"]], "draft body"), "Draft"],
  [30818, ev(30818, [["title", "Wiki Page"], ["d", "wiki-page"]], "wiki text"), "Wiki Page"],
  [30004, ev(30004, [["title", "Best of"], ["a", "30023:x:y"], ["e", eid]]), "2 items"],
  [20,    ev(20, [["imeta", "url https://x/photo.jpg", "m image/jpeg"], ["title", "a photo"]]), "https://x/photo.jpg"],
  [21,    ev(21, [["imeta", "url https://x/v.mp4", "image https://x/poster.jpg"], ["title", "a video"]]), "<video"],
  [22,    ev(22, [["imeta", "url https://x/s.mp4"], ["title", "short"]]), "<video"],
  [34235, ev(34235, [["url", "https://x/h.mp4"], ["title", "h video"]]), "<video"],
  [34236, ev(34236, [["url", "https://x/v2.mp4"], ["title", "v short"]]), "<video"],
  [1063,  ev(1063, [["url", "https://x/f.pdf"], ["m", "application/pdf"], ["size", "123456"]], "a file"), "120.6 KB"],
  [1986,  ev(1986, [["url", "https://x/a.mp3"]], "an audio thing"), "<audio"],
  [30005, ev(30005, [["title", "Video set"], ["a", "34235:x:y"]]), "1 video"],
  [30030, ev(30030, [["title", "Pack"], ["emoji", "wave", "https://x/wave.png"]]), "emoji-grid"],
  [1337,  ev(1337, [["l", "kotlin"], ["name", "Snippet.kt"]], "fun main() {}"), "fun main() {}"],
  [1617,  ev(1617, [["subject", "Fix the thing"]], "--- a/f\n+++ b/f"), "codeblock"],
  [1621,  ev(1621, [["subject", "It breaks"]], "steps to reproduce"), "It breaks"],
  [30617, ev(30617, [["name", "my-repo"], ["description", "a repo"], ["web", "https://x/repo"], ["clone", "https://x/repo.git"]]), "my-repo"],
  [30063, ev(30063, [["title", "v1.2.0"], ["url", "https://x/rel.tgz"]], "release notes"), "v1.2.0"],
  [30311, ev(30311, [["title", "Live show"], ["status", "live"], ["streaming", "https://x/hls.m3u8"], ["current_participants", "42"]]), "status-pill live"],
  [31922, ev(31922, [["title", "Conference"], ["start", "2026-09-01"], ["end", "2026-09-03"], ["location", "Lisbon"]]), "Lisbon"],
  [31923, ev(31923, [["title", "Meetup"], ["start", String(now + 86400)]]), "Meetup"],
  [31924, ev(31924, [["title", "My calendar"], ["a", "31923:x:y"]]), "1 event"],
  [30402, ev(30402, [["title", "Bike for sale"], ["price", "250", "USD"], ["location", "Berlin"], ["image", "https://x/bike.jpg"]], "good bike"), "250 USD"],
  [30018, ev(30018, [], JSON.stringify({ name: "Widget", description: "a widget", price: 10, currency: "EUR", images: ["https://x/w.jpg"] })), "10 EUR"],
  [30017, ev(30017, [], JSON.stringify({ name: "My stall", description: "shop", currency: "USD" })), "My stall"],
  [9041,  ev(9041, [["amount", "2100000000"]], "help the cause"), "2,100,000 sats"],
  [30009, ev(30009, [["name", "Helper"], ["description", "helps"], ["image", "https://x/badge.png"]]), "Helper"],
  [31990, ev(31990, [["web", "https://app.example"]], JSON.stringify({ name: "CoolApp", about: "does things", picture: "https://x/app.png" })), "CoolApp"],
  [32267, ev(32267, [["name", "NativeApp"], ["icon", "https://x/icon.png"], ["description", "native"]]), "NativeApp"],
  [31989, ev(31989, [["d", "30023"], ["a", "31990:x:y"], ["a", "31990:z:w"]]), "2 handlers for kind 30023"],
  [31890, ev(31890, [["title", "My feed"], ["description", "stuff"]]), "My feed"],

  // ---- the reactive kinds: what they point at is the card ------------------
  [5,     ev(5, [["e", eid], ["k", "1"]], "posted by mistake"), "asks to delete 1 event"],
  [6,     ev(6, [["e", eid]], JSON.stringify({ content: "the original note" })), "the original note"],
  [16,    ev(16, [["e", eid]], ""), "reposted"],
  [7,     ev(7, [["e", eid]], "+"), "liked"],
  [17,    ev(17, [["r", "https://site.example"]], "+"), "site.example"],
  [8,     ev(8, [["a", `30009:${pk}:helper`], ["p", pk2]]), "1 recipient"],
  [9,     ev(9, [], "a chat line"), "a chat line"],
  [42,    ev(42, [["e", eid]], "a channel line"), "a channel line"],
  [1311,  ev(1311, [["a", `30311:${pk}:show`]], "a live chat line"), "a live chat line"],
  [1111,  ev(1111, [["E", eid], ["e", eid]], "a comment"), "in reply to"],
  [1068,  ev(1068, [["option", "a", "Yes"], ["option", "b", "No"], ["endsAt", String(now + 3600)]], "Best colour?"), "Best colour?"],
  [1018,  ev(1018, [["e", eid], ["response", "a"]]), "voted on"],
  [1984,  ev(1984, [["p", pk2, "spam"]], "keeps posting the same link"), "<b>spam</b>"],
  [1985,  ev(1985, [["L", "nip68"], ["l", "photo", "nip68"], ["e", eid]]), "photo"],
  [4550,  ev(4550, [["a", `34550:${pk}:dev`], ["e", eid]], JSON.stringify({ content: "the approved post" })), "the approved post"],
  [9734,  ev(9734, [["p", pk2], ["amount", "21000"]], "nice one"), "21 sats"],
  [9735,  ev(9735, [["p", pk2], ["e", eid],
            ["description", JSON.stringify({ pubkey: pk, content: "thanks!", tags: [["amount", "1000000"]] })]]), "1,000 sats"],
  [30315, ev(30315, [["d", "music"], ["r", "https://track.example"]], "listening to something"), "listening to something"],
  [34550, ev(34550, [["d", "dev"], ["description", "a community"], ["p", pk2, "", "moderator"]]), "1 moderator"],

  // ---- NIP-51: every list and set --------------------------------------
  [10000, ev(10000, [["p", pk2], ["t", "spam"], ["word", "airdrop"], ["e", eid]]), "1 word"],
  [10001, ev(10001, [["e", eid]]), "1 note"],
  [10003, ev(10003, [["e", eid], ["a", `30023:${pk}:art`], ["t", "reading"], ["r", "https://x.example"]]), "1 article"],
  [10004, ev(10004, [["a", `34550:${pk}:dev`]]), "1 community"],
  [10005, ev(10005, [["e", eid]]), "1 channel"],
  [10006, ev(10006, [["relay", "wss://bad.example"]]), "bad.example"],
  [10007, ev(10007, [["relay", "wss://search.example"]]), "search.example"],
  [10008, ev(10008, [["a", `30009:${pk}:b`], ["e", eid]]), "1 badge"],
  // A `group` tag is `["group", <id>, <relay url>, <name?>]`: the one NIP-51 entry whose value
  // is not element 1.
  [10009, ev(10009, [["group", "abc123", "wss://groups.example", "My Group"]]), "groups.example"],
  [10011, ev(10011, [["a", `30000:${pk}:friends`]]), "1 follow set"],
  [10012, ev(10012, [["relay", "wss://feed.example"], ["a", `30002:${pk}:set`]]), "1 relay set"],
  [10013, ev(10013, [["relay", "wss://private.example"]]), "private.example"],
  [10015, ev(10015, [["t", "bitcoin"], ["a", `30015:${pk}:tech`]]), "1 interest set"],
  [10017, ev(10017, [["p", pk2]]), "1 person"],
  [10018, ev(10018, [["a", `30617:${pk}:repo`]]), "1 repository"],
  [10020, ev(10020, [["p", pk2]]), "1 person"],
  [10030, ev(10030, [["emoji", "wave", "https://x/w.png"], ["a", `30030:${pk}:pack`]]), "emoji-grid"],
  [10050, ev(10050, [["relay", "wss://dm.example"]]), "dm.example"],
  [10054, ev(10054, [["p", pk2], ["url", "https://pod.example/feed.xml"]]), "1 feed"],
  [10063, ev(10063, [["server", "https://blossom.example"]]), "1 server"],
  [10064, ev(10064, [["p", pk2]]), "1 person"],
  [10096, ev(10096, [["server", "https://files.example"]]), "files.example"],
  [10101, ev(10101, [["p", pk2]]), "1 person"],
  [10102, ev(10102, [["relay", "wss://wiki.example"]]), "wiki.example"],
  [30001, ev(30001, [["d", "legacy"], ["e", eid], ["a", `30023:${pk}:y`], ["p", pk2], ["t", "misc"]]), "1 event"],
  [30003, ev(30003, [["d", "reading"], ["title", "Reading list"], ["description", "things worth keeping"],
            ["e", eid], ["a", `30023:${pk}:art`], ["t", "books"]]), "Reading list"],
  [30006, ev(30006, [["d", "pics"], ["e", eid]]), "1 picture"],
  [30007, ev(30007, [["d", "1"], ["p", pk2]]), "kind 1"],
  [30008, ev(30008, [["d", "set"], ["a", `30009:${pk}:b`]]), "1 badge"],
  [30015, ev(30015, [["d", "tech"], ["title", "Tech"], ["t", "nostr"], ["t", "bitcoin"]]), "2 hashtags"],
  [30267, ev(30267, [["d", "apps"], ["a", `31990:${pk}:app`]]), "1 app"],
  [39089, ev(39089, [["d", "pack"], ["title", "Nostr starters"], ["p", pk2]]), "Nostr starters"],
  [39092, ev(39092, [["d", "pack"], ["title", "Photo starters"], ["p", pk2]]), "1 member"],
  [39701, ev(39701, [["d", "example.com/post"], ["title", "A page"], ["description", "about it"]]), "example.com/post"],

  // ---- the remaining families ----------------------------------------------
  [30383, ev(30383, [["d", eid], ["rank", "5"]]), "rank-big"],
  [30384, ev(30384, [["d", `30023:${pk}:art`], ["rank", "9"]]), "rank-big"],
  [1618,  ev(1618, [["subject", "Add the thing"]], "please merge"), "Add the thing"],
  [1619,  ev(1619, [["subject", "Add the thing, again"]], "rebased"), "rebased"],
  [1622,  ev(1622, [], "a git reply"), "a git reply"],
  [1630,  ev(1630, [["e", eid]], "reopening"), "status-pill lead open"],
  [1631,  ev(1631, [["e", eid]], "merged"), "status-pill lead merged"],
  [1632,  ev(1632, [["e", eid]], "wontfix"), "status-pill lead closed"],
  [1633,  ev(1633, [["e", eid]], "not ready"), "status-pill lead draft"],
  [30618, ev(30618, [["d", "repo"], ["refs/heads/master", "abc1234"], ["HEAD", "ref: refs/heads/master"]]), "1 branch"],
  [30312, ev(30312, [["title", "The room"], ["status", "open"]]), "The room"],
  [30313, ev(30313, [["title", "The conference"], ["start", String(now + 86400)]]), "The conference"],
  [31925, ev(31925, [["a", `31923:${pk}:meetup`], ["status", "accepted"]]), "status-pill accepted"],
  [30403, ev(30403, [["title", "Draft bike"], ["price", "100", "EUR"]]), "100 EUR"],
  [30020, ev(30020, [], JSON.stringify({ name: "Auction item", price: 5, currency: "USD" })), "Auction item"],
  [1222,  ev(1222, [["imeta", "url https://x/voice.mp3"]]), "<audio"],
  [1244,  ev(1244, [["imeta", "url https://x/reply.mp3"]]), "<audio"],
  [30040, ev(30040, [["title", "The Book"], ["author", "Alice"], ["a", `30041:${pk}:ch1`]]), "1 section"],
  [30041, ev(30041, [["title", "Chapter One"]], "the chapter text"), "the chapter text"],
  [30166, ev(30166, [["d", "wss://relay.example"], ["N", "50"], ["N", "65"], ["s", "strfry"]],
            JSON.stringify({ name: "Example Relay", description: "a relay" })), "NIP-50"],
  [10166, ev(10166, [["frequency", "3600"], ["c", "open"], ["k", "10002"]]), "every 1h"],
  // One Trusted List per member type, each asserting the noun its own kind counts in.
  [30392, ev(30392, [["d", "tl-verified"], ["title", "Verified Human"], ["metric", "influence"],
                     ["p", pk, "", "88"]]), "1 member"],
  [30393, ev(30393, [["d", "tl-notes"], ["title", "Worth Reading"], ["e", eid]]), "1 event"],
  [30394, ev(30394, [["d", "tl-articles"], ["title", "Long Reads"], ["a", `30023:${pk}:essay`]]), "1 article"],
  [30395, ev(30395, [["d", "tl-ids"], ["title", "Known Books"], ["i", "isbn:9780316769488"]]), "1 identifier"],
];

// Every registered kind has a fixture, and every fixture targets a registered kind.
const registered = new Set(renderers.keys());
const covered = new Set(FIXTURES.map(([k]) => k));
const missing = [...registered].filter((k) => !covered.has(k)).sort((a, b) => a - b);
const stale = [...covered].filter((k) => !registered.has(k)).sort((a, b) => a - b);
assert.deepStrictEqual(missing, [], `registered kinds without a fixture: ${missing}`);
assert.deepStrictEqual(stale, [], `fixtures for unregistered kinds: ${stale}`);

// A card and its type-ahead row are one kind's knowledge at two sizes: one key set.
assert.deepStrictEqual([...rows.keys()].sort((a, b) => a - b), [...registered].sort((a, b) => a - b),
  "every card must bring the popup row that goes with it, and no row may name a kind nothing renders");

// A kind good enough to render is good enough to name and tint.
const unnamed = [...registered].filter((k) => kindLabel(k) === `kind ${k}`).sort((a, b) => a - b);
const untinted = [...registered].filter((k) => !kindTone(k)).sort((a, b) => a - b);
assert.deepStrictEqual(unnamed, [], `registered kinds with no label: ${unnamed}`);
assert.deepStrictEqual(untinted, [], `registered kinds with no family tone: ${untinted}`);

// KNOWN_KINDS is an identity, not a subset: a label for a kind nothing renders promises a card.
assert.deepStrictEqual(KNOWN_KINDS, [...registered].sort((a, b) => a - b),
  "the kinds we name and the kinds we render must be the same set");

// The operator page names kinds from this registry; the list it counts comes from the store.
const statsPage = readFileSync(new URL("../../main/resources/stats.html", import.meta.url), "utf8");
assert(/import\s*\{[^}]*kindLabel[^}]*\}\s*from\s*"\.\/web\/shared\/kinds\.js"/.test(statsPage),
  "stats.html must take kind names from shared/kinds.js, not carry a second copy");

for (const [kind, fixture, expect] of FIXTURES) {
  for (const opts of [undefined, { full: true }]) {
    const html = card(fixture, opts);
    assert(html.includes("<article"), `kind ${kind}: no card frame (${opts ? "full" : "preview"})`);
    assert(!html.includes("undefined"), `kind ${kind}: leaked "undefined" (${opts ? "full" : "preview"})`);
  }
  assert(card(fixture, { full: true }).includes(expect), `kind ${kind}: full render missing "${expect}"`);
}

// The generic floor: unknown kind renders, labelled honestly.
const unknown = card(ev(12345, [["title", "Mystery"], ["summary", "odd"]], "???"), { full: true });
assert(unknown.includes("kind 12345") && unknown.includes("Mystery"), "generic floor");

// ---- the type-ahead row ----------------------------------------------------
//
// The same card in the two lines the search field's popup has.
const channel = FIXTURES.find(([k]) => k === 40)[1];
assert.deepStrictEqual(rowOf(channel), { name: "my channel", sub: "chat", pic: "https://x/c.png" },
  "a channel's row is its name, its description, and its own picture");
assert(!popupRow(channel, 0).includes("{&quot;"), "…and no part of that JSON reaches the page");

// What every row owes: something to say, and none of the four ways of saying nothing.
for (const [kind, fixture] of FIXTURES) {
  const { name, sub } = rowOf(fixture);
  const html = popupRow(fixture, 0);
  assert(name, `kind ${kind}: a type-ahead row with no name at all`);
  assert(!/^[{[]/.test(name) && !/^[{[]/.test(sub), `kind ${kind}: the row leads with the event's raw JSON`);
  assert(!/^[0-9a-f]{16,}$/.test(name), `kind ${kind}: the row's name is a hash, which places nothing`);
  assert(name !== sub, `kind ${kind}: the row says the same thing on both lines`);
  assert(!html.includes("undefined") && !html.includes("[object Object]"), `kind ${kind}: a value leaked into the row`);
  assert(html.includes(`class="popup-item"`) && html.includes("kind-badge"), `kind ${kind}: the row lost its frame`);
}

// What each row says where it is more than the card's title, written as
// `name · sub` because which line a fact lands on is the row's business.
const ROW_SAYS = [
  // A payload is a document, not a line of text.
  [40, "my channel · chat"], [30017, "My stall · shop"], [30018, "Widget · 10 EUR · a widget"],
  [31990, "CoolApp · does things"], [30166, "Example Relay · a relay"],
  [6, "the original note"], [4550, "approved a post · the approved post"],
  // A fragment is not one either: what these say is what they point at.
  [7, "liked a note"], [16, "reposted a note"], [5, "asks to delete 1 event"],
  [9735, "zapped 1,000 sats · thanks!"], [9734, "asks to zap 21 sats"], [1018, "voted on a poll"],
  [1984, "reports as spam"], [1985, "labels photo"], [8, "awards a badge to 1 recipient"],
  [31989, "recommends 2 handlers for kind 30023"], [10166, "monitors relays every 1h"],
  [1631, "applied or merged"], [31925, "rsvp: accepted"],
  // The countable kinds carry no prose, so the count is the row's first line too.
  [3, "follows 2 people"], [10002, "2 relays"], [30000, "Friends · 1 member"],
  [10040, "trusts 1 score dimension"],
  // A named set says what it holds and what it is for.
  [30003, "Reading list · 1 event · 1 article · 1 hashtag · things worth keeping"],
  [30618, "repo · 1 branch"], [30040, "The Book · 1 section"], [30030, "Pack · 1 emoji"],
  [10000, "1 person · 1 hashtag · 1 word · 1 event"],
  // The ones whose fact has to be read out of somewhere first.
  [30382, "rank 87"], [1063, "application/pdf · 120.6 KB"], [30311, "Live show · live"],
  [31922, "Conference · 2026-09-01 · Lisbon"], [30402, "Bike for sale · 250 USD"],
  [1617, "Fix the thing"], [39000, "Chachi · a group about groups"], [9041, "goal: 2,100,000 sats"],
];
for (const [kind, expect] of ROW_SAYS) {
  const fixture = FIXTURES.find(([k]) => k === kind)[1];
  const { name, sub } = rowOf(fixture);
  assert(`${name} · ${sub}`.includes(expect), `kind ${kind}: the row reads "${name} · ${sub}", not "${expect}"`);
}

// The name falls back to the author, never to the content; the sub then
// carries the author unless the name already is them.
const unnamedChannel = ev(40, [], JSON.stringify({ about: "a channel whose creator left the name off" }));
assert.strictEqual(rowOf(unnamedChannel).name, shortNpub(pk), "with nothing to name it, a row leads with who made it");
assert.strictEqual(rowOf(unnamedChannel).sub, "a channel whose creator left the name off", "…and still says what it knows");
assert.strictEqual(rowOf(ev(1, [], "")).sub, "", "a row that IS the author does not say them twice");
assert.strictEqual(rowOf(ev(1, [], "hello")).sub, shortNpub(pk), "…and one that is not, does");
// A profile is the one row whose subject is its own author.
assert.deepStrictEqual(rowOf(ev(0, [], JSON.stringify({ name: "carol" }))), { name: "carol", sub: "", pic: "" },
  "a profile with no bio says nothing under its name — least of all the name again, as an npub");
// `clip` counts characters, so a note opening with a paragraph break is folded first.
assert.strictEqual(rowOf(ev(1, [], "\n\n\n  the first line\n\nand the next")).name, "the first line and the next",
  "a row is ONE line, whatever the text did with its newlines");
// A relation is to what the event points at, and an `a` tag names an article
// as readily as a note; with no target the noun goes.
assert.strictEqual(rowOf(ev(7, [["a", `30023:${pk}:art`]], "+")).name, "liked an entry");
assert.strictEqual(rowOf(ev(7, [["e", eid]], "+")).name, "liked a note");
assert.strictEqual(rowOf(ev(7, [], "+")).name, "liked", "with nothing to point at, the row says only what it knows");
assert.strictEqual(rowOf(ev(6, [["a", `30023:${pk}:art`]])).name, "reposted an entry");

// Whatever a row says, the card it opens must say too.
for (const [kind, tags] of [
  [30000, [["p", pk2]]], [30002, [["relay", "wss://r.example"]]],
  [30005, [["a", "34235:x:y"]]], [30030, [["emoji", "wave", "https://x/w.png"]]],
]) {
  const set = ev(kind, [["d", "s"], ["title", "T"], ["description", "what the set is for"], ...tags]);
  assert(rowOf(set).sub.includes("what the set is for"), `kind ${kind}: its row drops the set's description`);
  assert(card(set, { full: true }).includes("what the set is for"), `kind ${kind}: its card drops the description its row shows`);
}

// A second line that repeats the first is not a second line.
const echoed = ev(40, [], JSON.stringify({ name: "Bitcoin India", about: "Bitcoin India" }));
assert.strictEqual(rowOf(echoed).name, "Bitcoin India");
assert.strictEqual(rowOf(echoed).sub, shortNpub(pk), "a description that is the name gives way to who posted it");
// The generic floor keeps the content on both lines: for a kind nothing renders it is all anybody knows.
assert.deepStrictEqual(rowOf(ev(12345, [["title", "Mystery"]], "the only thing known about it")),
  { name: "Mystery", sub: "the only thing known about it", pic: "" },
  "an unregistered kind still says what it carries");
// A JSON field can be any type, and `${}` on an object is "[object Object]".
assert.strictEqual(rowOf(ev(40, [], JSON.stringify({ name: { evil: 1 }, about: "x" }))).name, shortNpub(pk),
  "a name that is not text is not a name");

// Permalink depth is real: long content clamps in preview, not in full.
const long = "x".repeat(2000);
const preview = card(ev(1, [], long));
const full = card(ev(1, [], long), { full: true });
assert(preview.includes("clamp") && preview.includes("…"), "preview clips and clamps");
assert(!full.includes("clamp") && full.includes(long), "full mode renders every character");

// A full-depth video with no url renders its text once, and a preview picture
// with no url still shows its text.
const noUrlVideo = card(ev(21, [["title", "the only body line"]]), { full: true });
assert.strictEqual((noUrlVideo.match(/the only body line/g) || []).length, 1, "video full/no-url: body once");
assert(card(ev(20, [], "picture text, no url")).includes("picture text, no url"), "picture preview keeps text without a url");

// ---- the git family -------------------------------------------------------
//
// NIP-34 puts `git format-patch` output in `content`, so the parser is asserted directly, then the card.
const FORMAT_PATCH = `From 4f4d5c1a9e8b7f6d5c4b3a2918273645ffee0011 Mon Sep 17 00:00:00 2001
From: Alice <alice@example.com>
Date: Tue, 4 Aug 2026 11:02:31 +0200
Subject: [PATCH v2 2/3] router: yield ingest while the relay
 is under read pressure

The poller did nothing with the mean it got back.

---
 router/PressurePoller.kt | 24 ++++++---
 1 file changed, 3 insertions(+), 1 deletion(-)

diff --git a/router/PressurePoller.kt b/router/PressurePoller.kt
index 3a4f2b1..9c8e7d0 100644
--- a/router/PressurePoller.kt
+++ b/router/PressurePoller.kt
@@ -41,9 +41,15 @@ class PressurePoller(
-    private fun sleepFor(): Duration = base
+    private fun sleepFor(): Duration {
+        val mean = pressure.mean()
+    }
`;
const parsed = parsePatch(FORMAT_PATCH);
// The fold is inside a sentence: joined wrong it reads "the relayis under read pressure".
assert.strictEqual(parsed.subject, "router: yield ingest while the relay is under read pressure",
  "a folded subject is one line again, with the space the fold stood for");
assert.deepStrictEqual(parsed.markers, ["PATCH v2 2/3"], "the bracket is metadata, not words in the title");
assert.strictEqual(parsed.message, "The poller did nothing with the mean it got back.",
  "the commit message is prose, and stops at git's `---`");
assert(parsed.diffLines[0].startsWith("diff --git "), "the diff starts at the diff");
assert(!parsed.diffLines.some((l) => l.includes("1 file changed")), "git's own stat block is not part of it");
// Lines, not a string: a megabyte patch is split once and sliced.
assert(Array.isArray(parsed.diffLines), "the diff stays a view of the lines it was split into");
// A bare diff, with no mail at all, keeps every line.
const bare = parsePatch("--- a/f\n+++ b/f\n@@ -1 +1 @@\n-a\n+b\n");
assert.strictEqual(bare.subject, "", "no mail, no subject to take from it");
assert(bare.diffLines[0] === "--- a/f" && bare.diffLines.includes("+b"), "and the whole diff survives");
// A `---` inside the message is prose; the separator is the last one before the patch.
const ruled = parsePatch("From abc1234 Mon Sep 17 00:00:00 2001\nSubject: t\n\nabove\n---\nbelow\n\n---\ndiff --git a/f b/f\n+x\n");
assert(ruled.message.includes("above") && ruled.message.includes("below"), "a rule inside a commit message is message");
// A mail with no blank line before its diff: the diff ends the headers too.
const unterminated = parsePatch("From abc1234 Mon Sep 17 00:00:00 2001\nSubject: t\ndiff --git a/f b/f\n@@ -1 +1 @@\n+x\n");
assert(unterminated.subject === "t" && unterminated.diffLines.length > 1,
  "the diff ends the headers too, for a mail that never wrote a blank line");
// A repeated header keeps the first value and takes no continuation from the second.
const twiceHeaded = parsePatch("From abc1234 Mon Sep 17 00:00:00 2001\nSubject: first\nSubject: second\n and its fold\n\nbody\n");
assert.strictEqual(twiceHeaded.subject, "first", "the first header wins, whole");
// Nothing here throws on a stranger's string.
for (const junk of ["", null, undefined, "From\n\n\n", "Subject: no from line", "\n\n\n"]) {
  assert.doesNotThrow(() => parsePatch(junk), `parsePatch(${JSON.stringify(junk)}) must not throw`);
}

const patchCard = card(ev(1617, [["a", `30617:${pk2}:vespa-relay`], ["t", "root"]], FORMAT_PATCH), { full: true });
// Git's own first line is the same 40 hex and the same fake date on every patch.
assert(!patchCard.includes("Mon Sep 17 00:00:00 2001"), "git's From line is not a title");
assert(patchCard.includes("router: yield ingest while the relay is under read pressure"), "the subject is");
assert(patchCard.includes(">PATCH v2 2/3<"), "the series rides as a pill");
assert(!patchCard.includes(">root<"), "…and the `t` tag saying the same thing does not double it");
assert(card(ev(1617, [["t", "root"]], "--- a/f\n+++ b/f\n@@ -1 +1 @@\n+x\n")).includes(">root<"),
  "…but with no series in the mail, the tag is what says so");
// Counted from the diff itself, so the numbers describe the lines on the card.
assert(patchCard.includes("+3") && patchCard.includes("−1") && patchCard.includes("1 file"),
  "the change is measured, in the line a reviewer reads first");
assert(/class="[^"]*d-add[^"]*"/.test(patchCard) && /class="[^"]*d-hunk/.test(patchCard),
  "and the diff is tinted rather than being one grey wall");
// A row is one line, and not git's From line.
const patchRow = rowOf(ev(1617, [["a", `30617:${pk2}:vespa-relay`]], FORMAT_PATCH));
assert(!patchRow.name.includes("Mon Sep 17 00:00:00 2001"), "git's From line is not a row either");
assert.strictEqual(patchRow.name, "router: yield ingest while the relay is under read pressure", "the subject is");
assert.strictEqual(patchRow.sub, "vespa-relay", "…over the repository it is a patch for");

// `+++`/`---` are file headers outside a hunk and ordinary lines inside one;
// a markdown rule deleted from a file is red.
const rules = card(ev(1617, [], "diff --git a/R.md b/R.md\n--- a/R.md\n+++ b/R.md\n@@ -1,2 +1,2 @@\n---- old rule\n+++++ new rule\n"), { full: true });
assert(/d-del[^>]*>---- old rule/.test(rules), "a deleted line is a deletion, whatever its text starts with");
assert(/d-add[^>]*>\+\+\+\+\+ new rule/.test(rules), "…and so is an added one");
assert(rules.includes("+1") && rules.includes("−1") && rules.includes("1 file"),
  "and both are counted once, under one file");

// A run of whitespace inside a line must not cost quadratic time to trim.
const spaces = "a" + " ".repeat(80000) + "b\nreal line\n\n\n";
const started = Date.now();
const wide = card(ev(1337, [], spaces));
const took = Date.now() - started;
assert(took < 250, `a line of 80k spaces must not cost ${took}ms to render`);
assert(wide.includes("real line") && !/\n\s*\n\s*<\/pre>/.test(wide), "…and the trailing blank lines still go");

// Code is clipped by lines: a diff cut mid-line is not a diff, and `clip()` would trim the indentation.
const longCode = Array.from({ length: 40 }, (_, i) => `    line ${i}`).join("\n");
const clipped = card(ev(1337, [["name", "F.kt"], ["l", "kotlin"]], longCode));
assert(clipped.includes("    line 0"), "indentation is not whitespace to be trimmed, it is the code");
assert(clipped.includes("26 more lines") && !clipped.includes("line 39"), "and the preview says what it left");
assert(card(ev(1337, [], longCode), { full: true }).includes("line 39"), "the permalink keeps every line");
// The filename is on the file, not a row in a table under it.
assert(clipped.includes('class="code-head"') && clipped.includes(">F.kt<"), "a file is named on its header bar");
assert(!/<dt>name<\/dt>/.test(clipped), "…and not in the props table it used to sit in");

// Which repository, on every kind that names one.
for (const [kind, tags] of [
  [1617, [["a", `30617:${pk2}:my-repo`]]],
  [1621, [["a", `30617:${pk2}:my-repo`]]],
  [1631, [["a", `30617:${pk2}:my-repo`], ["e", eid]]],
  [30618, [["d", "my-repo"]]],                       // no `a` at all: derived from `d`
  [30063, [["d", "my-repo@v1"], ["title", "v1"]]],   // and from `<repo>@<version>`
]) {
  const html = card(ev(kind, tags), { full: true });
  assert(html.includes("repo-line") && html.includes(">my-repo<"), `kind ${kind} must say which repository it is in`);
}
// An `a` tag that is not a repository is not a repository line: a NIP-22
// comment on a patch carries the patch's address in the same slot.
assert(!card(ev(1621, [["a", `30023:${pk2}:an-article`]])).includes("repo-line"),
  "only a 30617 address is the repository");
// A card drawn under that repository's own page does not repeat it.
assert(!card(ev(1621, [["a", `30617:${pk2}:my-repo`]]), { within: `30617:${pk2}:my-repo` }).includes("repo-line"),
  "the page already said which repository this is");

// The root `e` is the target: NIP-34 lets a status also name the revisions
// and statuses it supersedes.
const gitRootId = "a".repeat(64);
const superseding = card(ev(1632, [["e", eid, "", "mention"], ["e", gitRootId, "", "root"]], "wontfix"), { full: true });
assert(superseding.includes(noteId(gitRootId)), "the root `e` is what the verdict is about");
assert(superseding.includes("supersedes 1 earlier event"), "and the others are said to be what they are");

// Maintainers come from the values of one tag, the slot no scan of `p` tags reaches.
const repo = ev(30617, [["d", "r"], ["name", "r"], ["maintainers", pk2, "not-a-pubkey", pk]]);
assert(card(repo, { full: true }).includes(npub(pk2)), "a maintainer is named");
assert(namedPubkeys(repo, { full: true }).includes(pk2), "…and declared, or the name renders as an npub");
assert(!namedPubkeys(repo, { full: true }).includes(pk), "the repo's own author is already its byline");

// Branches and tags are two groups, and HEAD names the default branch rather
// than printing `ref: refs/heads/main`.
const state = card(ev(30618, [["d", "r"], ["HEAD", "ref: refs/heads/main"],
  ["refs/heads/main", "4f4d5c1aaaa"], ["refs/tags/v1", "bb22cc33ddd"]]), { full: true });
assert(state.includes("1 branch") && state.includes("1 tag"), "each group counts itself");
assert(state.includes("ref-chip head") && state.includes(">4f4d5c1<"), "the default branch is marked, and a ref carries its commit");
assert(!state.includes("ref: refs/heads/main"), "HEAD is resolved, not printed");

// A release's artifacts read as file names; the url stays in the href.
const release = card(ev(30063, [["d", "r@v1"], ["url", "https://x.example/downloads/relay-1.0.jar"]]), { full: true });
assert(release.includes(">relay-1.0.jar<"), "an artifact is named by its file");
assert(release.includes('href="https://x.example/downloads/relay-1.0.jar"'), "and the url is where a url belongs");
assert(card(ev(30063, [["d", "vespa-relay@v0.9.3"]]), { full: true }).includes("v0.9.3"),
  "with no `title`, the version in `d` is the title");

// ---- a video card in the feed --------------------------------------------
//
// A kind 22 whose `d` is a client-generated UUID, whose caption is in `content`, and which names no poster.
const short22 = ev(22, [
  ["d", "f56d739a-09c9-4f0b-ba82-f8c21e1a6b8e"],
  ["alt", "Vertical Video"],
  ["imeta", "url https://video.example/x.mp4", "m video/mp4", "dim 1088x1920", "duration 42"],
  ["t", "isleofskye"],
], "Vlogging directly from the phone");
const shortPreview = card(short22);
assert(!shortPreview.includes("f56d739a"), "a generated `d` is an identifier, not a caption");
assert(shortPreview.includes("Vlogging directly from the phone"), "the caption in `content` reaches the card");
assert(shortPreview.includes("<video") && shortPreview.includes("https://video.example/x.mp4"),
  "the video plays in the results list, not only on its permalink");
// The url rides in `data-src`; app.js promotes it when the card nears the viewport.
assert(/data-src="https:\/\/video\.example/.test(shortPreview) && !/<video[^>]* src=/.test(shortPreview),
  "the video url is deferred, not fetched by every card in the list");
assert(shortPreview.includes("#t=0.1"), "with no poster, ask for the first frame rather than a black box");
assert(shortPreview.includes("aspect-ratio: 1088 / 1920"), "`dim` reserves the frame before the video loads");
assert(shortPreview.includes(">0:42<"), "duration reads as a clock, not as a count of seconds");
assert(shortPreview.includes("#isleofskye"), "topic tags ride along as chips");
// Repeated and pre-hashed `t` tags are one chip, not three that look alike.
const dupTags = card(ev(22, [["imeta", "url https://x/v.mp4"],
  ["t", "Scotland"], ["t", "scotland"], ["t", "#scotland"]], "hi"));
assert.strictEqual((dupTags.match(/tag-chip/g) || []).length, 1, "one topic, one chip");
assert(dupTags.includes(">#Scotland<"), "and it keeps the spelling the author wrote first");

// With no content, the media's own description carries the card; the NIP-31
// `alt` is for clients that cannot render kind 22 at all.
const alted = card(ev(22, [["alt", "Vertical Video"], ["imeta", "url https://x/v.mp4", "alt A puffin, up close"]]));
assert(alted.includes("A puffin, up close") && !alted.includes("Vertical Video"), "the media's description beats the NIP-31 fallback");

// A poster is a picture we already have: no second request for a frame.
const posterVideo = card(ev(21, [["imeta", "url https://x/v.mp4", "image https://x/p.jpg"], ["title", "a video"]]));
assert(posterVideo.includes('poster="https://x/p.jpg"') && posterVideo.includes('preload="none"'), "poster instead of a metadata fetch");
assert(!posterVideo.includes("#t=0.1"), "no first-frame seek when the event named a poster");

// The title has its own line, so it must not also be the body.
const titled = card(ev(21, [["imeta", "url https://x/v.mp4"], ["title", "One Title"]], "One Title"), { full: true });
assert.strictEqual((titled.match(/One Title/g) || []).length, 1, "title once, never as its own caption");

// `dim` is the one event-supplied value that reaches a style attribute.
const hostileDim = card(ev(22, [["imeta", "url https://x/v.mp4", "dim 1x1;background:url(javascript:alert(1))"]]));
assert(!hostileDim.includes("aspect-ratio") && !hostileDim.includes("javascript:"), "a `dim` that is not WxH styles nothing");

// The `d` fallback still does the work it was there for: a community's name.
assert(card(ev(34550, [["d", "nostr-devs"]])).includes("nostr-devs"), "a readable `d` is still a title");

// ---- a hashtag chip is a search --------------------------------------------
//
// The chip's href is fed back to the tokenizer this app runs its REQ from.
const chipHref = (html) => (/<a class="tag-chip" href="([^"]*)"/.exec(html) || [])[1] || null;
const tagged = card(ev(22, [["imeta", "url https://x/v.mp4"], ["t", "isleofskye"]], "a video"));
const href = chipHref(tagged);
assert(href, "a topic tag renders as a link, not as inert text");
const q = new URLSearchParams(href.slice(href.indexOf("?"))).get("q");
assert.strictEqual(q, "#isleofskye", "the chip asks for the topic as the field would have written it");
// The case variants are the query builder's business; the chip's is only that the topic is in there.
assert(buildFilters(q, { kinds: null, limit: 10 })[0]["#t"].includes("isleofskye"),
  "and that query builds the REQ for that topic");
// NIP-51 interests carry the same hashtags in bare form and link the same way.
assert.strictEqual(chipHref(card(ev(30015, [["d", "mine"], ["t", "isleofskye"]]))), href,
  "one hashtag, one link, wherever the card family puts it");
// A mute word is not a hashtag: it stays inert on purpose.
assert(!chipHref(card(ev(10000, [["word", "spoilers"]]))), "a mute word is not a search to run");

// ---- a group is a search too, and its host is not its id --------------------
//
// The hashtag rule, applied to NIP-29.
const groupList = card(ev(10009, [["group", "abc123", "wss://groups.example/", "My Group"]]), { full: true });
const groupHrefOf = (html) => (/<a href="(\/\?q=group[^"]*)"/.exec(html) || [])[1] || null;
let gh = groupHrefOf(groupList);
assert(gh, "a group on a list is a link, not inert text");
const gq = new URLSearchParams(gh.slice(gh.indexOf("?"))).get("q");
assert.strictEqual(gq, "group:abc123", "it asks for the id, which is what an `h` tag carries");
assert.deepStrictEqual(buildFilters(gq, { kinds: null, limit: 10 })[0]["#h"], ["abc123"],
  "and that query builds the REQ for that group");

// Both halves are asserted: showing the url is half a fix if the id stopped being reachable.
assert(groupList.includes("groups.example"), "the host relay is shown");
assert(groupList.includes("My Group"), "…and so is the name the list cached");
assert(!/relay-list[^]*?>abc123</.test(groupList), "the group id is never drawn as a relay url");

// The host is optional: an entry with an id and no relay url is still one they saved.
const hostless = card(ev(10009, [["group", "orphan-id", "", "Orphan"]]), { full: true });
assert(hostless.includes("Orphan"), "a group tag with no host relay is still drawn");
assert(!/nothing public here/i.test(hostless), "…so the card never reports an empty list it does not have");
assert.strictEqual(new URLSearchParams(groupHrefOf(hostless).slice(1)).get("q"), "group:orphan-id",
  "…and it still links to the search for its id");

// A group called "General" tells the reader nothing about which general it is.
const groupRec = card(ev(39000, [["d", "chachi"], ["name", "Chachi"], ["private"]]), { full: true });
assert.strictEqual(new URLSearchParams(groupHrefOf(groupRec).slice(1)).get("q"), "group:chachi",
  "the record's title is the search for its own posts");
assert(groupRec.includes("chachi") && groupRec.includes("group id"), "the id is on the card, not only in its json");
assert(groupRec.includes("members only"), "a bare `private` tag is a flag, and its PRESENCE is the value");

// ---- a chat line says which chat -------------------------------------------
//
// A line of chat is a fragment of a conversation, so the `h` tag draws as a
// pill beside the badge, linking into the same `group:` search.
const chatIn = (tags, opts) => card(ev(9, tags, "a chat line"), opts);
const pillOf = (html) => (/<a class="group-pill" href="([^"]+)" title="([^"]*)">([^<]*)<\/a>/.exec(html) || null);

let pill = pillOf(chatIn([["h", "unknown-room"]], { full: true }));
assert(pill, "a chat line names the room it was said in");
assert.strictEqual(new URLSearchParams(pill[1].slice(pill[1].indexOf("?"))).get("q"), "group:unknown-room",
  "and the room is a search for everything posted in it");
assert.strictEqual(pill[3], "unknown-room",
  "with nothing on the page able to name it, the ID stands — which is still more than the card said before");
assert(pillOf(chatIn([["h", "unknown-room"]])), "…at both depths: a result row needs the context more than a permalink does");
assert(!pillOf(card(ev(1, [], "a plain note"), { full: true })), "an event with no `h` names no room");
assert(!pillOf(card(ev(9, [], "a chat line with no room"), { full: true })),
  "…and neither does a chat line that carries none");

// The pill sits to the left of the badge: what this event is, and where it was said, found together.
const bylinePill = chatIn([["h", "unknown-room"]], { full: true });
assert(bylinePill.indexOf('class="group-pill"') < bylinePill.indexOf('class="kind-badge"'),
  "the room comes before the badge that says it is a chat");

// Both halves come from one cache, so a card and the search box can never
// disagree about a group's name.
seedGroupEvents([{ id: eid, kind: 39000, pubkey: pk2, created_at: now, content: "",
  tags: [["d", "nos"], ["name", "nos engineers"]] }]);
pill = pillOf(chatIn([["h", "nos"]], { full: true }));
assert.strictEqual(pill[3], "nos engineers", "a group the page has met draws its name");
assert(pill[2].includes("nos"), "…and the hover still carries the id, which is what the filter actually asks for");

// Two relays signing one id under different names reach this card as the bare id: an `h` names
// no host, so a name would claim one room where the search returns several.
seedGroupEvents([
  { id: eid, kind: 39000, pubkey: pk, created_at: now, content: "", tags: [["d", "general"], ["name", "General"]] },
  { id: eid, kind: 39000, pubkey: pk2, created_at: now, content: "", tags: [["d", "general"], ["name", "Generalists"]] },
]);
assert.strictEqual(pillOf(chatIn([["h", "general"]], { full: true }))[3], "general",
  "an id its hosts disagree about keeps the id, on the card exactly as in the search box");

// An id the search language cannot carry loses its link, not its label.
for (const bad of ["my group", "hello.", "x?"]) {
  const html = chatIn([["h", bad]], { full: true });
  assert(!pillOf(html), `\`${bad}\` must not be drawn as a link to another group`);
  const span = /<span class="group-pill" title="([^"]*)">([^<]*)<\/span>/.exec(html);
  assert(span, `\`${bad}\` still names the room it was posted to`);
  assert.strictEqual(span[2], bad, "…as its own id, unlinked");
}
// The same guard at the two older call sites that mint the same href.
const badRec = card(ev(39000, [["d", "my group"], ["name", "Mine"]]), { full: true });
assert(!/href="\/\?q=group/.test(badRec), "a 39000 whose `d` cannot be tokenized links nowhere");
assert(badRec.includes("Mine"), "…and still draws the group");
const badList = card(ev(10009, [["group", "my group", "wss://r.example/", "Listed"]]), { full: true });
assert(!/href="\/\?q=group/.test(badList), "the same for a `group` tag on a list");
assert(badList.includes("Listed"), "…which also keeps its name");

// An `h` tag on a fixture that has none is a tag the poison loop below never gets.
const hostileRoom = card(ev(9, [["h", `"><b BAD>`]], "x"), { full: true });
assert(!hostileRoom.includes("<b BAD>"), "a group id reached the card as MARKUP");
assert(!/href="[^"]*<b/.test(hostileRoom), "…and its href is a query string, not a document");

// ---- a picture post is an album --------------------------------------------
//
// NIP-68 gives a picture post one imeta per picture, and every one is drawn.
const album = ev(20, [
  ["title", "Five days on Skye"],
  ["imeta", "url https://x/1.jpg", "dim 1600x1200", "alt the Cuillin ridge"],
  ["imeta", "url https://x/2.jpg"], ["imeta", "url https://x/3.jpg"],
  ["imeta", "url https://x/4.jpg"], ["imeta", "url https://x/5.jpg"],
], "the whole trip");
const albumPreview = card(album), albumFull = card(album, { full: true });
assert.strictEqual((albumPreview.match(/<img/g) || []).length, 4, "the list shows four of five");
assert(albumPreview.includes("…and 1 more"), "and says so rather than silently dropping one");
assert.strictEqual((albumFull.match(/<img/g) || []).length, 5, "the permalink shows every picture");
assert(albumPreview.includes("media-grid"), "several pictures are a grid, not a stack");

// A lone picture gets the frame a video gets, shaped by its own `dim`.
const onePic = card(ev(20, [["imeta", "url https://x/1.jpg", "dim 1600x1200", "alt the Cuillin ridge"]], "one photo"));
assert(onePic.includes("media-frame sized") && onePic.includes("aspect-ratio: 1600 / 1200"), "one picture, one shaped frame");
assert(onePic.includes('alt="the Cuillin ridge"'), "the imeta description IS the alt text");
assert(!onePic.includes("media-grid"), "one picture is not a grid");

// Escaping holds in every renderer that touches content.
const hostile = card(ev(1, [], `<img src=x onerror=alert(1)>`), { full: true });
assert(!hostile.includes("<img src=x"), "content is escaped");

// Cards link internally, not to njump.
const note = card(ev(1, [], "hi"));
assert(note.includes('href="/note1') && note.includes('href="/npub1'), "note links internal");
assert(!note.includes("njump.me"), "search cards no longer link out");

// ---- a Trusted List's members are people, deduped and hex-only ------------
//
// Lists repeat entries, and a value that is not a key must never reach npub(). `peopleOf` is
// what gridPeople declares to the profile loader, so faces drawn and profiles fetched are one set.
{
  const lister = "b".repeat(64);
  const dup = ev(30392, [["d", "x"], ["title", "VH"], ["p", pk], ["p", pk], ["p", "not-a-key"]]);
  const html = card({ ...dup, pubkey: lister }, { full: true });
  assert(/>1 member</.test(html), `a repeated member is counted once: ${/result-body">([^<]*)</.exec(html)[1]}`);
  // person-cell, not av-wrap: the byline draws a face of its own.
  assert.strictEqual((html.match(/person-cell/g) || []).length, 1, "and drawn once");
  assert.deepStrictEqual(namedPubkeys({ ...dup, pubkey: lister }, { full: true }), [pk],
    "the profiles the page fetches are exactly the faces it draws");
}

// ---- the provenance row ---------------------------------------------------
//
// No standing label. Every card gets the row from one seam (shell), so it is asserted through the renderer.
{
  const { seedProvenance } = await import(new URL("../../main/resources/web/provenance.js", import.meta.url));
  const lister = "b".repeat(64), bot = "d".repeat(64);
  // The reader delegated `lister` for every declaration kind; labels take no delegation.
  const trusted = new Map([30382, 30383, 30384, 30385, 30392, 30393, 30394, 30395].map((k) => [k, new Set([lister])]));
  const target = { id: eid, pubkey: pk, kind: 0, created_at: now, tags: [], content: "{}" };
  const page = [
    target,
    { id: "2".repeat(64), pubkey: lister, kind: 30392, created_at: now, tags: [["d", "x"], ["title", "Verified Human"], ["p", pk]], content: "" },
    { id: "3".repeat(64), pubkey: lister, kind: 30392, created_at: now, tags: [["d", "y"], ["title", "Verified Human"], ["p", pk]], content: "" },
    { id: "4".repeat(64), pubkey: bot, kind: 1985, created_at: now, tags: [["L", "ugc"], ["l", "zapped", "ugc"], ["p", pk]], content: "" },
  ];
  seedProvenance(page, trusted);
  const html = card(target);
  assert(html.includes('class="prov pills"'), "a spliced card draws its provenance row");
  assert(!/prov-why|>why</.test(html), "the row carries no standing label — the pills are the row");

  // The three destinations, spelled by base.js and nowhere else.
  assert(html.includes(`href="/${naddr(`30392:${lister}:x`)}"`),
    "a list pill opens the list's own page — the same address its card opens");
  assert(html.includes(`href="/?q=zapped"`), "a label pill runs a search for itself");

  // Two tones, and the count that makes the duplicate honest.
  assert(html.includes('class="prov-pill vouched"'), "a delegated source takes the yours tone");
  assert(html.includes('class="prov-pill open"'), "an ungated label never takes it");
  assert(/Verified Human <span class="n">2<\/span>/.test(html),
    "two lists with one title are one pill and a count, not two identical chips");

  // One delegated publisher on this page, so a face on the gated pill would be the same face every time.
  const gated = /<a class="prov-pill vouched"[^>]*>(.*?)<\/a>/.exec(html)[1];
  const open = /<a class="prov-pill open"[^>]*>(.*?)<\/a>/.exec(html)[1];
  assert(!gated.includes("av-wrap"), "one publisher: the tone already says it is yours");
  assert(open.includes("av-wrap"), "nothing gated a label, so who said it is the whole question");

  // Every renderer, not just the ones through shell(): a profile is what a Trusted List splices.
  for (const [kind, fixture] of FIXTURES) {
    // A NIP-32 label may name an event, a pubkey and an address at once.
    seedProvenance([
      fixture,
      { id: "2".repeat(64), pubkey: bot, kind: 1985, created_at: now,
        tags: [["L", "ugc"], ["l", "spliced", "ugc"], ["e", fixture.id], ["p", fixture.pubkey],
               ["a", `${fixture.kind}:${fixture.pubkey}:${(fixture.tags.find((t) => t[0] === "d") || [])[1] || ""}`]],
        content: "" },
    ], trusted);
    assert(card(fixture).includes('class="prov pills"'),
      `kind ${kind}: a spliced card draws no provenance row — a hand-rolled frame that forgot it`);
  }

  // The popup is drawn over the results list, so app.js's hydrate must not touch the shared
  // provenance map (`row: "keep"`); that is only safe while this holds.
  seedProvenance(page, trusted);
  assert(!popupRow(target, 0).includes("prov"),
    "a type-ahead row draws no provenance — if it ever should, app.js's `keep` mode is wrong");

  // A card nothing points at draws no row at all: its presence is the signal.
  seedProvenance(page, trusted);
  assert(!card({ id: "9".repeat(64), pubkey: pk, kind: 1, created_at: now, tags: [], content: "hi" }).includes("prov pills"),
    "an ordinary hit gets no row");
  seedProvenance([], trusted);
  assert(!card(target).includes("prov pills"), "and the row clears with the page");
}

// ---- every card is a link to its own page --------------------------------
//
// `data-href` is what app.js navigates on, and the same destination is a real anchor inside the
// card so middle-click and Tab work. Which anchor differs by family, so the assertion is that the
// two agree. Neither appears at permalink depth.
const hrefAttr = (html) => (/data-href="([^"]*)"/.exec(html) || [])[1] || null;
for (const [kind, fixture] of FIXTURES) {
  const preview = card(fixture);
  const href = hrefAttr(preview);
  assert(href, `kind ${kind}: a result card with nowhere to click`);
  assert(preview.includes(`<a class="by-date" href="${href}"`) || preview.includes(`<a href="${href}"`),
    `kind ${kind}: the card navigates somewhere no link goes — unreachable by keyboard or middle-click`);
  assert.strictEqual(hrefAttr(card(fixture, { full: true })), null, `kind ${kind}: the permalink links to itself`);
}
// Where an event lives is selfHref's answer, and nothing else builds the path.
const appSrc = readFileSync(new URL("../../main/resources/web/app.js", import.meta.url), "utf8");
assert(!/`\/\$\{(noteId|npub)\(/.test(appSrc),
  "app.js builds an entity path by hand — ask cards/base.js's selfHref instead, so every route to an event agrees");

// A profile's page is the person: the kind 0's id names one revision of a bio.
assert.strictEqual(hrefAttr(card(ev(0, [], "{}"))), `/${npub(pk)}`, "a profile card opens the person");
assert.strictEqual(hrefAttr(card(ev(1, [], "hi"))), `/${noteId(eid)}`, "a regular event opens the event");

// A parameterized replaceable event's page is its address: an id names one revision. It is also
// what makes a provenance pill and the list card open the same page.
assert.strictEqual(hrefAttr(card(ev(30392, [["d", "tl-verified"], ["title", "Verified Human"], ["p", pk]]))),
  `/${naddr(`30392:${pk}:tl-verified`)}`, "a Trusted List opens its address, not one revision of it");
assert.strictEqual(hrefAttr(card(ev(30023, [["d", "essay"], ["title", "An Essay"]], "words"))),
  `/${naddr(`30023:${pk}:essay`)}`, "every addressable kind takes the same rule, not just the new ones");
// An absent `d` is a legal address (NIP-01 reads it as the empty string).
assert.strictEqual(hrefAttr(card(ev(30392, [["title", "Untitled"], ["p", pk]]))),
  `/${naddr(`30392:${pk}:`)}`, "an addressable event with no d is addressed by the empty d");
// 10040 is a plain replaceable kind outside 30000-39999, with no `d` to be addressed by.
assert.strictEqual(hrefAttr(card(ev(10040, [["30382:rank", pk, "wss://x"]]))),
  `/${noteId(eid)}`, "a non-addressable replaceable kind still opens by id");
// `naddr`'s TLV length prefix is one byte, so a `d` over 255 UTF-8 bytes has no encoding.
assert.strictEqual(
  hrefAttr(card(ev(30023, [["d", "d".repeat(256)], ["title", "Long d"]], "words"))),
  `/${noteId(eid)}`,
  "an address that cannot be encoded falls through to the note — a card with an id is never unclickable",
);
// The fall-through is the encoding's, not a relaxation of the rule.
assert.strictEqual(hrefAttr(card(ev(30023, [["d", "d".repeat(255)], ["title", "Long d"]], "words"))),
  `/${naddr(`30023:${pk}:${"d".repeat(255)}`)}`, "…and one that fits its byte is still addressed");

// An event with no usable id has nowhere to go, and must not offer "/".
assert.strictEqual(hrefAttr(card({ kind: 1, pubkey: pk, created_at: now, tags: [], content: "x" })), null,
  "no id, no click target — navigating to the home page is not the same as opening the note");

// Names over npubs, npubs over nothing, hex never.
seedProfiles([
  ev(0, [], JSON.stringify({ name: "bob", display_name: "Bob Score" }), now),            // pk
  { ...ev(0, [], JSON.stringify({ name: "olga" }), now), pubkey: pk2 },                  // pk2, the author
]);
const scored = card({ ...ev(30382, [["d", pk], ["rank", "42"]]), pubkey: pk2 }, { full: true });
assert(scored.includes(">Bob Score</a>"), "score subject shows the name");
assert(!scored.includes(">npub1"), "score subject shows no npub once named");
const observer = card({ ...ev(10040, [["30382:rank", pk, "wss://x"]]), pubkey: pk2 }, { full: true });
assert(observer.includes(">Bob Score</a>"), "observer service shows the name");
// A named profile drops its pubkey row; a nameless one keeps it, as npub.
assert(!card(ev(0, [], JSON.stringify({ name: "carol" })), { full: true }).includes("<dt>pubkey</dt>"), "named profile: no pubkey row");
const nameless = card(ev(0, [], "{}"), { full: true });
assert(nameless.includes("<dt>pubkey</dt>") && nameless.includes(">npub1"), "nameless profile: npub row remains");
// Rendered text never contains a bare hex pubkey (attributes like data-pk may).
for (const html of [scored, observer, nameless]) {
  const text = html.replace(/<[^>]*>/g, " ");
  assert(!text.includes(pk) && !text.includes(pk2), "hex is a storage format, not display text");
}

// ---- replies name a person, and link the parent ---------------------------
//
// Three claims: which `e` tag is the parent (NIP-10: reply, else root, else the last one), who
// wrote it (the tag's 5th slot, its 4th, a NIP-22 `p`), and where the link goes (the parent event,
// never the profile). Asserted on the reply line alone, since the card around it links both.
const parentId = "a".repeat(64);
const rootId = "c".repeat(64);
const lineOf = (html) => (/<div class="reply-line">([\s\S]*?)<\/div>/.exec(html) || ["", ""])[1];
const linkedTo = (html) => {
  const href = (/href="\/([a-z0-9]+)"/.exec(lineOf(html)) || ["", ""])[1];
  return href ? nip19Parse(href) : null;
};

// pk2 is "olga" in the cache; here she is the parent's author, hinted where NIP-10 puts it.
const reply = card(ev(1, [["e", gitRootId, "", "root"], ["e", parentId, "wss://hint.example", "reply", pk2]], "my answer"), { full: true });
assert(lineOf(reply).includes("in reply to"), "a reply says what it is");
assert(lineOf(reply).includes(">olga</a>"), "the parent is a person, named");
assert(!lineOf(reply).includes("npub1") || !lineOf(reply).includes(">npub1"), "a known name displaces the npub");
assert(!/href="\/npub1/.test(lineOf(reply)), "the link is not the parent's profile");
assert.strictEqual(linkedTo(reply).id, parentId, "the `reply` marker wins over `root`");
assert.deepStrictEqual(linkedTo(reply).relays, ["wss://hint.example"], "the tag's relay hint rides into the link");
assert.strictEqual(linkedTo(reply).author, pk2, "…and so does the author, for the entity page's fallback");
// The preview and the permalink say the same thing: one template, two depths.
assert(lineOf(card(ev(1, [["e", parentId, "", "reply", pk2]], "x"))).includes(">olga</a>"), "the results list names them too");

// Marker precedence, and the deprecated positional fallback the corpus is full of.
assert.strictEqual(linkedTo(card(ev(1, [["e", parentId, "", "root"]], "x"), { full: true })).id, parentId,
  "a lone root marker IS the parent — a direct reply to the opening post marks nothing else");
assert.strictEqual(linkedTo(card(ev(1, [["e", rootId], ["e", parentId]], "x"), { full: true })).id, parentId,
  "unmarked: the last `e` tag is the parent");
assert.strictEqual(linkedTo(card(ev(1, [["e", rootId], ["e", parentId, "", "mention"]], "x"), { full: true })).id, rootId,
  "a `mention` is a quote, not a parent");
assert.strictEqual(lineOf(card(ev(1, [], "not a reply at all"), { full: true })), "", "a note that answers nothing says nothing");
assert.strictEqual(lineOf(card(ev(7, [["e", parentId]], "+"), { full: true })), "",
  "a reaction already says 'liked <note>' — it does not also reply to it");

// Nobody named: the shape a reply takes before the lookup lands.
const unresolved = card(ev(1, [["e", parentId]], "x"), { full: true });
assert(lineOf(unresolved).includes("note1"), "an unresolved parent still shows what it points at");
assert.strictEqual(linkedTo(unresolved).id, parentId, "…and still links there");

// NIP-22 puts the author where NIP-10 puts the marker, and requires a `p` naming the same person.
assert(lineOf(card(ev(1111, [["e", parentId, "", pk2]], "c"), { full: true })).includes(">olga</a>"), "NIP-22's 4th slot is the author");
assert(lineOf(card(ev(1111, [["e", parentId], ["p", pk2]], "c"), { full: true })).includes(">olga</a>"), "a NIP-22 comment's `p` names the parent's author");
// A comment on an article has no `e` at all: the author is in the address.
const onArticle = card(ev(1111, [["A", `30023:${pk2}:art`], ["a", `30023:${pk2}:art`]], "c"), { full: true });
assert(lineOf(onArticle).includes(">olga</a>") && /href="\/naddr1/.test(lineOf(onArticle)), "an addressable parent names its author and links the address");

// A NIP-28 channel message carries the channel in its root `e` tag; read as a
// parent it would put "in reply to" over every line in the room.
assert.strictEqual(lineOf(card(ev(42, [["e", gitRootId, "", "root"]], "hi all"), { full: true })), "", "a channel is not a parent");
assert.strictEqual(linkedTo(card(ev(42, [["e", gitRootId, "", "root"], ["e", parentId, "", "reply", pk2]], "hi"), { full: true })).id, parentId,
  "…but a reply inside one is");

// REPLY_KINDS is read by namedPubkeys to decide whose profile to load, so it
// must be exactly the kinds whose card draws the line, in both directions.
for (const kind of REPLY_KINDS) {
  const html = card(ev(kind, [["e", parentId, "", "reply", pk2]], "answering"), { full: true });
  assert(lineOf(html).includes(">olga</a>"), `kind ${kind} is declared reply-shaped, but its card names no parent`);
}
for (const kind of [...renderers.keys()].filter((k) => !REPLY_KINDS.has(k))) {
  assert.strictEqual(lineOf(card(ev(kind, [["e", parentId, "", "reply", pk2]], "x"), { full: true })), "",
    `kind ${kind} draws a reply line without being declared reply-shaped, so its parent's profile is never loaded`);
}

// Whoever the line names, namedPubkeys must declare.
assert(namedPubkeys(ev(1, [["e", parentId, "", "reply", pk2]])).includes(pk2), "the parent's author is a name this page owes itself");
assert(namedPubkeys(ev(1111, [["a", `30023:${pk2}:art`]])).includes(pk2), "…including the one written into an address");
assert.deepStrictEqual(namedPubkeys(ev(1, [["e", parentId]])), [], "an unhinted parent declares nobody until the lookup lands");

// A set renders its contents, not only a title and a badge.
const bookmarks = card(ev(30003, [
  ["d", "reading"], ["title", "Reading list"], ["description", "things worth keeping"],
  ["a", `30023:${pk}:art`], ["e", eid], ["t", "books"],
]), { full: true });
assert(bookmarks.includes("bookmark set"), "30003 says what it is");
assert(bookmarks.includes("Reading list") && bookmarks.includes("things worth keeping"), "30003 shows its own title and description");
assert(bookmarks.includes('href="/naddr1'), "an `a` tag links to its entity page");
assert(bookmarks.includes('href="/note1'), "an `e` tag links to its note page");
assert(bookmarks.includes("1 article") && bookmarks.includes("1 hashtag"), "each section counts what it holds");

// A list whose items are all NIP-44 encrypted is common; "0 people" would claim an empty list.
const private_ = card(ev(10000, [], "AbCdEf==?iv=xyz"), { full: true });
assert(private_.includes("nothing public here") && private_.includes("encrypted"), "an all-private list says so");
assert(!private_.includes("0 people"), "an encrypted list is not an empty one");

// A list's people are drawn as a grid of face + name, one cell per person.
const muted = card(ev(10000, [["p", pk2]]), { full: true });
assert(muted.includes('class="people-grid full"') && muted.includes('class="person-cell"'), "a list's people get a cell each");
assert(muted.includes(">olga</span>"), "…with the name under the face");
assert(!card(ev(10000, [["p", pk2]])).includes("face-strip"), "the preview draws the same grid the permalink does");
// Two depths, one template: the stylesheet is told which by the same `full` the frame carries.
assert(card(ev(10000, [["p", pk2]])).includes("av-xl") && muted.includes("av-xxl"),
  "the preview takes the smaller face and the permalink the larger one");

// When a list overruns its cap, the last cell is the count.
const crowd = Array.from({ length: PEOPLE_GRID.full + 5 }, (_, i) => i.toString(16).padStart(64, "0"));
// The more-cell is `person-cell more`: the open-ended match counts every cell, the `href` one only people.
const cells = (html) => (html.match(/class="person-cell/g) || []).length;
const faces = (html) => (html.match(/class="person-cell" href/g) || []).length;
const follows = ev(3, crowd.map((p) => ["p", p]));
for (const [depth, opts, cap] of [["preview", undefined, PEOPLE_GRID.preview], ["full", { full: true }, PEOPLE_GRID.full]]) {
  const html = card(follows, opts);
  assert.strictEqual(cells(html), cap, `the ${depth} grid fills exactly its cap`);
  assert.strictEqual(faces(html), cap - 1, `…with the last cell spent on the count, not on a face`);
  assert(html.includes(`+${crowd.length - (cap - 1)}`), `the ${depth} grid counts out loud who did not fit`);
}
// Under the cap there is no more-cell at all, and every cell is a person.
const short = card(ev(3, [["p", pk2], ["p", pk]]));
assert.strictEqual(cells(short), 2);
assert.strictEqual(faces(short), 2, "a list inside the cap spends no cell on a count");
assert(!short.includes("more-face"), "…and claims nothing beyond itself");
// Four digits do not fit in the circle.
const many = card(ev(3, Array.from({ length: 8433 }, (_, i) => ["p", i.toString(16).padStart(64, "0")])));
assert(many.includes("+8.4k"), "a four-figure remainder is compacted, not printed in full");

// shortNpub's head-and-tail form is already short and must not be clipped a second time.
const strangerCell = card(ev(3, [["p", "a".repeat(64)]]));
assert(/class="person-name mono">npub1[a-z0-9]{7}…</.test(strangerCell), "the grid's npub fallback is truncated once");
assert(strangerCell.includes(`title="${npub("a".repeat(64))}"`), "…and the whole key is in the title");

// Repeated `p` tags are one person.
const twice = card(ev(3, [["p", pk2], ["p", pk2], ["p", pk], ["p", "not-a-pubkey"]]), { full: true });
assert.strictEqual(cells(twice), 2, "a repeated person is one person");
assert(twice.includes("follows <b>2</b> people"), "…and the count says the same number the grid draws");
const twiceList = card(ev(10000, [["p", pk2], ["p", pk2]]), { full: true });
assert(twiceList.includes("1 person"), "the same rule, counted by the NIP-51 table");

// PEOPLE_GRID_KINDS is what cards.js loads profiles from, so it must be exactly the grid kinds.
for (const kind of renderers.keys()) {
  const drawn = card(ev(kind, [["d", "x"], ["p", pk2]]), { full: true }).includes("people-grid");
  assert.strictEqual(drawn, PEOPLE_GRID_KINDS.has(kind),
    `kind ${kind}: draws a people grid = ${drawn}, but declares one = ${PEOPLE_GRID_KINDS.has(kind)}`);
}

// A tab is a `kinds` filter, so a kind in the wrong tab is a search that cannot find it under any chip.
const TAB_TONE = { people: "people", notes: "note", articles: "article", media: "media", code: "code", live: "live", lists: "list" };
const appJs = readFileSync(new URL("../../main/resources/web/app.js", import.meta.url), "utf8");
const tabs = [...appJs.matchAll(/slug:\s*"([a-z]+)",\s*kinds:\s*(null|\[([^\]]*)\])/g)]
  .map((m) => [m[1], m[3] ? m[3].split(",").map((s) => Number(s.trim())).filter(Number.isFinite) : []]);
assert(tabs.length >= 2, "the tab table parsed");
for (const [slug, kinds] of tabs) {
  for (const k of kinds) {
    assert(renderers.has(k), `tab "${slug}" filters on kind ${k}, which no renderer covers`);
    assert.strictEqual(kindTone(k), TAB_TONE[slug], `tab "${slug}" filters on kind ${k}, which belongs to the "${kindTone(k)}" family`);
  }
}

// Totality: no renderer may throw, whatever the event looks like. entity.js renders a hinted
// relay's answer outside showEntity's try/catch.
const DEGENERATE = [
  ["no tags array", (k) => ({ id: eid, pubkey: pk, kind: k, created_at: now })],
  ["null in tags", (k) => ({ id: eid, pubkey: pk, kind: k, created_at: now, tags: [["title"], null, ["e"], []] })],
  ["no id", (k) => ({ pubkey: pk, kind: k, created_at: now, tags: [] })],
  ["no created_at", (k) => ({ id: eid, pubkey: pk, kind: k, tags: [] })],
  ["null content", (k) => ({ id: eid, pubkey: pk, kind: k, created_at: now, tags: [], content: null })],
  ["nothing but a kind", (k) => ({ kind: k })],
];
for (const [label, make] of DEGENERATE) {
  for (const kind of [...renderers.keys(), 999999]) {
    for (const opts of [undefined, { full: true }]) {
      let html;
      try { html = card(make(kind), opts); }
      catch (e) { assert.fail(`kind ${kind} threw on a ${label} event (${opts ? "full" : "preview"}): ${e.message}`); }
      assert(!html.includes("Invalid Date"), `kind ${kind}: "Invalid Date" reached the page on a ${label} event`);
      assert(!html.includes("undefined"), `kind ${kind}: leaked "undefined" on a ${label} event`);
    }
    // Not the same code path: a row reads fields a card never touches, and renders on every keystroke.
    let row;
    try { row = popupRow(make(kind), 0); }
    catch (e) { assert.fail(`kind ${kind}: its type-ahead row threw on a ${label} event: ${e.message}`); }
    assert(!row.includes("undefined") && !row.includes("[object Object]"),
      `kind ${kind}: a ${label} event leaked into its type-ahead row`);
  }
}

// Whoever a card writes a name for, namedPubkeys must name too. Rendered with an empty profile
// cache, since personLink puts the full npub in the title attribute either way.
seedProfiles([]);
for (const [kind, fixture] of FIXTURES) {
  const html = card(fixture, { full: true });
  const shown = [...new Set([...html.matchAll(/title="(npub1[a-z0-9]+)"/g)].map((m) => pubkeyParam(m[1])))].filter(Boolean);
  const declared = new Set(namedPubkeys(fixture, { full: true }));
  // The author is the byline's own link, loaded by every caller already.
  const undeclared = shown.filter((p) => p !== fixture.pubkey && !declared.has(p));
  assert.deepStrictEqual(undeclared, [], `kind ${kind}: names ${undeclared.length} pubkey(s) namedPubkeys does not declare, so they render as npubs`);
}
// The zap receipt's sender lives in no tag at all.
const zapper = "d".repeat(64);
assert(namedPubkeys(ev(9735, [["p", pk2],
  ["description", JSON.stringify({ pubkey: zapper, tags: [["amount", "1000"]] })]])).includes(zapper),
  "the zap sender comes out of the stringified request, not the tags");
assert.deepStrictEqual(namedPubkeys(ev(9735, [["description", "not json at all"]])), [],
  "a malformed receipt names nobody rather than throwing");
// A list's people are declared only as far as this depth's grid reaches.
for (const [depth, opts, cap] of [["preview", undefined, PEOPLE_GRID.preview], ["full", { full: true }, PEOPLE_GRID.full]]) {
  const declared = namedPubkeys(follows, opts);
  assert.deepStrictEqual(declared, crowd.slice(0, cap - 1),
    `at ${depth} depth a follow list declares exactly the faces it draws, in order`);
}
// People a card only shows a picture of stay undeclared: a face needs no lookup.
assert.deepStrictEqual(namedPubkeys(ev(34550, [["d", "c"], ["p", pk2, "", "moderator"]])), [], "faces are not names");

// Nothing an event carries reaches the page as markup. The payload is short so no clip truncates
// it, opens with a quote to catch an attribute break-out, and is asserted never to appear
// verbatim, which `onerror=` would in correctly escaped output too.
const XSS = `"><b BAD>`;
const poison = (fixture) => ({
  ...fixture,
  content: XSS,
  tags: [...(fixture.tags || []).map((t) => [t[0], ...t.slice(1).map(() => XSS)]), [XSS, XSS, XSS]],
});
const ESCAPED = (html) => !html.includes("<b BAD>");

for (const [kind, fixture] of FIXTURES) {
  // The row is a third interpolation site, whose values come straight off a stranger's JSON.
  assert(ESCAPED(popupRow(poison(fixture), 0)), `kind ${kind}: an event's own text reached the type-ahead row as MARKUP`);
  for (const opts of [undefined, { full: true }]) {
    const depth = opts ? "permalink" : "preview";
    assert(ESCAPED(card(poison(fixture), opts)), `kind ${kind}: an event's own text reached the ${depth} as MARKUP`);
    // A named timestamp tag that is not a number is escaped, not formatted.
    for (const name of ["endsAt", "expiration", "starts", "start", "end", "published_at"]) {
      const html = card({ ...fixture, tags: [...(fixture.tags || []), [name, XSS]] }, opts);
      assert(ESCAPED(html), `kind ${kind}: a "${name}" tag reached the ${depth} as MARKUP`);
    }
  }
}

// A url is not linkable just because it is escaped: `javascript:` in an href runs on click.
for (const [kind, fixture] of FIXTURES) {
  for (const name of ["r", "web", "url", "website", "streaming", "recording"]) {
    const html = card({ ...fixture, tags: [...(fixture.tags || []), [name, "javascript:BAD"]] }, { full: true });
    assert(!/href="javascript:/i.test(html), `kind ${kind}: a "${name}" tag became a javascript: href`);
  }
}
assert.strictEqual(safeUrl("javascript:alert(1)"), null, "javascript: is not a link");
assert.strictEqual(safeUrl("java\nscript:alert(1)"), null, "nor is it once the parser has stripped the newline");
assert.strictEqual(safeUrl("data:text/html,<script>"), null, "data: documents are not links either");
assert.strictEqual(safeUrl("/local/path"), null, "a relative url never meant this origin");
assert.strictEqual(safeUrl("https://ok.example/x?a=1"), "https://ok.example/x?a=1", "http(s) passes through unchanged");

// poison() destroys the `e` tag shape the reply line needs, so the line gets its own poisoning
// in the slots that survive validation.
for (const kind of REPLY_KINDS) {
  for (const opts of [undefined, { full: true }]) {
    const html = card(ev(kind, [
      ["e", rootId, XSS, "root", XSS],
      ["e", parentId, `wss://evil.example/${XSS}`, "reply", XSS],
      ["a", `30023:${pk2}:${XSS}`],
      ["p", XSS],
    ], XSS), opts);
    assert(lineOf(html), `kind ${kind}: the poisoned reply fixture rendered no line, so this asserts nothing`);
    assert(ESCAPED(html), `kind ${kind}: a reply's parent tag reached the ${opts ? "permalink" : "preview"} as MARKUP`);
  }
}

// ---- an article previews as cover, title, summary --------------------------
//
// Asserted on the card rather than the reducer: the property is what a reader sees.
const marked = ev(30023, [["d", "a"], ["title", "On Relays, Bandwidth, and Who Pays"], ["image", "https://x/cover.jpg"],
  ["published_at", String(now - 400)]],
  "# On Relays, Bandwidth, and Who Pays\n\n## Somebody is paying for this\n\nA relay that accepts **every** event " +
  "from *everybody* is running a [charity](https://x/c) with an unbounded budget.\n\n## The ways out\n\n" +
  "1. Paid relays.\n2. Zaps to the operator.\n");
const markedPreview = card(marked);
for (const mark of ["##", "**", "](", "1. "]) {
  assert(!markedPreview.includes(mark), `the preview summary showed markdown \`${mark}\` instead of the prose under it`);
}
assert(markedPreview.includes("A relay that accepts every event from everybody"),
  "the reduced summary is the article's first prose, with its emphasis unwrapped");
assert(!markedPreview.includes("Somebody is paying for this A relay"),
  "a heading labels the prose under it; joined into one run it reads as a sentence that isn't there");
assert.strictEqual((markedPreview.match(/On Relays, Bandwidth, and Who Pays/g) || []).length, 1,
  "a body opening with the article's own title must not make the preview say it twice");
// One voice: the line stands in for the article whether it came from the `summary` tag or the body.
const summarised = card(ev(30023, [["d", "b"], ["title", "T"], ["summary", "the summary tag itself"]], "body"));
assert(summarised.includes("the summary tag itself") && /result-body[^"]*muted/.test(summarised), "tag summary is muted");
assert(/result-body[^"]*muted/.test(markedPreview), "and so is the one reduced from the body");
// The cover is a banner, not an avatar, and the permalink keeps the full markdown.
assert(markedPreview.includes('class="thumb cover"'), "an article's image gets the landscape frame");
assert(card(marked, { full: true }).includes("## Somebody is paying for this"),
  "the permalink still shows the body exactly as its author wrote it");
// `published_at` under a byline that already says "6m ago" is the same date twice.
assert(!markedPreview.includes("published"), "the preview carries one date, in the byline");
assert(card(marked, { full: true }).includes("published"), "the permalink is where the publication date belongs");
// An article that is nothing but headings still says something.
assert(card(ev(30023, [["d", "c"], ["title", "T"]], "# Only a heading")).includes("Only a heading"),
  "some words beat none when the body has no prose at all");
// The type-ahead row takes the same reduction: its few characters must not go on heading syntax.
const markedRow = rowOf(marked);
assert.strictEqual(markedRow.name, "On Relays, Bandwidth, and Who Pays", "the row leads with the title");
for (const mark of ["#", "**", "](", "1. "]) {
  assert(!markedRow.sub.includes(mark), `the row's summary showed markdown \`${mark}\` instead of the prose under it`);
}
assert(markedRow.sub.startsWith("A relay that accepts every event from everybody"), "…and the prose is what follows it");

console.log(`all kinds: ${FIXTURES.length} bespoke renderers + type-ahead rows + generic floor, all assertions passed`);
