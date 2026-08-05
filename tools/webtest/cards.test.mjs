import assert from 'assert';
import { readFileSync } from 'node:fs';
globalThis.location = { protocol: "http:", host: "localhost:7787" };
globalThis.window = { addEventListener: () => {} };

const { card, namedPubkeys } = await import(new URL("../../relay/src/main/resources/web/cards.js", import.meta.url));
const { pubkeyParam } = await import(new URL("../../relay/src/main/resources/web/shared/nip19.js", import.meta.url));
const { renderers, safeUrl } = await import(new URL("../../relay/src/main/resources/web/cards/base.js", import.meta.url));
const { kindLabel, kindTone, KNOWN_KINDS } = await import(new URL("../../relay/src/main/resources/web/shared/kinds.js", import.meta.url));
const { seedProfiles } = await import(new URL("../../relay/src/main/resources/web/shared/profiles.js", import.meta.url));

const pk = "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2";
const pk2 = "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d";
const eid = "0123456789abcdef".repeat(4);
const now = Math.floor(Date.now() / 1000);
const ev = (kind, tags = [], content = "") => ({ id: eid, pubkey: pk, kind, created_at: now - 3600, tags, content });

// One fixture per kind, each asserting the renderer's distinctive output.
// [kind, event, substring the FULL render must contain]
const FIXTURES = [
  [0,     ev(0, [], JSON.stringify({ name: "alice", about: "hi", picture: "https://x/p.png", nip05: "a@b.co", website: "https://a.co" })), "nip05"],
  [1,     ev(1, [], "hello world"), "hello world"],
  [11,    ev(11, [], "thread post"), "thread post"],
  [9802,  ev(9802, [["r", "https://src.example/page"]], "the quoted passage"), "quote"],
  [40,    ev(40, [], JSON.stringify({ name: "my channel", about: "chat", picture: "https://x/c.png" })), "my channel"],
  [41,    ev(41, [], JSON.stringify({ name: "renamed channel" })), "renamed channel"],
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

  // ---- the reactive kinds: what they point AT is the card ------------------
  [5,     ev(5, [["e", eid], ["k", "1"]], "posted by mistake"), "asks to delete 1 event"],
  [6,     ev(6, [["e", eid]], JSON.stringify({ content: "the original note" })), "the original note"],
  [16,    ev(16, [["e", eid]], ""), "reposted"],
  [7,     ev(7, [["e", eid]], "+"), "liked"],
  [17,    ev(17, [["r", "https://site.example"]], "+"), "site.example"],
  [8,     ev(8, [["a", `30009:${pk}:helper`], ["p", pk2]]), "1 recipient"],
  [9,     ev(9, [], "a chat line"), "a chat line"],
  [42,    ev(42, [["e", eid]], "a channel line"), "a channel line"],
  [1311,  ev(1311, [["a", `30311:${pk}:show`]], "a live chat line"), "a live chat line"],
  [1111,  ev(1111, [["E", eid], ["e", eid]], "a comment"), "replying to"],
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

  // ---- NIP-51: every list and set, which is what 30003 was missing ---------
  [10000, ev(10000, [["p", pk2], ["t", "spam"], ["word", "airdrop"], ["e", eid]]), "1 word"],
  [10001, ev(10001, [["e", eid]]), "1 note"],
  [10003, ev(10003, [["e", eid], ["a", `30023:${pk}:art`], ["t", "reading"], ["r", "https://x.example"]]), "1 article"],
  [10004, ev(10004, [["a", `34550:${pk}:dev`]]), "1 community"],
  [10005, ev(10005, [["e", eid]]), "1 channel"],
  [10006, ev(10006, [["relay", "wss://bad.example"]]), "bad.example"],
  [10007, ev(10007, [["relay", "wss://search.example"]]), "search.example"],
  [10008, ev(10008, [["a", `30009:${pk}:b`], ["e", eid]]), "1 badge"],
  [10009, ev(10009, [["group", "abc123", "wss://groups.example"]]), "1 group"],
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

  // ---- the rest of the families the registry had half of -------------------
  [30383, ev(30383, [["d", eid], ["rank", "5"]]), "rank-big"],
  [30384, ev(30384, [["d", `30023:${pk}:art`], ["rank", "9"]]), "rank-big"],
  [1618,  ev(1618, [["subject", "Add the thing"]], "please merge"), "Add the thing"],
  [1619,  ev(1619, [["subject", "Add the thing, again"]], "rebased"), "rebased"],
  [1622,  ev(1622, [], "a git reply"), "a git reply"],
  [1630,  ev(1630, [["e", eid]], "reopening"), "<b>open</b>"],
  [1631,  ev(1631, [["e", eid]], "merged"), "<b>applied or merged</b>"],
  [1632,  ev(1632, [["e", eid]], "wontfix"), "<b>closed</b>"],
  [1633,  ev(1633, [["e", eid]], "not ready"), "<b>draft</b>"],
  [30618, ev(30618, [["d", "repo"], ["refs/heads/master", "abc"], ["HEAD", "ref: refs/heads/master"]]), "1 branch"],
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
];

// THE COVERAGE CLAIM, enforced: every registered kind must have a fixture,
// and every fixture must target a registered kind.
const registered = new Set(renderers.keys());
const covered = new Set(FIXTURES.map(([k]) => k));
const missing = [...registered].filter((k) => !covered.has(k)).sort((a, b) => a - b);
const stale = [...covered].filter((k) => !registered.has(k)).sort((a, b) => a - b);
assert.deepStrictEqual(missing, [], `registered kinds without a fixture: ${missing}`);
assert.deepStrictEqual(stale, [], `fixtures for unregistered kinds: ${stale}`);

// The badge is the only part of a card that says WHAT it is, so a kind good
// enough to render is a kind good enough to name and tint. Without this,
// dressing a kind and forgetting its label ships a beautifully rendered
// bookmark set whose badge still reads "kind 30003" — which is the shape of
// the bug this suite grew to catch.
const unnamed = [...registered].filter((k) => kindLabel(k) === `kind ${k}`).sort((a, b) => a - b);
const untinted = [...registered].filter((k) => !kindTone(k)).sort((a, b) => a - b);
assert.deepStrictEqual(unnamed, [], `registered kinds with no label: ${unnamed}`);
assert.deepStrictEqual(untinted, [], `registered kinds with no family tone: ${untinted}`);

// KNOWN_KINDS is the answer to "which kinds do we support", and kind_stats.html
// counts exactly it. An identity, not a subset: a label for a kind nothing
// renders would put a row on the operator's page for a kind the search cannot
// show, and a renderer missing from it would go uncounted.
assert.deepStrictEqual(KNOWN_KINDS, [...registered].sort((a, b) => a - b),
  "the kinds we count and the kinds we render must be the same set");
const kindStats = readFileSync(new URL("../../relay/src/main/resources/kind_stats.html", import.meta.url), "utf8");
assert(/import\s*\{[^}]*KNOWN_KINDS[^}]*\}\s*from\s*"\/web\/shared\/kinds\.js"/.test(kindStats),
  "kind_stats.html must read its kinds from shared/kinds.js, not carry a second copy");

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

// Permalink depth is real: long content clamps in preview, not in full.
const long = "x".repeat(2000);
const preview = card(ev(1, [], long));
const full = card(ev(1, [], long), { full: true });
assert(preview.includes("clamp") && preview.includes("…"), "preview clips and clamps");
assert(!full.includes("clamp") && full.includes(long), "full mode renders every character");

// Media corner cases: a full-depth video with no url renders its text ONCE
// (the embed-vs-body composition double-rendered it), and a preview picture
// with no url still shows its text (it used to show nothing).
const noUrlVideo = card(ev(21, [["title", "the only body line"]]), { full: true });
assert.strictEqual((noUrlVideo.match(/the only body line/g) || []).length, 1, "video full/no-url: body once");
assert(card(ev(20, [], "picture text, no url")).includes("picture text, no url"), "picture preview keeps text without a url");

// Escaping holds in every renderer that touches content.
const hostile = card(ev(1, [], `<img src=x onerror=alert(1)>`), { full: true });
assert(!hostile.includes("<img src=x"), "content is escaped");

// Cards link internally now, not to njump.
const note = card(ev(1, [], "hi"));
assert(note.includes('href="/note1') && note.includes('href="/npub1'), "note links internal");
assert(!note.includes("njump.me"), "search cards no longer link out");

// Names over npubs, npubs over nothing, hex never.
// With a profile in the cache, the score and observer cards name the person;
// the npub survives only in the hover title and the href.
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
// Rendered TEXT never contains a bare hex pubkey (attributes like data-pk may).
for (const html of [scored, observer, nameless]) {
  const text = html.replace(/<[^>]*>/g, " ");
  assert(!text.includes(pk) && !text.includes(pk2), "hex is a storage format, not display text");
}

// A set renders its CONTENTS, which is the whole complaint that started this:
// a 30003 with twelve saved articles used to render as a title and a badge
// reading "kind 30003". Now it names itself, counts what it holds, and links
// each entry — the `a` tags as naddr pages, the `e` tags as note pages.
const bookmarks = card(ev(30003, [
  ["d", "reading"], ["title", "Reading list"], ["description", "things worth keeping"],
  ["a", `30023:${pk}:art`], ["e", eid], ["t", "books"],
]), { full: true });
assert(bookmarks.includes("bookmark set"), "30003 says what it is");
assert(bookmarks.includes("Reading list") && bookmarks.includes("things worth keeping"), "30003 shows its own title and description");
assert(bookmarks.includes('href="/naddr1'), "an `a` tag links to its entity page");
assert(bookmarks.includes('href="/note1'), "an `e` tag links to its note page");
assert(bookmarks.includes("1 article") && bookmarks.includes("1 hashtag"), "each section counts what it holds");

// A list whose items are all NIP-44 encrypted is legal and common. Saying
// "0 people" would read as an empty list, which is a different claim.
const private_ = card(ev(10000, [], "AbCdEf==?iv=xyz"), { full: true });
assert(private_.includes("nothing public here") && private_.includes("encrypted"), "an all-private list says so");
assert(!private_.includes("0 people"), "an encrypted list is not an empty one");

// A tab is a `kinds` filter, so a kind in the wrong tab is a search that
// cannot find it under any chip. "Media" carried 31922 — a calendar date the
// live family renders — while 1986 audio was in no tab at all.
const TAB_TONE = { people: "people", notes: "note", articles: "article", media: "media", code: "code", live: "live", lists: "list" };
const appJs = readFileSync(new URL("../../relay/src/main/resources/web/app.js", import.meta.url), "utf8");
const tabs = [...appJs.matchAll(/slug:\s*"([a-z]+)",\s*kinds:\s*(null|\[([^\]]*)\])/g)]
  .map((m) => [m[1], m[3] ? m[3].split(",").map((s) => Number(s.trim())).filter(Number.isFinite) : []]);
assert(tabs.length >= 2, "the tab table parsed");
for (const [slug, kinds] of tabs) {
  for (const k of kinds) {
    assert(renderers.has(k), `tab "${slug}" filters on kind ${k}, which no renderer covers`);
    assert.strictEqual(kindTone(k), TAB_TONE[slug], `tab "${slug}" filters on kind ${k}, which belongs to the "${kindTone(k)}" family`);
  }
}

// TOTALITY: no renderer may throw, whatever the event looks like.
//
// Not a hypothetical. entity.js dials the relay hints in an identifier this
// relay does not hold, renders what comes back, and only THEN submits it here
// for verification — deliberately, so the reader sees the thing and the
// relay's verdict on it. That render is outside showEntity's try/catch and
// its caller adds no .catch, so a throwing renderer left the page on its
// skeleton with "fetching names…" still showing. 70 of the 118 kinds threw on
// an event with no `tags` array.
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
  }
}

// THE ENRICHMENT CLAIM: whoever a card writes a NAME for, namedPubkeys must
// name too — on both pages, since both now ask it rather than scanning tags
// themselves. Rendering was always shared; loading the profiles was not, and
// the results list showed npubs where the permalink showed names.
//
// Asserted by rendering with an EMPTY profile cache: personLink puts the full
// npub in the title attribute whether or not a name was found, so every one
// that appears is a person this card names and therefore a profile the page
// owed itself.
seedProfiles([]);
for (const [kind, fixture] of FIXTURES) {
  const html = card(fixture, { full: true });
  const shown = [...new Set([...html.matchAll(/title="(npub1[a-z0-9]+)"/g)].map((m) => pubkeyParam(m[1])))].filter(Boolean);
  const declared = new Set(namedPubkeys(fixture));
  // The author is the byline's own link, loaded by every caller already.
  const undeclared = shown.filter((p) => p !== fixture.pubkey && !declared.has(p));
  assert.deepStrictEqual(undeclared, [], `kind ${kind}: names ${undeclared.length} pubkey(s) namedPubkeys does not declare, so they render as npubs`);
}
// …and the zap receipt's sender, the one who lives in no tag at all.
const zapper = "d".repeat(64);
assert(namedPubkeys(ev(9735, [["p", pk2],
  ["description", JSON.stringify({ pubkey: zapper, tags: [["amount", "1000"]] })]])).includes(zapper),
  "the zap sender comes out of the stringified request, not the tags");
assert.deepStrictEqual(namedPubkeys(ev(9735, [["description", "not json at all"]])), [],
  "a malformed receipt names nobody rather than throwing");
// A follow list's thousands stay faces: naming them would be a profile fetch
// per member on every result in the list.
assert.deepStrictEqual(namedPubkeys(ev(3, [["p", pk2], ["p", pk]])), [], "faces are not names");

// THE ESCAPING CLAIM: nothing an event carries reaches the page as markup.
//
// Every renderer builds HTML by string concatenation, and a props table takes
// its VALUE raw so a row can be a link — so "escaped" is a promise each card
// makes one interpolation at a time, and four of them had quietly broken it:
// `fmtTs(tagOf(ev, …))` hands its argument back verbatim when it is not a
// number, so `["endsAt", "<img src=x onerror=…>"]` on a kind 1068 executed.
// A page that renders strangers' events cannot hold that promise by review.
//
// Asserted for the WHOLE registry, in both depths, with the payload in every
// tag value, in the content, and as a tag NAME — the last because tagsWhere
// hands names through to `<dt>` and to the mono spans in the 10040 card.
// Short on purpose: cards clip long values, and a payload longer than the
// tightest clip could be truncated past recognition and score a false pass.
// It opens with a quote so an attribute-context break-out is caught too — and
// the assertion is that the payload never appears VERBATIM, which is exactly
// what escaping prevents (`"` becomes &quot;, `<` becomes &lt;) and what raw
// interpolation produces. `onerror=…` would not do: escaped output still
// contains that text, inertly, and the test would fail on correct code.
const XSS = `"><b BAD>`;
const poison = (fixture) => ({
  ...fixture,
  content: XSS,
  tags: [...(fixture.tags || []).map((t) => [t[0], ...t.slice(1).map(() => XSS)]), [XSS, XSS, XSS]],
});
const ESCAPED = (html) => !html.includes("<b BAD>");

for (const [kind, fixture] of FIXTURES) {
  for (const opts of [undefined, { full: true }]) {
    const depth = opts ? "permalink" : "preview";
    assert(ESCAPED(card(poison(fixture), opts)), `kind ${kind}: an event's own text reached the ${depth} as MARKUP`);
    // The same, with the payload only where a timestamp is read — the tag
    // shapes above are replaced wholesale, and these four cards read a named
    // tag whose value they formatted rather than escaped.
    for (const name of ["endsAt", "expiration", "starts", "start", "end", "published_at"]) {
      const html = card({ ...fixture, tags: [...(fixture.tags || []), [name, XSS]] }, opts);
      assert(ESCAPED(html), `kind ${kind}: a "${name}" tag reached the ${depth} as MARKUP`);
    }
  }
}

// …and a url is not linkable just because it is escaped: `javascript:` in an
// href is a script the reader runs on themselves by clicking.
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

console.log(`all kinds: ${FIXTURES.length} bespoke renderers + generic floor, all assertions passed`);
