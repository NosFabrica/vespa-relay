// The URL is the search: what a view writes into the address bar comes back as the same view,
// a link is routed to the view it names, and a restore never pushes the place it restores.
import assert from "node:assert/strict";

const {
  FEED_URL, feedUrl, isFeedHref, entitySegOf, searchUrl, readSearch, linkTarget, whileRestoring, pushUrl, goTo,
} = await import(new URL("../../main/resources/web/urlstate.js", import.meta.url));
const { npub } = await import(new URL("../../main/resources/web/shared/nip19.js", import.meta.url));

const ok = (name) => console.log(`  ✓ ${name}`);
const TABS = [
  { slug: "all", kinds: null },
  { slug: "notes", kinds: [1] },
  { slug: "media", kinds: [20, 21] },
];
const PK = "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2";

{
  const view = { text: "bitcoin mining", page: 2, tab: TABS[2], tabs: TABS, sort: "recent", spam: true, viewingAs: PK };
  const url = searchUrl(view);
  assert.ok(url.startsWith("/?"), "anchored at the root, whatever path launched it");
  assert.ok(url.includes(`as=${npub(PK)}`), "the lens is written as an npub, like everywhere else on the page");
  const back = readSearch(url.slice(1), TABS);
  assert.deepEqual(
    { text: back.text, page: back.page, tab: back.tab, sort: back.sort, spam: back.spam, as: back.as, feed: back.feed },
    { text: view.text, page: view.page, tab: view.tab, sort: view.sort, spam: view.spam, as: PK, feed: false },
  );
  ok("a results view written to the url comes back as the same view");
}

{
  assert.equal(searchUrl({ text: "x", tab: TABS[0], tabs: TABS }), "/?q=x", "the first chip and every default write nothing");
  assert.equal(searchUrl({ text: "x", showing: false, tab: TABS[0], tabs: TABS }), "/", "a query not on screen is not a place");
  assert.equal(searchUrl({ text: "", page: 3, tab: TABS[0], tabs: TABS }), "/", "no query, no page");
  const r = readSearch("?q=%20padded%20&tab=nope&as=not-a-key", TABS);
  assert.equal(r.text, "padded", "the query is trimmed");
  assert.equal(r.tab, TABS[0], "an unknown chip is the first");
  assert.equal(r.as, null, "a lens that is not a key is no lens");
  assert.equal(readSearch(`?as=${PK}`, TABS).as, PK, "…and bare hex is one");
  ok("defaults stay out of the url, and what does not parse reads as the default");
}

{
  assert.equal(feedUrl(TABS[0]), FEED_URL);
  assert.equal(feedUrl(TABS[1]), "/?feed=1&tab=notes");
  for (const t of TABS) {
    const r = readSearch(feedUrl(t).slice(1), TABS);
    assert.ok(r.feed && r.tab === t, `the feed narrowed to ${t.slug} reopens narrowed`);
    assert.ok(isFeedHref(feedUrl(t)));
  }
  assert.ok(!isFeedHref("/?feed=10"), "a prefix of the flag is not the flag");
  ok("the feed url carries its chip both ways");
}

{
  assert.equal(entitySegOf("/npub1abc"), "npub1abc");
  assert.equal(entitySegOf("/NOTE1ABC"), "NOTE1ABC", "bech32 may be upper-case");
  assert.equal(entitySegOf("/"), null);
  assert.equal(entitySegOf("/npub1abc/extra"), null);
  assert.equal(entitySegOf("/stats.html"), null);
  assert.deepEqual(linkTarget("/"), { view: "home" });
  assert.deepEqual(linkTarget("/?feed=1&tab=media"), { view: "feed" });
  assert.deepEqual(linkTarget("/?q=%23nostr&tab=notes"), { view: "search", text: "#nostr" });
  assert.deepEqual(linkTarget("/nevent1qqq"), { view: "entity" });
  assert.equal(linkTarget("/stats.html"), null, "another page is the browser's to load");
  assert.equal(linkTarget("/web/app.css"), null);
  ok("a link is routed to the view it names, and anything else is left to the browser");
}

{
  const pushed = [];
  globalThis.location = { pathname: "/", search: "" };
  globalThis.history = { pushState: (_s, _t, url) => { pushed.push(url); const u = new URL(url, "http://x"); location.pathname = u.pathname; location.search = u.search; } };
  pushUrl("/?q=a");
  pushUrl("/?q=a");
  assert.deepEqual(pushed, ["/?q=a"], "re-submitting the same search is not a second place");
  const r = whileRestoring(() => { pushUrl("/?q=b"); return 7; });
  assert.equal(r, 7);
  assert.deepEqual(pushed, ["/?q=a"], "a restore does not push what it restores");
  assert.throws(() => whileRestoring(() => { throw new Error("x"); }));
  pushUrl("/?q=c");
  assert.deepEqual(pushed, ["/?q=a", "/?q=c"], "…and a restore that threw does not leave pushes off");
  whileRestoring(() => goTo("/note1x"));
  assert.deepEqual(pushed.at(-1), "/note1x", "a navigation the reader asked for is pushed even mid-restore");
  ok("the back stack gets one entry per place, and none from a restore");
}

console.log("urlstate: the url and the view are one thing, both ways");
