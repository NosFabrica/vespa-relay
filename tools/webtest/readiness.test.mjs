// The readiness verdict: which link of the trust chain a signed-in reader is
// missing, and what the panel is allowed to claim about it.
//
// Asserts the PROPERTIES, not the prose — the words live in readiness.js and
// are meant to be rewritten. What must not change is the ordering (the first
// unmet link wins, everything below it waits), and the three rules this page
// has already got wrong somewhere else: an unfinished read is not an absence,
// a non-answer is not a zero, and a missing denominator is not a percentage.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  assess, fraction, counted, worthShowing, REFUSED, TIMED_OUT,
} from "../../web/src/main/resources/web/shared/readiness.js";

const ok = (name) => console.log(`  ✓ ${name}`);

// A reader with everything, as the baseline every case below breaks one way.
const healthy = () => ({
  relayList: { seen: true, declared: 2, writeRelays: ["wss://relay.damus.io", "wss://nos.lol"] },
  scoreListSeen: true,
  rankService: { service: "abc", relay: "wss://nip85.nosfabrica.com" },
  scores: { here: 145968, there: 145968 },
  probe: { authed: 1, anon: 1 },
  posts: { here: 1880, there: 1880, relay: "wss://relay.damus.io" },
});

const statusOf = (v, key) => v.chain.find((l) => l.key === key)?.status;

// ---- nothing is claimed before the answer that decides it -----------------
{
  assert.equal(assess({}).state, "checking");
  assert.equal(assess({ relayList: { writeRelays: ["wss://a.com"] } }).state, "checking");
  const half = assess({ ...healthy(), scores: { here: null, there: null } });
  assert.equal(half.state, "checking");
  assert.equal(worthShowing(half), false, "a spinner for a check about to say ready is still a nag");
  ok("an unanswered stage is `checking`, and `checking` is never shown");
}

// ---- the ordering: the first unmet link wins ------------------------------
{
  // Broken at the top, with everything below it ALSO missing. A column of red
  // crosses would say four things are wrong when one is.
  const v = assess({ relayList: { seen: false, writeRelays: [] }, scoreListSeen: false, rankService: null });
  assert.equal(v.state, "no-relay-list");
  assert.equal(statusOf(v, "relayList"), "broken");
  assert.equal(statusOf(v, "scoreList"), "waiting");
  assert.equal(statusOf(v, "scores"), "waiting");
  assert.equal(statusOf(v, "ranked"), "waiting");
  assert.equal(v.chain.filter((l) => l.status === "broken").length, 1, "exactly one link is ever broken");
  ok("the first unmet link is the verdict; every link below it waits");
}

// ---- a link that waits says NOTHING about itself --------------------------
//
// The ordering above was right in the verdict column and contradicted one
// column to its left. `chainHtml` reads each link's `detail` to write its
// subtitle, a waiting link carries none, and every branch of that switch
// treated an absent detail as the healthy case — so the panel headlined
// "search can't rank for you yet" and then printed, as its own evidence,
// "Ranked search — returns results" and "Your trusted-scores list — names a
// service for rank". Two halves hold the property, because it takes two
// modules to break it.
{
  // Half one, here: a waiting link carries no detail to write words from.
  for (const facts of [
    { relayList: { seen: false, writeRelays: [] }, scoreListSeen: false, rankService: null },
    { ...healthy(), scoreListSeen: false },
    { ...healthy(), rankService: null },
    { ...healthy(), scores: { here: 0, there: 145968 } },
  ]) {
    for (const l of assess(facts).chain) {
      if (l.status !== "waiting") continue;
      assert.equal(l.detail, null, `waiting link \`${l.key}\` carried a detail to describe itself with`);
    }
  }

  // Half two, in readiness.js: the switch that turns detail into words is not
  // entered for a waiting link at all. Asserted against the SOURCE, the way
  // avatar.test.mjs checks the stylesheet it cannot import — readiness.js
  // reads the document at import and cannot be loaded here.
  const src = readFileSync(
    new URL("../../web/src/main/resources/web/readiness.js", import.meta.url), "utf8"
  );
  const chainFn = src.slice(src.indexOf("function chainHtml"), src.indexOf("function fetchFormHtml"));
  assert.ok(chainFn, "chainHtml has moved — this assertion no longer reads it");
  assert.match(
    chainFn,
    /if \(l\.status !== "waiting"\)\s*switch \(l\.key\)/,
    "chainHtml builds a subtitle for a link it never checked"
  );
  ok("a link below the break describes neither itself nor a check that never ran");
}

{
  // "No relay list" and "a relay list we cannot use" are different facts, and
  // the panel told readers the wrong one: a list naming only `ws://` relays
  // loses every entry on an https page (the browser refuses those connections
  // outright), leaving the same empty array as never having published one — so
  // somebody who HAD published a list was told we had never seen it.
  const unusable = assess({
    relayList: { seen: true, declared: 2, writeRelays: [] },
    scoreListSeen: false, rankService: null,
  });
  assert.equal(unusable.state, "no-usable-relays");
  assert.equal(unusable.tone, "blocked", "same severity, and the same next step");
  assert.equal(statusOf(unusable, "relayList"), "broken");
  assert.equal(unusable.chain[0].detail.declared, 2, "the chain can say how many it could not use");
  ok("a relay list we cannot dial is not the same as no relay list");
}

// ---- each link, broken on its own -----------------------------------------
{
  assert.equal(assess({ ...healthy(), scoreListSeen: false }).state, "no-score-list");
  // A 10040 that names a followers service and no rank service: present, and
  // still unable to rank. Broken, not absent — the reader's next step differs.
  const noRank = assess({ ...healthy(), rankService: null });
  assert.equal(noRank.state, "no-rank-service");
  assert.equal(statusOf(noRank, "scoreList"), "broken");
  ok("a list with no rank dimension is a broken link, not a missing one");
}

{
  // Zero here IS a claim: this relay answered, and holds none of the service's
  // cards. Ranked search returns nothing, so it is blocked whatever the
  // upstream says — including when the upstream declined to be counted.
  const cold = assess({ ...healthy(), scores: { here: 0, there: 145968 } });
  assert.equal(cold.state, "no-scores-yet");
  assert.equal(cold.tone, "blocked");
  assert.equal(assess({ ...healthy(), scores: { here: 0, there: REFUSED } }).state, "no-scores-yet");
  ok("no scores here is blocked, not `0%` of a working import");
}

// ---- the percentage, and when there isn't one -----------------------------
{
  const v = assess({ ...healthy(), scores: { here: 62847, there: 145968 } });
  assert.equal(v.state, "importing");
  assert.equal(v.tone, "partial");
  assert.equal(Math.round(v.percent * 100), 43);
  assert.equal(statusOf(v, "scores"), "partial");
  // The end-to-end link cannot claim more than the scores under it.
  assert.equal(statusOf(v, "ranked"), "partial");
  ok("a short import reports here/there as a fraction");
}

{
  // The last stretch of an import is not worth a warning: what is still to
  // come is the tail of the provider's own ranking, and "Importing your
  // provider's scores — 99%" nags somebody whose search is, for any result
  // they will look at, already complete.
  const nearly = assess({ ...healthy(), scores: { here: 144508, there: 145968 } });
  assert.equal(nearly.state, "ready");
  assert.equal(worthShowing(nearly), false, "99% is not a warning");
  assert.equal(statusOf(nearly, "scores"), "ok");
  // The bar is the ROUNDED percentage, the one the words print — so no panel
  // ever appears headlined with a number the reader was told not to worry
  // about. 89.6% rounds to 90 and is silent; 89.4% rounds to 89 and speaks.
  assert.equal(assess({ ...healthy(), scores: { here: 89600, there: 100000 } }).state, "ready");
  const short = assess({ ...healthy(), scores: { here: 89400, there: 100000 } });
  assert.equal(short.state, "importing");
  assert.equal(Math.round(short.percent * 100), 89);
  ok("an import within a rounded 90% of done says nothing at all");
}

{
  // Both non-answers, and both mean the same thing to the panel: no
  // denominator. The sentinels are OBJECTS, so they are truthy — the trap
  // observer_stats.html documents, and the reason nothing compares them
  // directly.
  for (const there of [REFUSED, TIMED_OUT, null]) {
    const v = assess({ ...healthy(), scores: { here: 3197, there } });
    assert.equal(v.state, "importing", `there=${JSON.stringify(there)}`);
    assert.equal(v.percent, null, "a non-answer must never become a percentage");
  }
  assert.equal(fraction(3197, REFUSED), null);
  assert.equal(fraction(3197, 0), null, "no denominator is a denominator of zero either");
  assert.equal(counted(REFUSED), false);
  assert.equal(counted(TIMED_OUT), false);
  assert.equal(counted(0), true, "zero is a count; that is the whole distinction");
  ok("silence never becomes a number");
}

{
  // We can hold MORE than an upstream serves — it deleted, we did not — and
  // 118% reads as a bug rather than as good news.
  assert.equal(fraction(200, 100), 1);
  const v = assess({ ...healthy(), scores: { here: 200000, there: 145968 } });
  assert.equal(v.state, "ready");
  ok("holding more than the upstream serves is parity, not 137%");
}

// ---- the end-to-end probe sees what the links cannot ----------------------
{
  // Cards here, and the lens still ranks nothing: the trust projection is per
  // service and is derived when the relay starts, so a service new to it ranks
  // nothing until then. No link check can see this.
  const v = assess({ ...healthy(), probe: { authed: 0, anon: 1 } });
  assert.equal(v.state, "projection-pending");
  assert.equal(statusOf(v, "ranked"), "broken");
  // ...but an empty answer means nothing without a control. On a relay with an
  // empty index BOTH sockets come back with nothing, and that is the store
  // having nothing to say rather than a broken lens.
  assert.equal(assess({ ...healthy(), probe: { authed: 0, anon: 0 } }).state, "ready");
  ok("an empty ranked read is only a verdict against an anonymous control");
}

// ---- your own posts hang off the chain, not in it -------------------------
{
  const v = assess({ ...healthy(), posts: { here: 1204, there: 1880, relay: "wss://relay.damus.io" } });
  assert.equal(v.state, "posts-behind");
  assert.equal(v.tone, "working", "ranking is fine; this is not a blocked state");
  assert.equal(Math.round(v.percent * 100), 64);
  // It is never a LINK: nothing above it depends on it, and putting it in the
  // chain would tell a reader whose lens is healthy that their search is broken.
  assert.equal(statusOf(v, "posts"), "aside");
  ok("posts behind is a working state, reported beside the chain");
}

// ---- and both sides of that count ask the same question -------------------
//
// The verdict above cannot see this one: `assess` is handed two numbers and
// has no way to know they were taken over different kinds. They were, for
// months. `postCounts` sent `{authors: [me]}` to both sides with no kind bound
// on either, and OUR side is narrowed by the router anyway — the mirror holds
// what router.conf pulls down — so a mirror missing nothing it had ever been
// asked for measured 31,118 here of 89,485 on the author's own relay and drew
// "35% mirroring", a percentage that could never reach 100. Asserted against
// the SOURCE, the way chainHtml is above: readiness.js reads the document at
// import and cannot be loaded here.
{
  const src = readFileSync(
    new URL("../../web/src/main/resources/web/readiness.js", import.meta.url), "utf8"
  );
  const fn = src.slice(src.indexOf("async function postCounts"), src.indexOf("async function askRemote"));
  assert.ok(fn.includes("scopedTo"), "postCounts has moved — this assertion no longer reads it");

  // One filter, built once, from the scope. Two filters that merely look alike
  // is exactly what this was.
  assert.match(fn, /const filter = scopedTo\(\{ authors: \[me\] \}, scope\)/);
  assert.equal((fn.match(/count\(filter\)/g) || []).length, 2, "both counts must be the one filter");
  assert.equal((fn.match(/req\(newest\)/g) || []).length, 2, "…and both newest reads too");
  assert.equal(
    (fn.match(/authors: \[me\]/g) || []).length, 1,
    "a second author filter spelled out by hand is a second question",
  );

  // And the caller never asks at all without a scope — `scopedTo` throws on
  // one, so a missing guard here is a pass that ends in an unhandled reject
  // rather than in the old number, but the panel would go quiet either way.
  const run = src.slice(src.indexOf("async function run("), src.indexOf("// ---- the asks"));
  assert.match(run, /const scope = await readMirrorScope\(\);/);
  assert.match(run, /if \(scope\) facts\.posts = await postCounts\(/);

  // Every await in a pass is a window somebody can sign out across, and the
  // guard belongs after each one — not only before the paint. Reading the
  // scope added a window where four asks, two to a stranger's relay, went out
  // for an account that had already left.
  const afterScope = run.slice(run.indexOf("const scope = await readMirrorScope();"));
  assert.match(
    afterScope.slice(0, afterScope.indexOf("postCounts(")),
    /if \(gen !== generation\) return;/,
    "the scope fetch is a window with no generation guard behind it",
  );
  ok("the posts count is scoped to the mirrored kinds on both sides, or not taken");
}

{
  // The fallback for a relay that will not count: whose newest event is newer.
  // "We have your posts up to 2 July" is an answer where a missing bar is not.
  const v = assess({
    ...healthy(),
    posts: { here: REFUSED, there: REFUSED, relay: "wss://relay.damus.io", newestHere: 1000, newestThere: 2000 },
  });
  assert.equal(v.state, "posts-behind");
  assert.equal(v.percent, null, "no count, so no bar — the dates carry it instead");
  // Same dates the other way round is not behind, and neither is one we cannot
  // compare at all.
  assert.equal(assess({ ...healthy(), posts: { here: REFUSED, there: REFUSED, newestHere: 2000, newestThere: 1000 } }).state, "ready");
  assert.equal(assess({ ...healthy(), posts: { here: REFUSED, there: REFUSED, newestHere: null, newestThere: null } }).state, "ready");
  ok("with no counts, the newest event on each side decides — or nothing does");
}

// ---- the relay url the reader types --------------------------------------
//
// The rules came out of observer_stats.html, where these urls arrive from
// strangers' tags and dropping a bad one silently is right. Typed into a field
// they need the opposite: a reason. Both halves are asserted, because the
// panel's one path out of its worst state is this field.
{
  globalThis.location = { protocol: "https:" };
  const { normalizeRelay, whyNotDialable } = await import(
    "../../web/src/main/resources/web/shared/relayurl.js"
  );

  assert.equal(normalizeRelay("wss://relay.damus.io"), "wss://relay.damus.io");
  assert.equal(normalizeRelay("relay.damus.io"), "wss://relay.damus.io", "a bare host is a relay");
  // The duplicate that made this page dial one host twice and print it twice.
  assert.equal(normalizeRelay("wss://nip85.brainstorm.world/"), normalizeRelay("wss://nip85.brainstorm.world"));

  // The regression. Prepending `wss://` to a string that ALREADY has a scheme
  // produces a url that parses and means nothing — `wss://https//relay.damus.io`,
  // whose host is "https". It was accepted, dialled, and printed back to the
  // reader as "Nothing of yours on https//relay.damus.io"; the same flaw made
  // the loopback rule below blind, because the hostname it checks was "http".
  assert.equal(normalizeRelay("https://relay.damus.io"), null);
  assert.equal(normalizeRelay("http://localhost:7778"), null, "the url in the corpus this rule was written for");
  assert.match(whyNotDialable("https://relay.damus.io"), /wss:\/\/relay\.damus\.io/, "and the reason offers the fix");

  // Somebody else's machine is not ours to dial on their behalf.
  assert.equal(normalizeRelay("wss://localhost:7777"), null);
  assert.equal(normalizeRelay("wss://192.168.1.4"), null);
  assert.equal(normalizeRelay("wss://172.20.0.9"), null);
  assert.equal(normalizeRelay("wss://172.15.0.9"), "wss://172.15.0.9", "…and 172.15 is not private");

  // An https page cannot open a ws:// socket at all — the browser refuses
  // before a packet moves, so "the relay didn't answer" would be a lie.
  assert.equal(normalizeRelay("ws://relay.damus.io"), null);
  assert.match(whyNotDialable("ws://relay.damus.io"), /https/, "the reason names the browser, not the relay");
  globalThis.location = { protocol: "http:" };
  assert.equal(normalizeRelay("ws://relay.damus.io"), "ws://relay.damus.io", "…but on http it is fine");

  // Every refusal has words. An empty field is a prompt, not an error.
  for (const bad of ["", "  ", "not a relay", "https://x.com", "ws://localhost"]) {
    assert.equal(typeof whyNotDialable(bad), "string", `no reason given for ${JSON.stringify(bad)}`);
    assert(whyNotDialable(bad).length > 10, `unhelpful reason for ${JSON.stringify(bad)}`);
  }
  assert.equal(whyNotDialable("wss://relay.damus.io"), null, "a good url has nothing to explain");
  ok("a typed relay url is normalised, or refused with a reason");
}

// ---- the healthy reader hears nothing -------------------------------------
{
  const v = assess(healthy());
  assert.equal(v.state, "ready");
  assert.equal(worthShowing(v), false);
  ok("a complete chain shows no panel at all");
}

console.log("readiness: ok");
