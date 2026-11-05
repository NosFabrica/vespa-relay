// The readiness verdict: which link of the trust chain a signed-in reader is missing. The first
// unmet link wins; an unfinished read is not an absence, a non-answer is not a zero, and a
// missing denominator is not a percentage.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  assess, fraction, counted, worthShowing, REFUSED, TIMED_OUT,
} from "../../main/resources/web/shared/readiness.js";

const ok = (name) => console.log(`  ✓ ${name}`);

// A reader with everything; each case below breaks it one way.
const healthy = () => ({
  relayList: { seen: true, declared: 2, writeRelays: ["wss://relay.damus.io", "wss://nos.lol"] },
  scoreListSeen: true,
  rankService: { service: "abc", relay: "wss://nip85.nosfabrica.com" },
  scores: { here: 145968, there: 145968 },
  probe: { authed: 1, anon: 1 },
  posts: { followed: true, relays: [{ relay: "wss://relay.damus.io/", syncStatus: "complete" }] },
});

/** The sync's answer, one row per (relay, status). */
const posts = (...rows) => ({ followed: true, relays: rows.map(([relay, syncStatus]) => ({ relay, syncStatus })) });

const statusOf = (v, key) => v.chain.find((l) => l.key === key)?.status;

{
  assert.equal(assess({}).state, "checking");
  assert.equal(assess({ relayList: { writeRelays: ["wss://a.com"] } }).state, "checking");
  const half = assess({ ...healthy(), scores: { here: null, there: null } });
  assert.equal(half.state, "checking");
  assert.equal(worthShowing(half), false, "a spinner for a check about to say ready is still a nag");
  ok("an unanswered stage is `checking`, and `checking` is never shown");
}

{
  const v = assess({ relayList: { seen: false, writeRelays: [] }, scoreListSeen: false, rankService: null });
  assert.equal(v.state, "no-relay-list");
  assert.equal(statusOf(v, "relayList"), "broken");
  assert.equal(statusOf(v, "scoreList"), "waiting");
  assert.equal(statusOf(v, "scores"), "waiting");
  assert.equal(statusOf(v, "ranked"), "waiting");
  assert.equal(v.chain.filter((l) => l.status === "broken").length, 1, "exactly one link is ever broken");
  ok("the first unmet link is the verdict; every link below it waits");
}

// A waiting link must carry no detail, and chainHtml must not enter its detail switch for one.
{
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

  // Asserted against the source: readiness.js reads the document at import and cannot be loaded here.
  const src = readFileSync(
    new URL("../../main/resources/web/readiness.js", import.meta.url), "utf8"
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
  // A list naming only `ws://` relays loses every entry on an https page.
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

{
  assert.equal(assess({ ...healthy(), scoreListSeen: false }).state, "no-score-list");
  const noRank = assess({ ...healthy(), rankService: null });
  assert.equal(noRank.state, "no-rank-service");
  assert.equal(statusOf(noRank, "scoreList"), "broken");
  ok("a list with no rank dimension is a broken link, not a missing one");
}

{
  // Zero here is an answer: this relay holds none of the service's cards.
  const cold = assess({ ...healthy(), scores: { here: 0, there: 145968 } });
  assert.equal(cold.state, "no-scores-yet");
  assert.equal(cold.tone, "blocked");
  assert.equal(assess({ ...healthy(), scores: { here: 0, there: REFUSED } }).state, "no-scores-yet");
  ok("no scores here is blocked, not `0%` of a working import");
}

{
  const v = assess({ ...healthy(), scores: { here: 62847, there: 145968 } });
  assert.equal(v.state, "importing");
  assert.equal(v.tone, "partial");
  assert.equal(Math.round(v.percent * 100), 43);
  assert.equal(statusOf(v, "scores"), "partial");
  assert.equal(statusOf(v, "ranked"), "partial");
  ok("a short import reports here/there as a fraction");
}

{
  const nearly = assess({ ...healthy(), scores: { here: 144508, there: 145968 } });
  assert.equal(nearly.state, "ready");
  assert.equal(worthShowing(nearly), false, "99% is not a warning");
  assert.equal(statusOf(nearly, "scores"), "ok");
  // The threshold is the rounded percentage, the one the words print.
  assert.equal(assess({ ...healthy(), scores: { here: 89600, there: 100000 } }).state, "ready");
  const short = assess({ ...healthy(), scores: { here: 89400, there: 100000 } });
  assert.equal(short.state, "importing");
  assert.equal(Math.round(short.percent * 100), 89);
  ok("an import within a rounded 90% of done says nothing at all");
}

{
  // The sentinels are objects, so they are truthy; nothing may compare them as numbers.
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
  for (const here of [REFUSED, TIMED_OUT]) {
    const v = assess({ ...healthy(), scores: { here, there: 145968 } });
    assert.equal(v.state, "checking", `here=${JSON.stringify(here)}`);
    assert.notEqual(v.tone, "blocked", "a non-answer must never raise the blocked panel");
  }
  ok("an unanswered count is not an answered zero");
}

{
  assert.equal(fraction(200, 100), 1);
  const v = assess({ ...healthy(), scores: { here: 200000, there: 145968 } });
  assert.equal(v.state, "ready");
  ok("holding more than the upstream serves is parity, not 137%");
}

{
  // The trust projection is derived per service when the relay starts.
  const v = assess({ ...healthy(), probe: { authed: 0, anon: 1 } });
  assert.equal(v.state, "projection-pending");
  assert.equal(statusOf(v, "ranked"), "broken");
  assert.equal(assess({ ...healthy(), probe: { authed: 0, anon: 0 } }).state, "ready");
  ok("an empty ranked read is only a verdict against an anonymous control");
}

{
  const v = assess({ ...healthy(), posts: posts(["wss://relay.damus.io/", "complete"], ["wss://nos.lol/", "paging"]) });
  assert.equal(v.state, "posts-behind");
  assert.equal(v.tone, "working", "ranking is fine; this is not a blocked state");
  assert.equal(v.percent, 0.5, "one relay of two read to the start");
  assert.equal(statusOf(v, "posts"), "aside");
  assert.deepEqual(v.progress.relays.filter((r) => !r.done).map((r) => r.relay), ["wss://nos.lol/"]);
  ok("posts behind is a working state, reported beside the chain, counted in relays the sync has finished");
}

{
  assert.equal(assess({ ...healthy(), posts: posts(["wss://relay.damus.io/", "complete"], ["wss://nos.lol/", "complete"]) }).state, "ready");
  assert.equal(assess({ ...healthy(), posts: null }).state, "ready", "no sync to ask is not a verdict");
  const v = assess({ ...healthy(), posts: { followed: false, relays: [], unread: ["wss://nos.lol/"] } });
  assert.equal(v.state, "posts-not-followed");
  assert.deepEqual(v.unread, ["wss://nos.lol/"], "the relays nothing here reads ride with the verdict, for the words");
  assert.equal(v.tone, "working");
  assert.equal(statusOf(v, "posts"), "aside");
  ok("every relay finished is ready; a reader nobody follows is said, not counted");
}

// assess() cannot tell where `posts` came from, so the source is asserted: the sync, once, behind the guard.
{
  const src = readFileSync(
    new URL("../../main/resources/web/readiness.js", import.meta.url), "utf8"
  );
  assert.ok(!src.includes("postCounts"), "the page counts posts on two relays again");
  assert.ok(!src.includes("mirrors.js"), "the page reads the relay's mirror scope again");
  const run = src.slice(src.indexOf("async function run("), src.indexOf("// ---- the asks"));
  assert.match(run, /const coverage = await readCoverage\(me, \{ relays: facts\.relayList\.writeRelays \}\);/);
  const after = run.slice(run.indexOf("const coverage = await readCoverage("));
  assert.match(
    after.slice(0, after.indexOf("facts.posts = coverage")),
    /if \(gen !== generation\) return;/,
    "the sync's answer is a window with no generation guard behind it",
  );
  ok("the posts stage asks the sync for this reader, behind the sign-out guard");
}

{
  globalThis.location = { protocol: "https:" };
  const { normalizeRelay, whyNotDialable } = await import(
    "../../main/resources/web/shared/relayurl.js"
  );

  assert.equal(normalizeRelay("wss://relay.damus.io"), "wss://relay.damus.io");
  assert.equal(normalizeRelay("relay.damus.io"), "wss://relay.damus.io", "a bare host is a relay");
  assert.equal(normalizeRelay("wss://nip85.brainstorm.world/"), normalizeRelay("wss://nip85.brainstorm.world"));

  // Prepending `wss://` to a string that already has a scheme yields a url whose host is "https".
  assert.equal(normalizeRelay("https://relay.damus.io"), null);
  assert.equal(normalizeRelay("http://localhost:7778"), null, "the url in the corpus this rule was written for");
  assert.match(whyNotDialable("https://relay.damus.io"), /wss:\/\/relay\.damus\.io/, "and the reason offers the fix");

  assert.equal(normalizeRelay("wss://localhost:7777"), null);
  assert.equal(normalizeRelay("wss://192.168.1.4"), null);
  assert.equal(normalizeRelay("wss://172.20.0.9"), null);
  assert.equal(normalizeRelay("wss://172.15.0.9"), "wss://172.15.0.9", "…and 172.15 is not private");

  assert.equal(normalizeRelay("ws://relay.damus.io"), null);
  assert.match(whyNotDialable("ws://relay.damus.io"), /https/, "the reason names the browser, not the relay");
  globalThis.location = { protocol: "http:" };
  assert.equal(normalizeRelay("ws://relay.damus.io"), "ws://relay.damus.io", "…but on http it is fine");

  for (const bad of ["", "  ", "not a relay", "https://x.com", "ws://localhost"]) {
    assert.equal(typeof whyNotDialable(bad), "string", `no reason given for ${JSON.stringify(bad)}`);
    assert(whyNotDialable(bad).length > 10, `unhelpful reason for ${JSON.stringify(bad)}`);
  }
  assert.equal(whyNotDialable("wss://relay.damus.io"), null, "a good url has nothing to explain");
  ok("a typed relay url is normalised, or refused with a reason");
}

{
  const v = assess(healthy());
  assert.equal(v.state, "ready");
  assert.equal(worthShowing(v), false);
  ok("a complete chain shows no panel at all");
}

console.log("readiness: ok");
