// What a signed kind-30166 record CLAIMS — the tag semantics behind the
// monitor-verdicts panel.
//
// This is the half worth testing because it is the half that can be wrong
// silently. Every failure here draws a confident, well-formatted, wrong answer:
// a cleared verdict shown as a fold onto itself, a month-old verdict shown as
// current, a stability answer invisible because the url was never folded. Each
// of those is a bug the Kotlin reader made first, and each is asserted below in
// the direction it failed.
import assert from "node:assert/strict";
import {
  MONITOR_KIND, hostOf, sameUrl, readRecord, isCurrent, groupByHost, summarise, walkRecords, TTL_SECONDS,
  FOLD_EPOCH, CONSISTENCY_EPOCH,
} from "../../relay/src/main/resources/web/shared/verdicts.js";

const ok = (name) => console.log(`  ✓ ${name}`);

const NOW = 1786630000;
/** A record in the shape `RelayAliasRecord.edit` signs. */
const rec = (url, tags, at = NOW) => ({
  id: url + JSON.stringify(tags),
  kind: MONITOR_KIND,
  pubkey: "4391".padEnd(64, "0"),
  created_at: at,
  tags: [["d", url], ...tags],
});
// The epoch goes in every fixture that is meant to READ as a verdict, because
// that is what the router writes — a fixture without one is a record from an
// older build, which is a case of its own and asserted as such below.
const sameAs = (target, evidence, at, epoch = FOLD_EPOCH) => ["same-as", target, evidence, String(at), epoch];
const consistent = (v, evidence, at, epoch = CONSISTENCY_EPOCH) => ["self-consistent", v, evidence, String(at), epoch];

// ---- the host, which is what decides a GROUP -------------------------------
{
  assert.equal(hostOf("wss://nos.lol/"), "nos.lol");
  assert.equal(hostOf("wss://nos.lol/cipher-zulu"), "nos.lol");
  assert.equal(hostOf("wss://NOS.LOL:443/beacon"), "nos.lol", "port and case are not part of the host");
  assert.equal(hostOf("ws://nos.lol"), "nos.lol", "the scheme is not either — a host serving both is one host");
  // The colons inside an IPv6 literal are not the port separator. Grouping this
  // wrong would split one host into as many groups as it has urls, and the
  // panel would draw every one of them as unfoldable.
  assert.equal(hostOf("wss://[2001:db8::1]:443/alpha"), "[2001:db8::1]");
  assert.equal(hostOf("wss://[2001:db8::1]/alpha"), "[2001:db8::1]");
  ok("the host is the hostname alone — no port, no path, no scheme");
}

// ---- fold vs cleared, which are told apart AFTER normalising ---------------
{
  // The cleared form points at the record's OWN url. Compared by string,
  // `wss://nos.lol` vs `wss://nos.lol/` reads as a FOLD of a url onto itself —
  // which is a duplicate the panel would report and the router does not have.
  const r = readRecord(rec("wss://nos.lol/", [sameAs("wss://nos.lol", "500 newest events, best 4 shared", NOW)]));
  assert.equal(r.cleared, true, "a trailing slash must not turn a cleared verdict into a fold");
  assert.equal(r.fold, null);
  assert.equal(sameUrl("wss://nos.lol", "wss://nos.lol/"), true);
  assert.equal(sameUrl("wss://NOS.LOL/", "wss://nos.lol"), true, "the authority is case-insensitive");
  assert.equal(sameUrl("wss://nos.lol/alpha", "wss://nos.lol/"), false, "a path is not nothing");
  assert.equal(sameUrl("wss://nos.lol/Inbox", "wss://nos.lol/inbox"), false, "…and a path is opaque, so its case is kept");
  ok("cleared and folded are told apart after normalising, never by string");
}

{
  const r = readRecord(rec("wss://nos.lol/cipher-zulu", [sameAs("wss://nos.lol/", "500 newest events, 498 shared with wss://nos.lol/", NOW)]));
  assert.equal(r.fold, "wss://nos.lol/");
  assert.equal(r.cleared, false);
  assert.equal(r.foldEvidence, "500 newest events, 498 shared with wss://nos.lol/",
    "the evidence is the point of the panel — it is what says WHY, and it is the monitor's own sentence");
  ok("a fold names the url it folded onto and carries its evidence");
}

// ---- the two verdicts are read INDEPENDENTLY -------------------------------
{
  // A url may carry a fold, a stability answer, both or neither. An early
  // return for a missing `same-as` hid every stability verdict on a url that
  // was never folded — the exact bug the Kotlin reader had.
  const r = readRecord(rec("wss://flaky.example/", [consistent("false", "203 + 179 events at a 7d anchor, 128 shared -> 0.715", NOW)]));
  assert.equal(r.stable, false);
  assert.equal(r.fold, null);
  assert.equal(r.cleared, false, "no same-as tag is NOT a cleared verdict — it is no verdict at all");
  const both = readRecord(rec("wss://x.example/a", [sameAs("wss://x.example/", "e", NOW), consistent("true", "s", NOW)]));
  assert.equal(both.fold, "wss://x.example/");
  assert.equal(both.stable, true);
  ok("a stability answer is read whether or not the url was ever folded");
}

{
  // An answer this reader does not recognise is not a verdict. Guessing
  // "unstable" would draw a relay as refused from the fan-out on a tag we
  // cannot read.
  assert.equal(readRecord(rec("wss://x.example/", [consistent("maybe", "", NOW)])).stable, null);
  ok("an unrecognised stability value is ignored, not guessed at");
}

// ---- the verdict's clock, which is not the record's ------------------------
{
  // Kind 30166 is addressable and shared: quartz's monitor rewrites the record
  // every time this client connects to the relay, carrying our tags forward. So
  // `created_at` tracks the last time we TALKED to it, not the last time we
  // MEASURED it — and reading the record's clock made half these verdicts
  // immortal on the Kotlin side. The tag carries its own.
  const stale = NOW - TTL_SECONDS - 1;
  const r = readRecord(rec("wss://busy.example/a", [sameAs("wss://busy.example/", "e", stale)], NOW));
  assert.equal(r.foldMeasuredAt, stale, "the record was rewritten a second ago; the measurement was not");
  assert.equal(isCurrent(r.foldMeasuredAt, NOW, r.foldEpoch, FOLD_EPOCH), false);
  assert.equal(isCurrent(NOW - TTL_SECONDS + 60, NOW, FOLD_EPOCH, FOLD_EPOCH), true);
  ok("a verdict ages on when it was MEASURED, not on the record it rides on");
}

{
  // A record from before the measured-at element existed. It must NOT fall back
  // to the event's clock, which is what this reader used to do: that clock is
  // rewritten every time the router connects, so the fallback dated every such
  // verdict as minutes old and drew it as current forever — on precisely the
  // relays still being dialled. No stamp is now no verdict, which is also what
  // the router does with it.
  const r = readRecord(rec("wss://old.example/a", [["same-as", "wss://old.example/", "e"]], NOW - 10));
  assert.equal(r.foldMeasuredAt, null, "an unstamped verdict must not borrow the record's clock");
  assert.equal(isCurrent(r.foldMeasuredAt, NOW, r.foldEpoch, FOLD_EPOCH), false);
  assert.equal(r.foldEvidence, "e", "…and it is still READ — the panel shows what the record says, expired or not");
  ok("a tag with no measured-at is stale, not dated from the record");
}

// ---- the rules a verdict was measured under --------------------------------
{
  // The forcing lever. A verdict measured under superseded rules is not a stale
  // reading of today's rule, it is a reading of a different one — so the router
  // discards it and re-measures, and this panel has to agree or it would draw a
  // url as folded while the fan-out dials it.
  const old = readRecord(rec("wss://x.example/a", [sameAs("wss://x.example/", "e", NOW, "1")]));
  assert.equal(old.fold, "wss://x.example/", "the record still SAYS this — being superseded is not being unreadable");
  assert.equal(old.foldEpoch, "1");
  assert.equal(isCurrent(old.foldMeasuredAt, NOW, old.foldEpoch, FOLD_EPOCH), false,
    "a verdict measured seconds ago under old rules is inside the TTL and still not acted on");

  // …and the host it belongs to counts it as expired rather than folded, which
  // is the number an operator reads to know a re-measure is outstanding.
  const [group] = groupByHost([rec("wss://x.example/a", [sameAs("wss://x.example/", "e", NOW, "1")])], NOW);
  assert.equal(group.folded, 0);
  assert.equal(group.expired, 1);
  ok("a verdict from superseded rules reads as expired, whatever its age");
}

// ---- grouping, which is the whole point ------------------------------------
{
  const events = [
    rec("wss://h.example/", [sameAs("wss://h.example/", "500 newest events, best 6 shared with 2 compared endpoint(s) on this host", NOW)]),
    rec("wss://h.example/alpha", [sameAs("wss://h.example/", "500 newest events, 500 shared", NOW)]),
    rec("wss://h.example/beta", [sameAs("wss://h.example/", "500 newest events, 499 shared", NOW)]),
    // Same host, verdict aged out: the router would re-probe this today, so the
    // panel must not draw it as folded.
    rec("wss://h.example/gamma", [sameAs("wss://h.example/", "e", NOW - TTL_SECONDS - 1)]),
    // A record with no verdict tag at all — quartz's passive monitor writing
    // reachability. Counted as silent, never as a verdict.
    rec("wss://h.example/delta", [["n", "clearnet"], ["rtt-open", "120"]]),
    rec("wss://other.example/", [consistent("true", "s", NOW)]),
  ];
  const [first, second] = groupByHost(events, NOW);
  assert.equal(first.host, "h.example", "the host with the most to explain comes first");
  assert.equal(first.urls.length, 5);
  assert.equal(first.folded, 2, "only verdicts the router would still act on");
  assert.equal(first.cleared, 1);
  assert.equal(first.expired, 1);
  assert.equal(first.survivors, 3, "the cleared url, the expired one and the silent one are all still dialled");
  assert.equal(first.urls[0].url, "wss://h.example/", "shortest first puts the pathless url where it belongs");
  assert.equal(second.host, "other.example");

  const sum = summarise([first, second], NOW);
  assert.equal(sum.hosts, 2);
  assert.equal(sum.urls, 6);
  assert.equal(sum.folded, 2);
  assert.equal(sum.expired, 1);
  assert.equal(sum.silent, 1, "a record with no verdict tag separates 'the fold is not running' from 'nothing is'");
  assert.equal(sum.stable, 1);
  ok("urls group by host, and an expired verdict is not a fold");
}

{
  // 30166 is addressable, so a paged read can serve two versions of one
  // address. The newest wins, or a stale copy draws over the current verdict.
  const events = [
    rec("wss://h.example/a", [sameAs("wss://h.example/", "old", NOW - 500)], NOW - 500),
    rec("wss://h.example/a", [sameAs("wss://h.example/", "new", NOW)], NOW),
  ];
  const [group] = groupByHost(events, NOW);
  const real = group.urls.filter((u) => !u.synthetic);
  assert.equal(real.length, 1, "one address is one row");
  assert.equal(real[0].foldEvidence, "new");
  ok("two versions of one address collapse to the newest");
}

// ---- the survivor, which usually has no record of its own ------------------
{
  // `RelayAliases.learn` clears — publishes a verdict about — only a leader
  // that nothing folded onto. So the very url a group collapses TO is the one
  // url with no `same-as` to find, and a group read purely from records drew
  // "23 urls · 0 dialled" for a host that is dialled exactly once. Measured on
  // the live store: `articles.layer3.news`, 23 folds, survivor absent.
  const events = [
    rec("wss://a.example/alpha", [sameAs("wss://a.example/lantern", "e", NOW)]),
    rec("wss://a.example/beta", [sameAs("wss://a.example/lantern", "e", NOW)]),
  ];
  const [group] = groupByHost(events, NOW);
  assert.equal(group.urls.length, 3, "the url the folds point at is on the list");
  assert.equal(group.inferred, 1);
  assert.equal(group.survivors, 1, "…and it is what makes the host's dialled count 1 rather than 0");
  const survivor = group.urls.find((u) => u.synthetic);
  assert.equal(survivor.url, "wss://a.example/lantern");
  assert.equal(group.urls[0], survivor, "the survivor leads its own group");
  // Inferred, not stated. It carries no verdict BY CONSTRUCTION, so counting it
  // as silent would inflate the one number meaning "nothing has looked at this".
  const sum = summarise([group], NOW);
  assert.equal(sum.silent, 0);
  assert.equal(sum.inferred, 1);
  ok("a survivor with no record of its own is inferred from the folds that point at it");
}

{
  // Only when the target really is on this host. A hand-edited or malformed
  // record can point anywhere, and inventing a row for it would put one host's
  // url inside another host's group.
  const [group] = groupByHost([rec("wss://a.example/x", [sameAs("wss://elsewhere.example/", "e", NOW)])], NOW);
  assert.equal(group.urls.length, 1, "a fold pointing off-host synthesises nothing");
  assert.equal(group.inferred, undefined);
  // An EXPIRED fold points at nothing the router acts on, so its target is not
  // a survivor to draw either.
  const stale = groupByHost([rec("wss://b.example/x", [sameAs("wss://b.example/", "e", NOW - TTL_SECONDS - 1)])], NOW);
  assert.equal(stale[0].urls.length, 1);
  ok("nothing is synthesised for an off-host target or an expired fold");
}

{
  assert.deepEqual(groupByHost([], NOW), []);
  assert.deepEqual(summarise([], NOW), { hosts: 0, urls: 0, folded: 0, cleared: 0, expired: 0, silent: 0, stable: 0, unstable: 0, inferred: 0 });
  // A record with no `d` tag is not addressed at anything and cannot be drawn.
  assert.equal(readRecord({ tags: [["same-as", "wss://x.example/"]], created_at: NOW }), null);
  ok("an empty store and a record with no d tag are both handled, not thrown on");
}

// ---- the rest of the record, which several writers share -------------------
{
  // A replaceable event has ONE address and more than one writer: quartz's
  // passive monitor writes `n` / `rtt-*` / `R` onto the same record our fold
  // writes `same-as` onto, and `RelayAliasRecord.edit` has to carry forward
  // every tag it does not own. A writer that rebuilds silently deletes the
  // others — measured once as `[d, n, rtt-open]` becoming `[d, same-as]` — and
  // the result is still a valid signed record that simply says less. The panel
  // draws them so that regression is visible in production, not just in a unit
  // test.
  const r = readRecord({
    created_at: NOW,
    pubkey: "a".repeat(64),
    content: '{"name":"x"}',
    tags: [
      ["d", "wss://gated.example/"],
      ["n", "clearnet"],
      ["R", "auth"],
      ["R", "payment"],
      ["rtt-open", "875"],
      ["rtt-read", "897"],
      ["s", "git+https://github.com/hoytech/strfry.git"],
      ["something-new", "42"],
      ["same-as", "wss://gated.example/", "e", String(NOW)],
    ],
  });
  assert.equal(r.network, "clearnet");
  assert.deepEqual(r.requirements, ["auth", "payment"], "a relay can be both auth-gated and paid");
  assert.equal(r.rttOpen, "875");
  assert.equal(r.rttRead, "897");
  assert.equal(r.software, "git+https://github.com/hoytech/strfry.git");
  assert.equal(r.hasDoc, true);
  assert.equal(r.cleared, true, "reading the metadata does not disturb the verdict");
  // Counted, never dropped: a record carrying a tag this reader has not heard
  // of must look different from one carrying nothing.
  assert.equal(r.extra, 1);
  ok("the tags other writers put on the record are read, and unknown ones are counted");
}

{
  // The clobbered shape: our verdict and nothing else. Nothing here throws, and
  // every metadata field is simply absent — which is what makes the difference
  // visible on the page.
  const r = readRecord(rec("wss://lonely.example/a", [sameAs("wss://lonely.example/", "e", NOW)]));
  assert.equal(r.network, undefined);
  assert.deepEqual(r.requirements, []);
  assert.equal(r.extra, 0);
  assert.equal(r.hasDoc, false);
  ok("a record carrying only our own tag reads as exactly that");
}

{
  // A synthesised survivor must have the SAME SHAPE as a read record, every
  // field of it. Omitting the collection fields is one `for…of` away from
  // throwing inside the renderer — which is exactly what happened: the panel
  // drew thousands of rows correctly, died on the first host whose survivor was
  // inferred, and left its own filter hidden with no error on screen.
  const [group] = groupByHost([rec("wss://s.example/a", [sameAs("wss://s.example/b", "e", NOW)])], NOW);
  const survivor = group.urls.find((u) => u.synthetic);
  const real = readRecord(rec("wss://s.example/a", [sameAs("wss://s.example/b", "e", NOW)]));
  for (const key of Object.keys(real)) {
    assert.ok(key in survivor, `a synthesised survivor is missing "${key}", which the renderer reads on every row`);
  }
  assert.deepEqual(survivor.requirements, []);
  ok("a synthesised survivor carries every field a read record does");
}

// ---- the walk, and the run of records it used to step over -----------------
{
  // A RELAY IN A BOTTLE. Holds `total` records, hands back the newest `limit`
  // at or below `until`, and stamps `run` of them with one identical
  // `created_at` — which is what quartz's monitor does when it flushes a batch.
  const relay = (total, run, at = 1000) => {
    const all = [];
    for (let i = 0; i < total; i++) all.push({ id: "e" + i, created_at: i < run ? at : at - 1 - i });
    all.sort((a, b) => b.created_at - a.created_at || a.id.localeCompare(b.id));
    let asks = 0;
    return {
      asks: () => asks,
      ask: async (limit, until) => {
        asks++;
        return all.filter((e) => until == null || e.created_at <= until).slice(0, limit);
      },
    };
  };

  // 900 records of which 600 share one second, read 500 at a time. A fixed page
  // can never span that run: it returns the same 500, the cursor cannot move,
  // and stepping below the boundary skips the other 100. Measured on the live
  // store as 4,595 records read of 5,296 — with the read reported COMPLETE.
  const stuck = relay(900, 600);
  const fixed = await walkRecords({ ask: stuck.ask, pageSize: 500, maxPage: 500 });
  assert.ok(fixed.events.length < 900, "the fixture does not reproduce the run this exists for");
  assert.equal(fixed.complete, false, "a walk that could not get past a run must NOT call itself complete");

  // The same relay, allowed to grow its page: the run fits, the cursor moves,
  // and every record is read.
  const grown = relay(900, 600);
  const full = await walkRecords({ ask: grown.ask, pageSize: 500, maxPage: 5000 });
  assert.equal(full.events.length, 900, "growing the page past the run is what recovers the missing records");
  assert.equal(full.complete, true);
  assert.ok(full.grew > 0, "it should have had to grow at least once");
  ok("a page that is entirely one second grows until it spans two, instead of stepping over the rest");
}

{
  // The ordinary case pays nothing: no run longer than a page, no growth.
  const all = [];
  for (let i = 0; i < 1200; i++) all.push({ id: "x" + i, created_at: 9000 - i });
  const ask = async (limit, until) => all.filter((e) => until == null || e.created_at <= until).slice(0, limit);
  const walk = await walkRecords({ ask, pageSize: 500, maxPage: 5000 });
  assert.equal(walk.events.length, 1200);
  assert.equal(walk.complete, true);
  assert.equal(walk.grew, 0, "a store with no same-second run must never grow its ask");
  ok("a store without same-second runs is walked at the base page size");
}

{
  // Nothing at all, and a relay that answers every ask with the same page.
  assert.deepEqual((await walkRecords({ ask: async () => [] })).events, []);
  assert.equal((await walkRecords({ ask: async () => [] })).complete, true);
  const same = [{ id: "a", created_at: 5 }, { id: "b", created_at: 4 }];
  const spin = await walkRecords({ ask: async () => same, pageSize: 500, maxPage: 500, maxPages: 50 });
  assert.equal(spin.events.length, 2, "a relay that will not walk backwards is given up on, not spun on");
  assert.ok(spin.pages < 50);
  ok("an empty store completes, and a relay that repeats itself is abandoned");
}
