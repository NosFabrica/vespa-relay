# Alias fold decisions

The history behind `monitor/.../RelayAliases.kt`, `AliasProbe.kt`,
`AliasFolding.kt` and `AliasMonitor.kt`, moved out of the source so the code
reads on its own. One paragraph per decision; `git log -L` on the function
finds the commit.

**The fold exists because relay lists mint urls without limit.** Measured on
this store, 7,333 discovered urls stood for 1,147 distinct `host:port`
endpoints, and weighting each by how many lists name it, the popular relays
were dialled 10.7x over. `HostStrikes` cannot help: it evicts an authority
that goes silent, and every one of these answers.

**Probing runs on its own clock, not inline in a sync cycle.** Folding inline
put a multi-minute probe pass between "discovery finished" and the first byte
on every cycle: 1:19 for a 225-url list in the Docker run, and a production
list is two orders of magnitude wider. `AliasFolding.applyVerdicts` reads
verdicts on the critical path; `measure` dials on `AliasMonitor`'s schedule.

**The candidate set is derived inside the pass, not submitted by streams.**
When streams pushed their worlds in, the first pass after a boot saw whichever
had finished discovering first: 34,997 urls waited six hours for a pass they
missed by three minutes. One `ALL_STREAMS` row rather than one per stream,
because grouping within a stream never folded a host whose urls were split
across two.

**Passes never overlap, sweep or fast lane.** `RelayVerdictRecord.edit` is a
read-modify-write with no compare-and-set on one addressable record per url,
so two passes writing one url at once silently drop a tag. The fast lane runs
the stability gate before fitness (it used to be fitness alone): a relay was
being graded `prime` and admitted to every roster before anything had asked
whether it answers the same question twice.

**A sweep with one url is not empty.** The "fewer than two" guard was written
when the monitor ran only the fold; once per-url passes joined, a router
discovering exactly one relay never graded it and every visit-mode roster was
permanently empty, issue 139's symptom by another route. The fold refuses a
world of one itself.

**The fingerprint is paged, from one shared anchor half an hour behind the
clock** (a minute, until the measurement under "The anchor is half an hour
back" below). Across 60 live hosts, `max_limit` was 500, 100, 1024, 2100, 10000,
nothing, or 0, and purplepag.es answers `{"limit": 1000}` with `CLOSED
blocked: limit too high`. Unanchored, `nos.lol` against `nos.lol/cipher-zulu`
scored 0.41 and escaped the fold on exactly the busy relay where duplication
costs most.

**The probe target is 500, not 1,000.** Re-measured over 35 hosts and 112
fold decisions, 500 agreed with 1,000 on 108, and all four disagreements were
`espelho.girino.org`, which cannot reproduce its own window. Median cost was
1.4s and 562 KB against 3.4s and 1,464 KB; 200 measured the same 520 KB
because the page is asked whole either way.

**The ask ladder is bare, then kind 1, then kind 39000.** 46 of 229 hosts in a
full-corpus sweep refused a bare filter with `CLOSED blocked: can't handle
empty filters`, taking 892 urls out of the fold, and every one retried
answered a kinds filter. khatru's groups mode refuses any unscoped query
(`must have 'h', 'e' or 'a' tag`); kind 39000 is the one window it serves,
verified on `groups.satsdisco.com` (55 ids), `groups.0xchat.com` (1,302),
`groups.fiatjaf.com` (16) and `relay29.notoshi.win` (27).

**A group-list window has its own floor, and a shared-id minimum.** Over 21
live NIP-29 hosts the kind-39000 window was min 1, median 9, max 1,302;
`DEFAULT_MIN_SAMPLE` admitted 7 of them, a floor of 3 admits 16. A pure ratio
cannot hold at that width: `{a, b, x}` scores 0.667 against a seven-id leader
and would fold `x`, a group nothing else serves, for the record's whole TTL.
Every live pair measured shares its list entirely, so three ids in common
costs the honest case nothing.

**A credential refusal ends the ladder.** On `filter.nostr.wine` the first
ask is refused in 1.6s and every ask after it on that connection is answered
with nothing, so six rungs cost 61s per url. A refused credential is not a
complaint about the filter.

**A thin window can fold but cannot clear.** `relay.damus.io/lantern-oscar-dynamo`
answered with zero events and was about to be recorded, for thirty days, as a
relay in its own right. Every path that writes a negative claim keeps
`minSample`; only `sameRelay` gets the filter's bar.

**Unmatched members are compared to each other, not just to the leader.**
haven splits `/inbox` from the public pool and `/inbox` sorts first, so on
`haven.calva.dev` six minted paths each shared 96 of 500 with the leader and
were published as six distinct relays while sharing everything with each
other. Against cluster heads rather than every pair, so `nostr.ac` with 20
genuinely distinct endpoints stays linear.

**Reproducibility gates only the negative claim.** `fiatjaf.com` self-scores
0.638 while its two paths score 0.787 and 0.730 against it, and those folds
are right; `multiplexer.huszonegy.world` is the same shape (self 0.594,
siblings 0.622 to 0.870). The bar of 0.9 sits in an empty gap: stable relays
score 0.998 and 1.000, the broken ones 0.435 and 0.694, and `fiatjaf.com`
scored 0.000 on a single page asked twice seconds apart.

**A failed reproducibility check restores the store's verdicts, not
`forget`.** `forget` also dropped the verdicts adopted moments earlier, so the
fan-out saw a host's settled duplicates as separate relays until the next
apply, and the card drew `0 of N checked` on a pass that decided plenty.

**`ws://x` folds onto `wss://x` when both answer.** A scheme is not an
endpoint; no relay in this corpus serves a different pool on plain `ws`. The
pairing costs one extra dial per pass per unmeasured plain url, bounded by
the 103 `ws://` urls in 1,852. The windows keep a one-directional veto so a
`ws://` serving 500 beside a `wss://` serving nine is never folded away.

**A group nothing can be read from folds on the shared name.** This is the
one fold that rests on no measurement. Over 45 multi-url hosts from live relay
lists it fires on three, and one is `filter.nostr.wine`, whose
`/npub1…?broadcast=true` paths are four users' feeds behind a payment wall.
It is on by default because a url nothing reads from is mirroring nothing;
what is lost is the day the relay starts answering, until the verdict lapses.

**The yardstick search walks past the preferred url.** The pathless url is the
right survivor but can be the one that will not answer (a paid front door, a
dead bare `.onion`). `asia.azzamo.net` had 12 urls at containment 1.000 and
folded 11 in five seconds once anything on the host could hold the ruler.
Three attempts, because they are sequential and a dead host costs three idle
windows in a row.

**"Exhausted" is a url that was asked and was silent, not one the search
looked at.** Counting a url the transport guard declined as exhausted removed
foldable urls from the member walk for a whole pass whenever Tor was
momentarily down. The sweep's stand-in yardstick must also clear
`usableWindow`: taking the first window let five ids beat forty and sent a
foldable host to a 24h cooldown.

**Undecidable hosts get a one-day cooldown, held in memory.** Groups are
probed widest first and a host wearing dozens of paths is exactly the shape
that cannot be decided, so `relay.lightning.pub` (four folds in two seconds)
and `multiplexer.huszonegy.world` (four in fourteen) queued behind
`espelho.girino.org` every pass. Not a signed record, because "we could not
measure this" is a fact about our pass. The first cut missed the fourth exit,
a leader that compared nothing.

**Verdicts are written as each group finishes.** A cold-store pass runs for a
quarter of an hour, and a restart during it lost every fingerprint.

**Every dial has a wall-clock deadline of twelve idle windows.** A pass built
from bounded calls still hung 74 minutes on one url of 12,374, holding the
fitness pass and three streams: quartz's `idleTimeoutMs` is an idle window,
disarmed by a relay that never stops sending and by the `onEvent` hook. The
deadline was the first theory for issue 172 and was wrong; the write loop was
the cause, and `FitnessBudgetLiveProbe` measured the nine relays named there
at 1.1 to 11.9s against a 240s budget.

**A page can carry events and a refusal at once.** `chorus.bonsai.com`
refuses a 500-limit ask as anti-scraping and answers the retry with 100
events plus `auth-required`; returning on the flag before ingesting threw
away a served window.

**`spoke` counts a CLOSED, not just an EOSE.** `anyRelayServed` is EOSE alone;
reading `blocked: limit too high` as silence skipped the fallback-page retry
and took every relay capping under 500 out of the fold.

**The second page is the only evidence of paging (issue 187).** A first page
that fills the target never moves the cursor, so `pageable: true` was a
statement about one anchored page. In one 11-minute staging window, 137
relays the mirror aborted as `UNPAGEABLE` were all graded `prime` and tagged
pageable, 49% of aborted visits. Eight dialled directly all advanced the
cursor, so this is not a reproduction of that fault; the argument stands on
its own. Page two keeps page one's `kinds`, null included, or a drain proves
nothing, and it gets no slack because the cursor is the relay's own stamp.

**The probe page is sized from the target.** `over` left every caller at 500,
so the fitness pass asked each relay for 500 to reach a verdict it had after
20: one sweep over 923 urls grew a 20,347-event store to 369,210.

**NIP-42 waiting is quartz's derived default.** `pendingOnAuthRequired = true`
was passed here with an A/B against `auth.nostr1.com` arguing ~19s per
auth-gated leader; amethyst #3905/#3906 made the default `hasAuthResponder()`
and bounded the wait by the auth rather than the idle window.

**The whole recorded world is grouped, survivors only.** A url arriving alone
on a host whose siblings dropped out of the candidate set was a group of one
while a record folding it sat unread. Urls that folded away are left out: on
a polluted corpus they are tens of thousands of members that exist only to be
walked past, and a group whose survivor is absent would elect a known
duplicate to lead it.

**An absent survivor re-elects, through `standIns`, never `aliases`.**
`relay.typedcypher.com`'s four paths folded onto a bare host that then stopped
upgrading the websocket; its `dead` verdict held the survivor out, all four
unfolded and were graded `prime`, and because `dead` lapses in 24h and a fold
stands for 30 days the host alternated between four prime urls and one dead
one twice a day for a month. The stand-in is routing only: `FitnessPass`
signs an `l=alias` record for every entry in the map it is handed, and
`publishFitness` owns that tag.

**`collapse` is one pass with one `canonicalOf` per url.** It was a `map`, a
`filter` and an `associateWith`, each re-deriving the stand-in, over 12,374
urls in front of every roster tick.

**`replace` is one walk per url, not a bulk forget then a bulk adopt.** The
map is shared by every stream and the monitor; between the two walks every
fold in the store was missing, and another stream landing there dialled the
duplicates for a whole cycle with nothing reporting it.

**`replace` resolves chains against the store's answer, not the live map.**
The end of each chain is computed from the verdicts as read, so the result does
not depend on the order the store returned them in, a cycle resolves to nothing,
and a url whose chain leads back to itself is skipped rather than pinned as its
own duplicate, which would leave it `measured` forever. `endOf` and `resolve`
stay separate for that reason: one reads the store's answer while the live map
is mid-update, the other reads the live map while learning.

**Containment at half, not a symmetric ratio near one.** One dial may be cut
short by the peer's `default_limit`, and a truncated window is still the same
window, so the test is how much of the smaller window appears in the larger.
The two windows are seconds apart against a moving feed, so near-identity would
fold nothing on exactly the busy relays where duplication costs most.

**The probe page matches the target, and the fallback page is tried
unconditionally.** 500 is the cap most hosts advertising `max_limit` publish,
and asking over a relay's cap risks a refusal rather than a truncation, so a
relay serving a full page answers in one round trip. A refusal and an empty
relay are indistinguishable at that layer, so an empty first ask is always
retried at the humbler page.

**`unresolved` tests `measured`, nothing narrower.** A leader everything
folded onto is a canonical, which a hand-written predicate did not count, so
every fully folded group was re-dialled once per pass forever.

**The next-pass countdown is unset while a pass runs.** A countdown computed
from the constant rendered as `measuring · next pass in 0s`, which reads as a
pass that is late rather than one in progress.

**The fast lane does not bump `generation`.** The counter has no reader in the
router today, and making the lane move it would be an unobservable behaviour
change; worth revisiting the day something reads it.

**`collapse` leaves two cases alone on purpose.** A present but un-dialable
survivor, and an elected stand-in that the caller's own gate then drops, are not
handled there: the stand-in election only fires for a survivor absent from the
candidate set, and a survivor that is present but does not answer is the
fitness pass's `dead` verdict to take, which then holds it out of the next set.

**`applyVerdicts` reads into a view of its own.** It adopted the store's
answer for whatever set it was handed into the fold's live map, and the
mirror's roster hands it one stream's urls outside the monitor's pass gate:
`replace` over that narrower set dropped the canonical mark of every url
whose alias sat outside it, mid-fold. The read path now builds a throwaway
`RelayAliases` from the same load, and only the fold's own pass replaces.

**A refusal of our key is not an answer the shared-name fold may rest on.**
`leaderPrint` counted an auth refusal as "spoke", so a host where every path
rejected our NIP-42 key folded on its hostname alone and signed public
`same-as` records across what may be distinct relays: a refusal reads the same
from any server. Auth-refused urls are now unmeasured for that rule, and
`FOLD_EPOCH` went to 3 with it so the folds signed under the old rule stop
being read.

**A plain canonical yields the lead to its secure twin.** `leaderOf` keeps an
existing canonical as the yardstick so a new alias cannot re-point a verdict,
and when that canonical was a `ws://` url every later path folded onto the
unencrypted endpoint (`relay.damus.io/yankee-india` → `ws://relay.damus.io/`
on staging). A plain canonical whose `wss://` twin is in the group and not
folded now hands it the lead; the plain url is then measured against it and
folds as a scheme twin, and its old aliases follow the chain.

**The roster's fold view also holds back what the fold has not decided.**
`applyVerdicts` returns `waiting` beside `aliases` and `standIns`: each
undecided url onto the url fitness grades for its server, computed from the
monitor's known set (`ServerIndex`, indexed by server so a roster rebuild
does not walk forty thousand urls per stream) so both sides name the same
stand-in. Trailing
slashes, after a path or a query, and the scheme never name a different
endpoint for this rule (`/ar` and `/ar/`, `?reject=3,6,7///` and its longer
runs): a url waits on its decided twin first. Quartz's normalizer keeps those
urls apart, so the records stay two; only the grade and the roster are one.

**Who stands in is one rule, and never a url proven to be another relay.**
Fitness and the roster elect through `eligibleIn`: a url the monitor knows,
not inconsistent, standing ungraded or `prime`. A stand-in graded as refusing
(restricted, silent, auth-refused) hands its server to the next path on the
next sweep, or one refusing root would keep a path-routing server off the
roster for good. A url measured `distinct` is never a stand-in: it is proven
its own relay, not proven to be the undecided path beside it, and holding the
outbox behind the inbox would send the outbox's authors to the wrong relay.
That holds for a canonical the fold also cleared: on lang.relays.land `/es`
is its own relay that `/es/` folds onto, and electing it held the quiet `/ar`
behind a different language. A
stand-in that itself waits on a twin hands its server straight to the twin, so
no map has a chain for a one-hop re-key to stop halfway along. And where the
monitor knows a canonical the stream does not name, the roster folds onto it
rather than electing an alias in its place, which put one relay on the roster
twice.

**The consistency pass waits on the fold too.** It walked every url the fold
had not folded away, so a server whose paths were all undecided was
paired-walked once per path. Over Tor that is the expensive case: staging's
status page showed `yzzss…onion/bravo-lima-yonder`, `/ember-glyph`,
`/india-yankee` each "holding for 2m · paired walk" on a socket of the 32 Tor
allows, 27 paths of one hidden service that answers on none of them. It now
takes `AliasFolding.heldAmong` from the same `ServerIndex` fitness reads and
walks only the stand-in; the rest report under their own reason, `waits on the
fold behind another url of its server`, so the candidate set still divides
once. In staging's records, 374 `.onion` servers carry more than one path
(2,581 urls): on 292 of them (2,008 urls) no path answers at all, so no
yardstick exists and the fold cannot decide them; on 64 the root is `prime`
and every other path answered the upgrade with HTTP 404, correctly `dead`; 18
folded. Tor's SOCKS replies were also untaught: `SOCKS: Host unreachable` (the
hidden service could not be reached) and `SOCKS: TTL expired` read "a reason we
do not recognise" on 1,399 of the 2,533 silent onion urls. `Silence` now places
them as `no route to the host` and `it never answered in time`; the grade is
`silent` either way, so no epoch bump.

**The yardstick search walks past a thin window.** It stopped at the first
url that answered at all, on the reading that a thin window is a fact about
the host. On a path-routing host it is a fact about the path:
`lang.relays.land/ar` served 7 events and `/zh` and `/es` 500 each, disjoint,
so the whole group was "no yardstick" for good and waited behind a root that
serves a web page. The search now goes on with two budgets, because the
cap existed for silence: `YARDSTICK_ATTEMPTS` (3) urls silent or declined,
each costing a deadline, and `THIN_ATTEMPTS` (12) thin answers, each one
short page. Three was not enough on its own: `/ar`, `/ca` and `/cs` sort first
on that host and are all quiet. The first thin window is kept for the
scheme-twin exit. A host thin everywhere costs twelve small probes a pass
instead of one. No epoch bump: a
yardstick found this way is held to the same bars, so nothing an earlier
verdict concluded changes.

**Why some hosts stay undecided, as far as a full local run showed.** A sweep
over 196 production relay lists (1,997 urls, 292 on the hosts #258 names)
left 155 hosts undecided: 121 with no yardstick (mostly a single url that
cannot answer, now also the thin-first case above), 21 our own transport, 12
nothing to compare, and `relay.nostr.net`, whose leader shared 237 of 500 ids
with its own second walk under the pass's load. Alone, the same walk repeats
at 0.98 over 45 seconds at a 60-second anchor and at 1.00 at deeper ones,
which read as "the anchor is not the cause". **That was wrong**: 45 seconds is
too short a gap to see it, as the next paragraph measures. Undecided costs one
roster entry per server, not one per path, because of the waiting rule above.

**The anchor is half an hour back, because a minute-old window does not
last.** Re-run over all 36,293 clearnet urls with the guard's line naming the
second walk's size, every one of the 25 hosts that failed it got a FULL second
walk (500 ids, 122 for fiatjaf.com) sharing 0 to 237 of the first: not a
relay throttling or our transport cutting the walk, but a different window
from the same anchor. Under a full sweep the second walk comes minutes after
the first, since every member is fingerprinted in between. Walking the same
anchor again after a gap, alone and unloaded:

| host | 60s anchor, +1 / +3 / +7 min | 10 min anchor, +3 / +7 / +15 min | 1 h anchor, +3 / +7 / +15 min |
|---|---|---|---|
| `yabu.me` | 500 / 500 / **1** | 500 / 498 / 481 | 500 / 498 / 494 |
| `relay.agentry.com` | 500 / 500 / **0** | 484 / 451 / 413 | 493 / 473 / 454 |
| `relay.primal.net` | 479 / 475 / **118** | 500 / 499 / 495 | 495 / 495 / 494 |
| `nos.lol` | 489 / 486 / **257** | 500 / 498 / 497 | 495 / 495 / 495 |
| `temp.iris.to` | 498 / 498 / **260** | 500 / 498 / 497 | 500 / 500 / 499 |
| `relay.nostr.net` | 334 / 332 / **146** of 344 | 350 / 347 / 346 of 350 | 321 / 321 / 321 of 326 |

At a minute the window holds for a few minutes and then collapses, so recent
events leave a relay after it has served them (the cliff at five to seven
minutes looks like NIP-40 expirations; what it is matters less than when). At
ten minutes and deeper the collapse is gone. A slower drift remains on hosts
whose 500 ids span over an hour (`relay.agentry.com`, `relay.binaryrobot.com`
and `schnorr.me` decay almost identically, one backend; `relay.angor.io`
likewise), which loses 2-3 points a minute at any depth and is somewhat
smaller the deeper the anchor. `relay.damus.io` answered at half an hour and
errored at an hour, twice, which caps the depth. No relay in either run sent a
NIP-01 `rate-limited:` refusal, so quartz's back-off handling (pinned at
`25a3f37c83` for it) has nothing to act on here. No epoch bump: a verdict
taken at the old anchor was proved at that anchor.

Validated by the same full replay at thirty minutes: hosts failing the guard
went from 25 (30 on the previous quartz) to 1, `tamby.mjex.me`, whose second
walk never answered; aliases from 14,436 to 15,300; undecided hosts from 708 to
680; every other grade within run-to-run noise, and still no server with two
unproven primes. `relay.damus.io`'s root went ungraded in that run because it
answered the upgrade with HTTP 503 during fitness, its per-IP budget
(`read bandwidth budget exhausted (1048570 bytes/min per IP)`, worded as
`ERROR:`, not NIP-01's `rate-limited:`), which fitness rightly defers.

**The yardstick ladder is the fitness pass's shapes.** `leaderPrint` asked a
bare filter, kinds 1, then group metadata, so an indexer refusing the first
and empty on the second never gave a window and its paths could not fold.
It walks `AliasProbe.SHAPES` now, and the group fingerprints through whichever
shape answered.


**Fold writes go through the same bounded writer as the other passes.** They
were each wrapped in an empty catch under only the record edit's own two-minute
deadline, one group's verdicts after another while the pass gate was held, so
a store that stopped answering stalled the stability gate, fitness and the
fast lane behind the fold, and a failed write was counted as learned with
nothing logged. `VerdictWriter` is `writeEach`'s limits held across a whole
pass: each write under `PUBLISH_DEADLINE_MS`, and once the wedge limit or
budget trips every later group's writes are refused rather than waited on.
Groups still write as they finish; only a write the store took counts as a
new alias, and the unwritten are logged and measured again next pass.

**What the passes hold in memory is bounded by what a sweep names.**
`RelayAliases`' three maps, `RelayConsistency`'s two sets and the fold's
cooldowns only ever grew: `replace` touches the urls it is handed and nothing
else, so a url that left the candidate set stayed in memory for the life of
the process. A full sweep now drops everything outside its world (the fold's
candidates plus every survivor the record names) and every lapsed cooldown.
A fast-lane batch is a handful of urls, never the set, so it prunes nothing.
The store stays the authority; a url that comes back is re-read from it.
