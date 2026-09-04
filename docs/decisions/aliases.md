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

**The fingerprint is paged, from one shared anchor a minute behind the
clock.** Across 60 live hosts, `max_limit` was 500, 100, 1024, 2100, 10000,
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

**`unresolved` tests `measured`, nothing narrower.** A leader everything
folded onto is a canonical, which a hand-written predicate did not count, so
every fully folded group was re-dialled once per pass forever.

**The next-pass countdown is unset while a pass runs.** A countdown computed
from the constant rendered as `measuring · next pass in 0s`, which reads as a
pass that is late rather than one in progress.

**The fast lane does not bump `generation`.** The counter has no reader in the
router today, and making the lane move it would be an unobservable behaviour
change; worth revisiting the day something reads it.
