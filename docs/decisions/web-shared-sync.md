# Shared stats page decisions

The history behind `web/src/main/resources/web/shared/` (`sync.js`,
`processors.js`, `statspage.js`, `readiness.js`, `query.js`, `stats.css`),
moved out of the source so the code reads on its own. One paragraph per
decision; `git log -L` on the function finds the commit.

## sync.js

**The judgements live in a module, not in the page.** The only pins over the
inline card were string greps for member names, and a grep cannot see a wrong
denominator. An audit against crafted documents found five shipped bugs: an
empty health object drawing an empty chip, `NaN%` from an absent numerator, a
quiet bar scaled by a row not on screen, a division by a zero capacity, and two
meters in one column whose full ends meant opposite things. The rebuild also
deleted fourteen exports that survived only because their own tests referenced
them; `measuringOf` lost its `share` for the same reason.

**`STUCK_LEG_SEC` is ten minutes.** The slowest healthy leg measured on this
deployment was the full purplepag.es `indexers` walk, about 10.8 minutes for
1.49M events. Anything lower marks legs doing exactly what they should.

**`STUCK_PASS_SEC` is five minutes, not `STUCK_LEG_SEC`.** A probe job is
bounded by the monitor's per-url deadline (twelve idle windows, four minutes at
the default `connectionTimeout = 20`), so a pass that has finished nothing in
five is one whose jobs are all outliving a bound meant to end them.

**`STUCK_CALL_SEC` is sixty.** The router's default, and a healthy
`oldestBatchSec` measured 43 on production. The router's own `slowAfterSec`
wins where published; a page marking at its own 60 under a router set to five
minutes would have the log and the colour disagree by the same word.

**`IN_FLIGHT_SHOWN` is unbounded.** At five it named five legs of a fan-out and
deferred five hundred, and the leg being looked for is by definition not in the
healthy head. The router already caps its rows (`RelayRotation.DEFAULT_IN_FLIGHT_ROWS`).

**`NAMES_IN_TOOLTIP` is twelve.** When the router's cap on host names moved to a
hundred, production's widest reason (186 hosts) produced a 1,740-character
tooltip against 159 at six names, and a native `title` is one unwrapped run
some browsers truncate. Twelve is double the old head and still one glance.

**The socket ceiling is 1024.** At OkHttp's stock 64 a 20,340-relay cycle
projected 330 hours. `sockets` near the ceiling is the healthy state of a
mirror that stays connected; only `socketsQueued` above zero is a fault.

**There is no `isLive`.** When the mirror wrote its state to a file the serving
relay read, the page inferred liveness from a `writtenAt` heartbeat at 150
seconds. The mirror serves its own page now; a page that renders is a process
that answered, and the past-tense constraint verdict went with the heartbeat.

**There is no `splitProcessors`.** Both planes' rows once arrived in one
`processors` array and the page sorted them by name with an allowlist. Each
plane publishes its own document now, so the allowlist that could go stale
against a newly registered processor is gone.

**A lock wait never appears alone in the stage shares.** The burst sweep in
AGENTS.md showed eight workers at `lock.ingest.wait 243.9s` across a 37.6s wall
delivering one worker's throughput; ranking by magnitude alone put that row on
screen with `write` and the matching `hold` cut off below it.

**A folded url is out of both halves of `probeProgress`.** The bare complement
of `unmeasured` counted every folded url as one the gate checked: the real card
read `12,024 of 16,752 checked for consistency` beside a tree showing 583
consistent, from the same document. The gate's own line is `595 of 5,323`.
Where the row publishes `newUrls` that is the denominator instead: a fold that
had just run eleven minutes read `143 of 1,754` with neither number moving.

**The funnel is a tree, not an icicle.** One row per level, each a share of one
width, needed three captions and a legend to say what indentation says for
free, and on the real card read as four unrelated bars. Absent is not zero: a
pass publishing none of the verdict members once drew a tree claiming every url
was unaccounted for, caught by a screenshot. Rooting at `sourced` alone drew an
eighth of a corpus whose records outnumbered its current lists five to one, so
`recordedOnly` joins the root. Hosts under a reason are two numbers, not rows:
one row per server on a corpus of two thousand.

**The quiet bar's denominator is the threshold.** Scaled against the worst
published row, five bars rendered at 0.08% each with the outlier at row eight,
outside the five drawn. Scaled against the worst shown row, five legs each a
healthy thirty seconds quiet rendered full, which reads as stuck.

**Pools are labelled by job, not transport.** `negentropy` beside `re-fetching
the past` read as a row about a protocol beside a row about time, with no cue
they are alternatives. The stable `pool` word remains the grouping key so a
reword cannot empty a table.

**A stream is one section.** The card drew a stream five times (a phase line,
a pool section repeating it, and a row in each of two job tables) with nothing
joining them, and the two roster numbers came off different members. The cut
that only sectioned rotating streams left out the stream that had never come
up, which is the one an operator looks for.

**One `totalsOf` for both cuts.** Written twice, the pool-wide and per-stream
remainders drifted: with `queued` absent one rendered a remainder anyway and
the other said nothing. The remainder is off units, never relays, because the
pool's unit is a (relay, stream) pair.

**`biting` needs both halves.** `deferred` is cumulative since boot, so on its
own a single refusal at boot painted the row hot for the life of the process.

**`HELD_SHOWN` is three and `CALLS_SHOWN` six where `IN_FLIGHT_SHOWN` is
unbounded.** A stream's legs are bounded by its transfer pool and every row is
interesting; a probe pass at the monitor's default dial concurrency holds five
hundred urls of which 499 are ordinary dials a second old, and the router
publishes up to two hundred store calls. Both lists arrive longest first, so
the front is the answer and `more` discloses the rest.

**Stage time is shares of the poll window, not durations.** The poll is clamped
to 30s to 5min and the chain waits for the previous response, so the window is
a different length every time and two readings of `write 45s` are not
comparable. A ratio also sidesteps `fmtDur` flooring a 400ms stage to "0s".
`before` null yields nothing rather than cumulative totals dressed as a rate.

**Limits and schedule are one row per job.** They were two tables keyed by
(stream, job) at two ends of the card, and a cap at its ceiling only means
something beside the queue backing up behind it, which was in the other table.
The union is walked so a job with only one half is still drawn.

**`recordedOnly` is inside `candidates`, not beside it.** The router derives
`known = sourced + recordedOnly` and then `candidates = known - dead`, so a
funnel that adds `recordedOnly` as a sibling of `candidates` counts every
record-only url twice. The fixture in `sync.test.mjs` once modelled exactly
that shape (a `recordedOnly` nine times the size of `candidates`, which the
router cannot produce), and the double count it hid shipped: staging drew
twice the corpus the router knew of. The fixture's children now sum to
`candidates` and the test holds that the partition closes with no
`unattributed` row, which is the check the old shape could never fail because
`unattributed` only fires when children fall short.

## processors.js

**The held urls are on the card.** A fitness pass held one url of 12,374 for 74
minutes and the url was not nameable from the card, the progress document, the
log or a thread dump, since a suspended coroutine has no frame.

**The corpus tree sits above the pass lines.** Drawn under the row that
publishes it, the widest thing on the card sat below two lines of clock; and
the round-up walked the store for five minutes per sweep while the card said
the three rows under it were idle.

**The queue row states the depth, the chip states the verdict.** Appending
"downloads are backpressured behind it" to every full queue put the busy-mirror
reading beside a health chip saying ingest had stopped. `bottleneck` decides
once; a second copy of the threshold here is how the two drift.

**The visits row keeps lifetime counters only.** It opened with the five roster
members the sync card's pool summary already partitions, in a second vocabulary
(`24 visiting` there, `24 with a worker now` here).

**Hosts are joined with a dash.** Two of the gate's seven reasons end in a
preposition, so ` on N host(s)` rendered `too few events to judge on on 501
host(s)`, and the reasons are free text off the wire.

**`visitsRun` is drawn unconditionally.** Every other counter on the row is
drawn on being non-zero, which is right for each and wrong for all at once: a
pool that has just booted has zero of everything, and a cell guarded all the
way down rendered empty beside a row that was plainly working.

**The quiet clock is drawn only past `STUCK_PASS_SEC`.** On a healthy pass it
is a number between 0 and 2 and the line is already long; past the threshold
it is the finding. `12,373 of 12,374, ~0s left` read as a pass about to finish
for 74 minutes, and nothing else on the row said otherwise.

## statspage.js

**Staleness is judged in a tier's own passes.** Six, so one slow pass is never
reported as a stopped one, and a dead tier is named within six minutes on the
fast cadence and ninety on the slow. The flat six hours survives only for a
document that states no cadence.

**`fill` carries scroll and filter across a rebuild.** The whole-body rebuild
reset scrollTop 5000 to 0 on every tick, which with counters refreshing every
minute made the page unusable.

**The fetch is not `cache: "no-store"`.** That mode neither keeps the response
nor sends `If-None-Match`, so the route's 304 never fired. Do not verify the 304
through Playwright: its Chromium has no HTTP cache and every repeat fetch is a
200. Check the server with curl:
`ET=$(curl -sD- -o/dev/null localhost:7777/stats.json | grep -i ^etag | cut -d' ' -f2)`
then `curl -o/dev/null -w '%{http_code}\n' -H "If-None-Match: $ET" localhost:7777/stats.json`.

**A failed poll keeps the page.** One blip on the timer replaced nine cards of
good numbers with an error line and took the filter text and scroll with it.

**The poll follows the document's fastest tier.** The route was written
asserting the page polled (it mints an ETag for that reason) and it did not; and
when it did, a flat five minutes left counters live in the JSON and not on
screen. Floor 30s so a mistuned interval cannot make every tab a per-second
poller; ceiling five minutes so a charts-only relay keeps its old schedule.

**`docUrl` is document-relative.** `/stats.json` is asked of the host root, so a
relay at that root answers a page mounted behind a prefix with its own document,
200, silently drawing the wrong service's numbers.

## readiness.js

**A ratio needs an honest denominator.** NIP-45 COUNT is widely unimplemented;
nip85.brainstorm.world answered none of the 45 (relay, service) pairs it serves.
Capped at 1 because we can hold more than an upstream serves (it deleted, we did not).

**"Enough" is 90%, compared against the rounded figure.** Straight against the
fraction, an import at 0.897 drew a panel headlined "90%", a number the reader
was told is not worth showing.

**A sentinel in `here` keeps checking.** Folded into the answered-zero branch, a
20s COUNT timeout raised the blocked panel for a reader whose scores may be
fully mirrored: a claim from a non-answer.

## query.js

**Hashtags match by unicode property and keep the hyphen.** `\w` cut `#café`
to `caf`; without the hyphen `#covid-19` lifted `covid` and left `-19` behind,
which NIP-50 reads as an exclusion. Stranded punctuation is removed because a
trailing full stop turned a plain tag read into a text query for ".".

**`until` is the second before the next midnight.** Midnight plus 86,399 lands
an hour inside the neighbouring day twice a year, so a same-day search reached
into tomorrow on the DST change.

**A side filter runs at a quarter of the limit.** The store applies `limit` per
filter, and since store 8a45e4d1a2 filters sharing a rank profile come back as
one ranking of the union, so the quarter bounds how many side hits compete for
the top of the page.

**`search` is sent whenever it carries anything.** It used to be dropped when
the words were empty on the belief that extensions alone were a query for
nothing; the store says an extensions-only query is unconstrained. That belief
cost `#nostr` its sort, spam floor and lens: three visible controls doing nothing.

**`effectiveSort` follows four rules measured against quartz 1866007e99** by
running `SearchQuery.parse`: last `sort:` wins, a quoted span is a phrase, a
leading minus is an exclusion, and the key is case-sensitive and must start a
token.

**A group link that would not tokenize is refused.** Whitespace ends a token and
trailing `.,;!?` is punctuation, so `group:my group` and `group:hello.` silently
returned somebody else's room. Quoting in the token language is the real fix
and a larger feature.

**Scope filters are not gated on the tab.** For `site:x` under a tab without
kind 1111 the gate left nothing standing for the scope, and the base filter
answered as if the token had never been typed.

## stats.css

**Tile numbers size to the tile.** Ten tabular digits are about 6.3em; the
11rem track affords 190px and 1.9rem wants 192, so "68,412,907" crossed the
tile's edge at a 1100px window and the Zaps row cut "1,882,401" mid-digit. At
360px the tile's 1.1rem gutters took 35 of a 129px track, which the phone
padding rule buys back. The size is declared twice because a browser without
container queries inherits the body's 15px otherwise.

**Coverage rows use `content-visibility: auto`.** On a 6,025-row document
(median of six cycles, three runs) clearing the url filter cost 240 to 337ms of
layout against 57 to 61ms; narrowing to two rows 39 to 46ms against 18 to 19ms.
The intrinsic size `max(12px, .78rem)` is the row's height by construction and
lands the scroll height within 0.1%. On the verdicts panel the same rule on the
group as well as the row took 155 to 162ms off clearing the filter.

**The coverage label is rtl only on the box.** With rtl reaching the text,
`nos.lol/` drew as `/nos.lol` and an IPv6 authority reordered into a plausible
but different address. The label column's 8rem floor is where a `*.nostr.land`
family stops clipping to three indistinguishable rows; at 380px the old flat
11rem gave the url 176px and the track 109px.

**Disposition colours are rules, not a probe.** The `getComputedStyle` probe
read a card still detached from the document, so nothing resolved and every
swatch rendered `rgba(0, 0, 0, 0)`.

**The live half of the card is marks.** The sentence-based version ran 520
words for two streams; the same facts as chips, bars and one table run 66.

**`.trend`, not `.spark`.** The kind-activity card already carried `chart
spark` sparklines and the trend rules were restyling them.

**`.card.pending` is scoped.** Bare `.pending` also matched the disposition
bar's `pending` segment, and `border-style` with no width resolves to `medium`,
so the largest segment on a live fan-out drew as a 3px dashed black box. Caught
on a running mirror, not in review.

**Group heads are padded asymmetrically.** The head sat 4.8px off its own data
while 17.6px separated it from the group above; the first head sat flush on the
axis at 0px.

**The `.why` rule is unscoped.** Scoped to `.pending`, the identical sentence in
a chart card fell through to the UA's `margin: 1em 0` and body size.

**Group heads are `nowrap`.** The head is sticky, so a stream naming twenty
kinds that wrapped its counts to a second line took those lines off the top of
every row scrolling under it. The filter chip ellipsises and the counts do not;
on a 390px phone the counts alone ran 50px past the card and clipped "relays
walked" away, so below 45rem they take their own row.

**The search boxes are styled at the element.** A raw `input[type=search]`
inherits the platform's chrome, and in dark mode drew a white field with black
text in the middle of a dark card; three boxes on the page had each been left
that way, since a class is what the next search box forgets.
