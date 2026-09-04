# Progress document decisions

The history behind `peers/.../progress/InFlight.kt`, `Processors.kt`,
`StoreCalls.kt` and `StatusVocabulary.kt`, moved out of the source so the
code reads on its own. One paragraph per decision; `git log -L` on the
member finds the commit.

**The glossary ships inside the document.** The `sync` section published a
dozen counts and the word "done" covered three of them: a fan-out leg that
started and came back (`fetching 16747/16752`, including every unreachable
leg), a band that settled, and the span in which every kind has produced
evidence. A definition that lives only in a KDoc is one the reader of the
JSON does not have. `StatusVocabularyTest` pins both directions: no term
without a published member, no published member without a term.

**Each document carries only its own half of the vocabulary.** The two
planes publish disjoint halves, and a definition for a member the document
does not carry is a promise that rots into fiction. `termsFor` walks the
document rather than taking a list, so there is no third thing to keep in
step with the two it describes.

**In-flight relays are named, not counted.** A production `sync.progress`
reported `pending = 2` on a stream that had received two events in eleven
and a half hours, and nothing in the system could say which two: the count
was derived by subtraction, the coverage card only draws relays with a band,
and container stderr rotates inside the hour. `RelayRotation` held both urls
the whole time.

**`transferringForSec` measures the slot, not the socket.**
`InFlightReportProbe` watched a url that could not be connected to at all
report `transferring 0s` for its whole life and end `CANNOT_CONNECT`. The
connect happens inside the slot, so a leg stuck on a handshake is
transferring; absent with a large `heldForSec` means the pool is saturated.

**`stage` and `pool` are two members.** `stage` is a sentence written to be
read once; `pool` is the word a page may group by. A page that grouped rows
by the sentence was grepping prose, and a gauge here was zeroed once when a
word was reworded.

**Probe passes publish a live position.** `Work` is written when a pass
returns, hours after it started on the stability gate, and the sweep unsets
its countdown while it runs, so for those hours the row said `measuring` and
nothing else. The walk is timed from `Handle.measuring`, not `begin`: the
gate reads verdicts a page of 500 at a time before it dials anything, and
that derivation must not land in the rate's numerator.

**`quietForSec` sits beside `etaSec`.** A fitness pass sat at `attempted:
12373, toProbe: 12374, etaSec: 0` for 74 minutes with every number on the
row agreeing with every other. The ETA is honest arithmetic on the rate so
far and reads 0 for a wedged last unit and a finishing pass alike.

**A finish with no begin does nothing.** `FitnessPass` brackets its own
`measure` because the fast lane calls it outside the monitor's loop, and the
sweep also brackets every pass it runs. Both finishes landed, so the fitness
row reported two `passesRun` per sweep.

**The processor's held list is whole and longest-held first.** It was cut
to twenty on the argument that 499 of 500 rows are ordinary dials a second
old, which is true of the rows and false of the list: the whole set sorted
by age is the distribution, and the distribution is the finding. A held
row is a job, so the pass's own dial gates bound it. A fitness pass once
held one url of 12,374 for 74 minutes and the url was recoverable only from
OkHttp thread names.

**`undecided` is never cut.** A reason is an enum value in this source, so
the network cannot grow the list, and the rows sum to `unmeasured`: a cut
tail surfaced on the card as `not accounted for` in the fault tone against a
pass that was working. The cap was one short twice, at six and at eight,
each time because a reason list grew and the number did not.

**Example hosts are a safety ceiling of a hundred, not a sample of three.**
Three names under a reason holding twenty-eight hosts withheld the one
actionable thing about the row, which servers; the pattern was already
legible from the reason string. `top` was six for the same reason and still
deliberately does not sum to the reason's urls, so a cut list cannot read as
the whole one.

**`newUrls` is the denominator, not `candidates`.** Most of a settled
candidate set carries a verdict from weeks ago and is never asked again, so
a position counted against it moved by a rounding error however much a pass
achieved. Dropping `foldedAway` from both halves turned `12,024 of 16,752`
into the honest `595 of 5,323`.

**Members a pass does not measure are null, not zero.** The alias fold
publishes no stability verdicts, and a zero from its row drew "0 consistent,
0 refused" beside a gate that had measured everything and found nothing.
Absent is "does not answer"; zero is "answered none".

**Store calls are attributed through the coroutine context.** Every
subsystem hits one anonymous `IEventStore`, so an engine queue at 599 had no
owner. Three call sites are functions of an `object` taking the store as a
parameter and two more are adapters the store's own interfaces construct, so
a constructor argument would thread through six public signatures for a
diagnostic. A call outside a scope carrying the element is untracked, never
misattributed; a global every test writes into would describe calls nobody
made.

**The slow-call warning is a minute; the wedge bound is ten.** A false
wedge retires the word, while a log line costs nothing and can be wrong all
day. A healthy `oldestBatchSec` measured 43 seconds on production, so a
minute clears the ordinary shape. Re-warning every five minutes gives a
timeline a wedge watched over hours can be read off.

**The server half of queue-versus-service is not published.** A fresh query
returning in 0.7s while queued ones wait 175s is inferred to be queueing;
only a service-start stamp from the store settles it, and the store builds
its own OkHttp client with no header or interceptor seam. `outstandingAtIssue`
carries what this side can say: the dispatcher is 1,024 wide, so a slow call
with few outstanding did not queue on our side.

**Live and lifetime counters share a row and do not partition.** `issued -
returned - failed - cancelled` is stamped at different instants from the
row's insertion and removal, so it need not equal `outstanding` on a busy
router. The live members come off one row snapshot so callers, age bands and
total agree by construction rather than by three atomics staying in step.

**Store call rows are capped at 200; in-flight relays are not.** A leg is a
worker and one configured width bounds it. A call is bounded by ingest
workers times the store's fan-out, plus one per concurrent visit, plus the
monitor's chunked reads; a router with 67 concurrent visits carried
hundreds. Rows are longest-running first so a cut only drops the youngest.

**Age bands are logarithmic.** Everything healthy is under a second, a
negentropy snapshot of a wide filter takes tens, and the two bands past five
minutes are where a wedge lives. Even bands put the whole healthy corpus in
one row.
