# Sync support decisions

The history behind the visit pool's supporting classes in `sync/.../sync/`
(`VisitQueue`, `VisitAborts`, `RelayComplaints`, `RelayPages`, `RelayReads`,
`FilterWidths`, `PoolLimits`, `AuditSchedule`, `RouterTuning`, `SyncManifest`,
`PressurePoller`, `UpstreamPush`), the heal plane (`heal/Healer`, `HealQueue`,
`WriteCapability`, `OkClassifier`) and `refused/RouterRefusalSink`, moved out
of the source so the code reads on its own. One paragraph per decision;
`git log -L` on the function finds the commit.

**Transfers are bounded by idle time, never by a wall clock.** `NEG_IDLE_MS`
resets on every message, so a relay still delivering a history that runs for
hours is never cut off. A wall-clock deadline can only ever fire on the
healthy case: one once truncated four working upstreams at exactly its 4h
mark. The monitor's probes size their own idle window per url from
`connectionTimeout` and do carry a hard deadline, because a probe is a single
twenty-event ask and cutting one costs a re-measure, not a truncated history.

**A relay may go quiet for ten idle windows before its remaining asks wait for
the revisit.** A stream with author-bound asks visits one relay once per bound
author, and a relay answering each chunk with a full empty idle window cost
`chunks * NEG_IDLE_MS`: measured at 5h00m on one url, of which 4h56m delivered
nothing. `LEG_QUIET_GIVE_UP_MS` is reset by every event, so it cannot fire on
a leg that is working.

**Three narrowings per visit.** A relay that states its limit is under it on
the first retry; one that only says the ask was too wide is halved, and from a
139-kind ask that is seven halvings, each re-walking the chunks that already
succeeded. The learned cap outlives the visit, so the bound costs nothing in
convergence. Measured (`WidthRescueLiveProbe`): `git.cloistr.xyz` fits in one
visit (139, 69, 34, 17, served); `purplerelay.com` is still refused at 17,
aborts, and the revisit narrows once more to 8 and pages back to 2023.

**Filter width caps come from the relay's refusal, not a constant.** On one
90-minute staging window nine relays refused the 139-kind `contentViaOutbox`
ask outright, in three shapes (`too many kinds in filter: 139`, `too many
kinds in filter`, `too many kinds (max 100)`), and a visit that re-asks the
same width can never complete. A static cap is either above somebody's limit
or below everybody's, costing round trips on the thousands of relays that
never had one. Caps are not persisted (one refused ask re-learns them) and
NIP-11 offers nothing to read: `limitation` carries `max_filters` and no kind
width.

**A width cap is only learned when it is strictly narrower.** A refusal that
quotes our own width back is the offence, not the limit, and halves; a repeat
of a cap already held is a refusal for another reason, and re-walking it
would loop. The phrase gate stays at `too many kinds` because widening it to
`too many` would let a rate-limit or allowlist refusal chunk an ask that was
never too wide.

**`RelayReads` exists because `fetchAllPages` is an extension on `NostrClient`.**
A pool calling it directly can only be driven by a real socket, so the pool's
own scheduling (which units run together, what a refusal ends, when a tail
opens and whose budget it costs) had no test and every change was verified by
reading; two defects in one afternoon said what that was worth. It is
deliberately three methods, not a client abstraction.

**`VisitQueue` owns the rotation invariants that five bare collections shared
by convention.** Two audited races lived there: a requeue swallowed when a
worker collided with a running visit, and revisit timers that stacked into
double-cadence chains. The park and the finishing visit's removal are one
`synchronized` step because, between a failed `inFlight.add` and the park, the
running visit could finish, see nothing parked and arm a timer, downgrading
the prompt requeue to a timer wait and buying one spurious back-to-back visit.

**Eviction disarms the pending revisit.** The delay is read when the timer is
armed, so a unit armed while tailed got the tailed cadence (half an hour
against five minutes) and the visit after an eviction found the old timer
standing and armed nothing: six times the freshness gap on the relays least
able to afford it.

**A lazy revisit job that loses the slot is cancelled, not dropped.**
`scope.launch(start = LAZY)` defers the body, not the parenting, so an
unstarted, uncancelled job stays an incomplete child of the router's scope for
the life of the process, one per lost race. The race is real because
`armRevisit` runs after the unit has left `inFlight` and the gap is as wide as
the caller's `revisitDelayMs` lambda.

**Abort reasons are a partition, and the relay's sentence rides with them.**
`abortedVisits` read 92.5% on staging and nothing said which relays or why;
the relay had usually been explicit (`restricted: not on the allowlist`) and
nobody was listening. The `CLOSED` message is dropped between quartz's
listener and `PagedFetchResult`, so `RelayComplaints` keeps the last thing
each relay said. A NOTICE counts as a CLOSED because relays refusing a wide
filter answer with a bare NOTICE and never EOSE.

**`awaitSince` gives the sentence a grace to arrive.** Quartz dispatches a
`CLOSED` to subscription listeners before connection listeners, so a plain
read right after `fetchAllPages` returns is a hop too early at random.
Measured: the narrowing got three sentences of three from `git.cloistr.xyz`
and lost the race on the second attempt against `purplerelay.com`, stopping
at 69 kinds and aborting a relay that was one halving from working.

**Complaints are bounded at 4,096 relays and 200 characters.** The listener
sits on the client both planes share, and a probe pass dials every url
discovery ever named (20,340 on one cycle) against a roster of hundreds (678
on the deployment issue 185 came from). Past the bound a known relay stays
fresh and a new one is dropped, since the repeat complainers are the ones
anything reads. The longest refusal measured was 62 characters.

**`VisitAborts.cleared` forgets a unit's last abort on a clean visit.** Without
it a pair that met a transient `CANNOT_CONNECT` at boot and wrote no band
since (a relay simply empty for the filter is the ordinary case) read
`refused`, `fault: true`, with a stale sentence at the top of a worst-first
table for the life of the process. The last abort is recorded before the
narration gate so a rationed line cannot blank the status row.

**`RelayPages` samples the socket because nothing downstream can (issue 187).**
A walk aborts on `downloaded == 0`, which means every event that arrived
failed the filter match, and every instrument in the process is downstream of
that match. Eight of the issue's relays dialled directly in six ask shapes,
including the real 141-kind ask through `fetchAllPages`, all drained
honestly; the fault is only visible where it happens. Counts are tallied on
arrival, not at render over the five retained rows: a 158-event page whose
first five matched once reported "all of them matching the ask", the one
sentence that points at our side of the walk.

**Pool limits are per (stream, job), with no router-wide cap.** A content
mirror over ~130 kinds and a thirty-relay index stream ride one pool, and
the first one's audits occupied every worker the second needed, since the
pool's fairness is per url. A second ceiling over the top would be a number an
operator keeps in step by hand, and its failure (a stream inside its own
share, refused by a limit named nowhere near it) is the one the shares exist
to make legible.

**`trySpare` is not counted as a deferral.** A tail past its stream's budget
goes to `earnTail`, which evicts the weakest tail and asks again, so a full
live gate is the pool's ordinary steady state. Counting it turned every stream
sitting at its budget into a stream with work being refused, permanently, in
the colour the page uses for a cap an operator should act on.

**An uncapped job's hold is over a null semaphore.** `Semaphore(Int.MAX_VALUE)`
starts at its ceiling, so the first `release()` throws `Maximum permit count
exceeded` out of the `finally` that ends a leg, on the path every deployment
that configured nothing takes. The cap and its permits are one `Gate` because
two maps built from one filter needed a fallback for a disagreement
construction had made impossible.

**`AuditClock` replaces a `Long?` that carried two conventions.** The engine
gate and the status row read `null` as "never run, so due" and a sentinel as
"nothing schedules this", opposite meanings in one nullable. The
`deleteMissing` decision about which clock schedules an ask was written in
three places (the engine gate, the status walk, the retraction plane) and the
copies disagreed, so the panel built to certify that audits run only when due
read permanently in arrears while the audits ran perfectly well.

**The manifest scopes the mirror comparison to the kinds actually asked for.**
A client comparing a filtered mirror against an unfiltered total drew 35% on a
relay that was not behind by one event (31,118 here of 89,485 there for one
author, the difference being kinds 3, 4, 5, 6, 7 and 1059 that the filter never
asked for). It is a separate file from `SYNC_STATE_FILE` because bands are
state rewritten every 30 seconds and this is config written once at boot; and
`publishes` is separate from `write`'s result because the two were briefly one
boolean, which announced "unset" at an operator whose volume was read-only.

**The pressure poller announces a feed that never connected.** One flag
conflated "the feed has ever worked" with "we have announced it down", so a
typo'd url was never reported while the boot line kept claiming ingest was
yielding: the configured-but-silently-inert failure this codebase forbids.
`poll()` restores the interrupt flag because `HttpClient.send` clears it on
throw, and swallowed with the HTTP failures a shutdown counted as a miss and
left `close()` a no-op.

**`UpstreamPush` takes one id snapshot per pass.** A round changes the
upstream's set (it gains what we push), never ours, and re-reading gigabytes
of ids per round bought nothing.

**Heal first, then remember.** A suppressed id is never downloaded again and
so can never re-trigger, so `RouterRefusalSink` enqueues the repair before
recording the id. The only way into the heal queue is a store refusal of an
event a relay actually served, which is the whole amplitude guard and the fact
the consent argument rests on.

**`HealQueue` drops rather than blocks, and drains one key at a time.** A heal
dropped is a retry the next offer of the stale copy rediscovers; blocking the
sweep would trade the thing being fixed for the fix. Draining by swapping the
map out lost offers that had resolved the old slot and counted them anyway, a
drift that only rose until it hit `totalLimit` and every later offer was
dropped: the healer silently stopping. Its mirror was a check-then-act
overwrite in `offer` that re-inserted an entry `total` never counted, a drift
that only fell and quietly widened the memory bound.

**The healer drains only what it will attempt.** Draining the whole queue and
breaking out at the cap threw the remainder away: an entry out of the queue
and never pushed is re-queued only on another refusal, which stops the moment
its id is suppressed, so the relay stayed stale forever with nothing to say so.

**Any answer clears write strikes, not only an accept.** Strikes measure
silence, and a relay that says `duplicate` is demonstrably not ignoring us.
Clearing only on `ACCEPTED` let a relay that timed out once and answered
every later push keep its strike, gain two more across passes, and be closed
for writes while plainly replying. `CLOSED` is excluded because it is a
verdict, not a sign of life. `probedCount` records clean states too: counting
only struck or closed relays read `0/0` on a fan-out where every relay
accepted, which is what a healer that never ran also prints.

**`rate-limited:` and `error:` are transient, and an unknown prefix is nothing.**
Reading a momentary refusal as policy would suppress the very ids the relay
was about to accept, permanently and silently. Unknown prefixes get the same
conservatism `Unreachability.proves()` applies to NIP-66 claims: silence about
someone else's server costs a retry, being wrong costs a false statement.

**A relay-scoped kind 62 is never handed to another relay.** `VanishTargets`
passes `ALL_RELAYS` and the relay's own url only, and filters before the
newest-wins pick: taking the newest first would let a newer relay-scoped
request mask an older `ALL_RELAYS` one, skipping a push that should happen,
silently.

**The healer exists because a tombstone is private memory.** A suppressed id is
as durable as one disk and fixes nothing for anyone else, while a healed source
stays healed for every mirror. A repair can only be queued because the relay
served its own stale copy of the author's event, so the push changes the
version a relay serves and never the distribution set; introducing an author
to a new relay remains `dir = up`'s job. Everything expensive happens per
relay at the end of that relay's own visit while its socket is open, yielding
to `ServingPressure` as ingest does.

**`RelayPages` reports subscription ids rather than filtering on them.** The
walk's own subscription id is not knowable on the connection listener, and a
page carrying another subscription's events is itself the answer to "what did
the relay send when the walk says nothing". The listener is armed per walk
rather than always on because it sits on the hottest path in the process:
disarmed, the work per message is one `isEmpty`.

**The manifest computes no union of kinds.** The relay's `MirrorReport` does
that from the per-stream lists, so two places cannot disagree about it. The
streams written are the ones this process is running, not every stream in the
config; a stream with no `kinds` gets no `kinds` member, and `writtenAt` lets
a reader spot a document that outlived its writer.
