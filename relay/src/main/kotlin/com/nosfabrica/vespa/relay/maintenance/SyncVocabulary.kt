/*
 * Copyright (c) 2026 NosFabrica
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
 * AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.nosfabrica.vespa.relay.maintenance

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/**
 * WHAT EVERY NUMBER IN THE `sync` SECTION MEANS, shipped inside the document.
 *
 * ## Why a glossary is part of the artifact
 *
 * The section publishes about a dozen counts and not one of them could be read
 * without this repository open beside it. Worse, three of them were being read
 * as the same thing — the word "done" covered all of:
 *
 *  - a fan-out leg that STARTED AND CAME BACK (`fetching 16747/16752`), which
 *    includes every leg that came back unreachable, capped or out of budget;
 *  - a walk that SETTLED (`complete` on a band — nothing outstanding below the
 *    span it walked);
 *  - a claim of GAP-FREE COVERAGE (`everyKindMin`/`Max`, which is not that
 *    either — it is the span in which every kind has produced evidence).
 *
 * The first is the least meaningful of the three and was read as progress.
 * Naming them apart is most of the fix; publishing the names beside the numbers
 * is the rest, because a definition that lives only in a KDoc is a definition
 * the reader of the JSON does not have.
 *
 * ## Rules this list is held to
 *
 * **One entry per published member, and no entry without one.** A term here that
 * nothing emits is a promise the document does not keep; a member emitted with
 * no term is the state this exists to end. `SyncVocabularyTest` pins both
 * directions against the other reports' output.
 *
 * **Say what the number is NOT, where it has been misread.** Half of these
 * entries are longer than a definition needs to be because the short version is
 * what produced the wrong reading in the first place.
 *
 * **Approximations are marked as such.** `frame`, `evidence` and the paging
 * fraction are estimates or envelopes, and every one of them has at some point
 * been quoted as a measurement.
 */
internal object SyncVocabulary {
    /**
     * The `terms` object of the `sync` section.
     *
     * Static — it describes the schema, not this deployment — so it is built
     * once and served from a `val`. It is ~2KB on a document already measured in
     * tens, and it is the difference between a chart a stranger can read and one
     * only this repository can.
     */
    val TERMS: JsonObject =
        buildJsonObject {
            put(
                "scope",
                "Every count in this section is per STREAM, and a stream is one configured ask. " +
                    "One relay reached by two streams is two rows with two independent verdicts, and them disagreeing " +
                    "is not a contradiction: each stream walks that relay at its own moment and to its own depth, " +
                    "and neither may resume from the other's claim.",
            )
            put(
                "returned",
                "APPROACH ONLY. A fan-out leg that started and came back — including one that came back unreachable, " +
                    "capped, or out of budget. It is not progress, not coverage, and not success; it is the cheapest " +
                    "thing to count and the most often misread. What became of each leg is in progress.streams[].cycle.taken. " +
                    "Published per stream as `returned`, whose denominator is `urls.taken` — the same set by construction.",
            )
            put(
                "settled",
                "Nothing outstanding below the span this stream has walked on this relay — published as `complete` on a " +
                    "row and counted as `reconciled`. Earned two ways: a finished negentropy reconcile, or a paged walk " +
                    "that drained (the relay EOSE'd an empty page, so there is nothing older). It says nothing about HOW.",
            )
            put(
                "open",
                "Walked, but not proven exhaustive — published as `paged`. The stream asked and stored what came back; " +
                    "the relay may hold more below where the walk stopped.",
            )
            put(
                "walkEnvelope",
                "`min`/`max` on a row: the outer edges of the created_at span this stream has walked on this relay, " +
                    "across every kind in the filter together. AN ENVELOPE, NOT COVERAGE — a long-lived kind's events " +
                    "set the edges for the short-lived kinds beside it.",
            )
            put(
                "evidence",
                "`everyKindMin`/`everyKindMax`: the part of the envelope in which EVERY kind in the filter has actually " +
                    "produced an event. Present only when it is narrower than the envelope. Still not a coverage claim in " +
                    "the other direction either — a kind whose floor sits higher may simply have started existing later, " +
                    "which is indistinguishable here from a walk that stopped.",
            )
            put(
                "holdings",
                "NOT PUBLISHED HERE, and the distinction that has cost the most. A band is walk STATE — where this stream " +
                    "has asked — and the store's own contents are a different question, answered by the corpus and kinds " +
                    "sections. A band floor newer than the oldest stored event is ordinary: another stream, or an earlier " +
                    "config, put those events there.",
            )
            put(
                "frame",
                "APPROXIMATE. `from`/`to` are the drawing frame, not a target: these filters carry no `since`, so " +
                    "\"100% covered\" is not a defined quantity. `from` is the deepest point anything in this document " +
                    "reaches, and it exists to make two relays comparable rather than to grade either.",
            )
            put(
                "relays",
                "Relays TOUCHED — never the relays configured. A dynamic stream discovers its list, so there is no " +
                    "configured denominator to publish and \"never asked\" is not knowable from here. On a stream it counts " +
                    "that stream's relays; at the top of the section it counts DISTINCT relays across every stream, which " +
                    "is the smaller number and the only one that describes the network rather than the work. " +
                    "Inside `foldedOnto` and `inFlight` the same name is a LIST rather than a count — those two publish " +
                    "urls, and each is bounded with its own `omitted`.",
            )
            put(
                "rows",
                "Rows in the section — one per (stream, relay). Always at least `relays`, and larger whenever a relay is " +
                    "walked by more than one stream. Summing the streams' own `relays` gives this, not `relays`.",
            )
            put(
                "hosts",
                "Distinct authorities behind those urls. Most relay software answers on every path, so one server can wear " +
                    "dozens of urls and every url-keyed count is inflated until the alias fold decides them. Arithmetic over " +
                    "strings, not the fold's verdict — available immediately, and honest about being that. Inside " +
                    "`undecided` it is the same unit for the same reason: what a probe pass decides is a SERVER, and one " +
                    "row per url would say the same thing forty times.",
            )
            put(
                "legs",
                "How many (filter, relay) bands were folded into a group — present only when a narrow asked one relay more " +
                    "than once. The other counts are per RELAY, and a relay counts as settled only when every leg of it did.",
            )
            put(
                "sweeping",
                "Relays with a live reconcile cursor: mid-walk right now. A sweep killed at 80% records no band at all, so " +
                    "without this a relay in progress is indistinguishable from one nobody has touched.",
            )
            put(
                "unnamed",
                "A group read from state written before the file nested by stream. Its keys name no stream, so it has no " +
                    "identity beyond its filter. Not a second mirror and not an error — it disappears once every deployment " +
                    "has booted on a build that writes the nested shape.",
            )
            put(
                "discovered",
                "Urls a cycle was handed by discovery, before anything was dropped. The whole of `urls`, and " +
                    "`discovered = foldedOntoAnother + refusedUnstable + excluded + taken` exactly.",
            )
            put(
                "relayListAgeSec",
                "How old the relay list this cycle fanned out over was when the cycle began. 0 means discovery ran for " +
                    "this cycle; anything else means the router reused the set a previous cycle derived, which it may do " +
                    "for up to that stream's refresh period. IT QUALIFIES `discovered`: on a recycling stream that count " +
                    "describes a store walk from this many seconds ago, so two consecutive documents carrying identical " +
                    "url counts are a mirror whose network has not been re-read, not necessarily one whose network has " +
                    "not changed. Says nothing about the dial decisions — the NIP-66 known-dead set and the host strikes " +
                    "are re-read every cycle whatever the list's age.",
            )
            put(
                "refusedUnstable",
                "Urls a stability pass MEASURED as unusable and removed from the fan-out: asked one filter twice, at a " +
                    "seven-day-old anchor, the relay gave two different answers. Not a claim about the operator and not " +
                    "an unreachable relay — it answers fine — but a server whose window is a different slice each time " +
                    "holds no stable cursor, so every cycle re-downloads what the last one already took. Distinct from " +
                    "`excluded`, which is an operator's instruction, and from `foldedOntoAnother`, which is a duplicate " +
                    "url: this one is our own signed measurement, carried as a `self-consistent` tag on the url's NIP-66 " +
                    "record and re-taken monthly, so a relay that is fixed rejoins on its own.",
            )
            put(
                "foldedOntoAnother",
                "Urls an alias verdict proved are the same server as another url in the same list, so they were never " +
                    "dialled. Their earlier band state is dropped rather than merged: a containment measurement is enough " +
                    "to stop dialling a duplicate and not enough to close the survivor's legs — which is also why a folded " +
                    "url has no row in the coverage section. Measured from the verdict map, not inferred from a subtraction.",
            )
            put(
                "foldedOnto",
                "WHICH urls folded, grouped by the survivor that absorbed them, with a couple of examples each. The " +
                    "biggest few only — the full list runs to thousands of urls, which is not publishable on a document " +
                    "fetched every poll — and `omitted` names how many survivors were left out, because a truncated list " +
                    "that does not say so reads as the whole answer. The full, per-url verdict is a signed NIP-66 kind " +
                    "30166 `same-as` record in this relay's own store, queryable over the protocol.",
            )
            put(
                "inFlight",
                "WHICH relays a stream has a worker on right now, longest-held first — the names behind `pending`, " +
                    "`busy` and the progress line's `running`, which were counts and nothing else. It spans passes, " +
                    "which is why it sits beside the cycle rather than inside it: a worker outlives the pass that " +
                    "handed it out, so the same url is this cycle's `pending` if this pass dialled it and its `busy` " +
                    "if an earlier one did. NOT \"the pending urls\": `pending` also counts urls the walk has not " +
                    "reached yet, which have no worker and are not here. Bounded to the longest-held few — the rest " +
                    "of a fan-out's workers are connect timeouts to dead hosts and are counted in `omitted`.",
            )
            put(
                "heldForSec",
                "How long a worker has held that relay, measured from the CLAIM — before the strike checks, the TCP " +
                    "pre-probe and the wait for a transfer slot, not just the download. On its own it says only that " +
                    "the leg is long; read it with `transferringForSec` (is it even on a socket) and `quietForSec` " +
                    "(is anything still arriving), which is what separates a relay with a real backlog from a walk " +
                    "that will not end.",
            )
            put(
                "transferringForSec",
                "How long that leg has held a TRANSFER SLOT — not how long it has been on a socket, because the " +
                    "websocket connect happens inside the slot and a url that never connects at all still holds one " +
                    "while it tries. ABSENT means it has not been admitted to the pool: it is in the guards (an " +
                    "earlier strike, our Tor proxy, the TCP pre-probe) or queued behind other legs, which is where " +
                    "most of a fan-out's workers are at any instant — 8 slots routinely carry 128 workers. Absent " +
                    "with a large `heldForSec` says OUR pool is saturated; present with a large one says a slot is " +
                    "committed to a transfer that is not finishing.",
            )
            put(
                "events",
                "Events that leg has received at the socket so far — its own count, where `received` is every leg of " +
                    "the cycle added together. Counted as they ARRIVE rather than when the leg ends, because the leg " +
                    "worth watching is the one that has not ended. Inside `rejections` the same name counts the " +
                    "events refused for one reason, which is the only other place this document counts events.",
            )
            put(
                "quietForSec",
                "How long since that leg last received anything, or since it was claimed if it never has. THE ONE " +
                    "THAT DECIDES what a long-held slot means: events still landing is a relay with a backlog and a " +
                    "slot well spent, this number climbing is a walk that is not going to end. Both look identical " +
                    "from the durations alone.",
            )
            put(
                "omitted",
                "How many rows a bounded list left out. Never silent and never zero by omission: these lists run to " +
                    "thousands of urls on a document fetched every poll, and a truncation that does not disclose " +
                    "itself reads as the whole answer.",
            )
            put(
                "excluded",
                "Urls dropped by CONFIG after the fold — a stream's `exclude` list, or this relay's own url, which is in " +
                    "plenty of other people's relay lists. Its own member because an operator's instruction being obeyed " +
                    "and a duplicate the router worked out for itself are different facts with different fixes; they were " +
                    "one number while the fold count was inferred from a subtraction.",
            )
            put(
                "taken",
                "Urls the cycle is responsible for, and the total the ten outcomes under it sum to. `pending` is derived " +
                    "from the other nine, which is what keeps the partition closed while the cycle is still running.",
            )
            put(
                "busy",
                "Not dialled because a worker from an EARLIER pass was still syncing that relay when this one came round. " +
                    "The router walks its relay list handing work to a fixed pool and does NOT wait for the pool to empty " +
                    "before walking again — one relay that takes hours costs one slot instead of stopping the mirror — so " +
                    "passes overlap and a relay slower than a pass is dialled every other pass. That is the rotation " +
                    "working. Read it against `pending` and `outcome`: a `completed` pass routinely ends with urls still " +
                    "in flight, which is the tail of the pool rather than a cycle that was killed. And do not read it as a " +
                    "verdict about the relay — `hostStruckOut` and `knownDead` are also 'not dialled' and are the opposite " +
                    "kind of fact.",
            )
            put(
                "delivered",
                "Reached, and it had events this store did not.",
            )
            put(
                "nothingNew",
                "Reached and answered cleanly with nothing new — a working relay this mirror is already in sync with. " +
                    "Not a failure.",
            )
            put(
                "unreachable",
                "Never answered. The only outcome this relay publishes anything about (a signed NIP-66 record), and " +
                    "deliberately the narrowest: an unknown failure stays quiet.",
            )
            put(
                "transferFailed",
                "Answered the handshake, then the transfer broke. NOT published and NOT struck out — the server was there, " +
                    "so calling it unreachable would be a false statement about someone else's machine.",
            )
            put(
                "noRoute",
                "The TCP pre-probe was refused or the name did not resolve, so no websocket was opened. This is what most " +
                    "of a large fan-out's shrinkage is: dead urls in old relay lists.",
            )
            put(
                "hostStruckOut",
                "Not dialled because a sibling url on the same authority failed enough times DURING THIS CYCLE to strike " +
                    "the host out. Nothing about a strike persists: the url is dialled again on the very next cycle, so " +
                    "the retry interval is the stream's refresh interval. Distinct from knownDead, which is the durable one.",
            )
            put(
                "knownDead",
                "Not dialled because an EARLIER run published a signed NIP-66 unreachability record for it that is still " +
                    "within its TTL (24h by quartz's default), so this cycle skipped it without asking. It comes back when " +
                    "the record ages out — or immediately, if anything else on its host delivers. These two — knownDead and " +
                    "hostStruckOut — were one number called \"skipped as dead\", which answered \"will it try again, and " +
                    "when\" in two opposite ways under one label. On the `reachability` processor the same name is the " +
                    "SET rather than one cycle\'s count: how many urls currently carry such a record, which is what " +
                    "makes this outcome explicable instead of mysterious.",
            )
            put(
                "torUnavailable",
                "Not dialled because OUR Tor proxy was not answering. A fact about this container, never about their relay, " +
                    "and nothing is published about these.",
            )
            put(
                "pending",
                "READ IT AGAINST `inFlight`, not against `outcome` alone. Derived, never counted: it is `taken` minus the " +
                    "nine terminal outcomes, which is what makes the partition add up mid-cycle. A `completed` cycle with a " +
                    "large `pending` is NOT a cycle that died — a pass ends when its last url is handed out, not when its " +
                    "last worker returns, so the tail of the pool is still running and `inFlight` names it (measured on a " +
                    "live run: 285 pending, every one of them a live worker, three of them downloading at 20k events each). " +
                    "The other reading is the one where `inFlight` is empty or absent: those urls never reached a verdict " +
                    "because the cycle stopped first, and they are dialled again on the next one. A router killed mid-fan-out " +
                    "is that case with the last file still saying `running`, which `staleForSec` is what gives away.",
            )
            put(
                "received",
                "Events this stream received from upstreams this cycle, counted at the socket. NOT the same as the events " +
                    "the store gained: ingest drops the copies other relays already delivered and the versions it already " +
                    "holds, so this is always the larger number, and the two disagreeing is not a fault in either.",
            )
            put(
                "holding",
                "A `phase` value, and the one that is easy to misread as idle. The stream's refresh interval came " +
                    "round and it DECLINED to start the next pass, because fewer than half its transfer slots were " +
                    "free — a pass started against a full pool cannot download anything, it can only walk the relay " +
                    "list and queue. Seconds of it is the rotation breathing between passes. Minutes or hours of it " +
                    "is a stream whose pool never frees up, and `inFlight` names the leg holding it.",
            )
            put(
                "owner",
                "Which half of the router opened a pass: `static` (a configured `urls` backfill) or `dynamic` (a " +
                    "`relaySource` fan-out). ONE stream name can carry both and they run at once — so without this a " +
                    "reader has two partitions under one name and no way to tell which is which.",
            )
            put(
                "outcome",
                "`running`, `completed` or `failed` for the cycle. Published because a cycle that aborted at 80% and one " +
                    "that finished left the identical trace: both simply stopped saying anything.",
            )
            put(
                "accountedFor",
                "Whether the partition actually holds in THIS document — `discovered = foldedOntoAnother + " +
                    "refusedUnstable + excluded + taken`, and the ten outcomes summing to `taken`. False is published rather than hidden: the counts are still worth " +
                    "having, and this is what stops a reader treating a broken partition as a whole one. `balanced` beside " +
                    "it is the router's own check, kept separate so a disagreement localises the fault.",
            )
            put(
                "passesRun",
                "How many passes a processor has run since this process started. Its own name because a stream's " +
                    "`passes` is a LIST of walks, and one word for a list and a count is exactly the overload this " +
                    "document exists to stop making.",
            )
            put(
                "passes",
                "The list of WALKS a stream still has worth reporting. On a stream it is the list of walks still worth " +
                    "reporting: a walk ends when its last url is handed out, not when its last worker returns, so a " +
                    "rotation normally has the previous pass finishing while the new one hands out — two rows, each " +
                    "with its own partition, and the older one's `pending` is exactly the legs still going. Absent " +
                    "when there is only one, which `cycle` already carries. A processor's count of passes is " +
                    "`passesRun`, deliberately a different word.",
            )
            put(
                "processors",
                "The router's work that is NOT a stream, and the answer to \"what else is running\". Six of them: the " +
                    "alias fold and the stability gate (both on the alias monitor's own six-hour clock, both writing " +
                    "tags onto the same NIP-66 kind-30166 records), the NIP-66 reachability monitor (quartz's, which " +
                    "watches every socket this client opens rather than dialling on a schedule), ingest, the healer " +
                    "and the upstream push. A processor that is not registered is one this router does not run — a " +
                    "deployment with no signer has no fold and no monitor at all — so an absent row is a fact rather " +
                    "than missing data.",
            )
            put(
                "candidates",
                "Urls a stream handed a processor's pass, before anything was decided — the router's own word for the " +
                    "argument it passes (`measure(label, candidates, …)`). The denominator `unmeasured` is counted " +
                    "against, and NOT the number that was dialled: most of a candidate set already carries a current " +
                    "verdict and is never asked again until it ages out.",
            )
            put(
                "unmeasured",
                "THE PROGRESS NUMBER for a processor: how many of its `candidates` still have no verdict after the pass " +
                    "that just ran. Falling pass over pass is the fold getting somewhere; standing still while " +
                    "`dialled` climbs is a set of hosts that cannot be decided, and `undecided` says which and why. " +
                    "Zero is the state both probe passes are working towards — every url measured, nothing left to " +
                    "ask — and it is reached and held for most of a monthly TTL. NOT the complement of `dialled`: that " +
                    "one counts what a single pass spent, this one what the whole candidate set still lacks.",
            )
            put(
                "dialled",
                "Dials that pass spent: fingerprints for the alias fold, paired walks for the self-consistency gate. " +
                    "Capped per pass, which is why `unmeasured` falls in steps rather than to zero — the cap is what " +
                    "keeps a first pass over a polluted store from becoming one enormous probe run.",
            )
            put(
                "decided",
                "New verdicts that pass reached AND published. High `measured` with `decided` at zero is a pass that " +
                    "dialled and learned nothing, which is a real and recoverable state — see `undecided`.",
            )
            put(
                "undecided",
                "WHICH HOSTS a probe pass left with nothing written down, grouped by why. Five causes end a group " +
                    "that way — out of probe budget, cooling down from an earlier failed pass, declined by our own " +
                    "transport, no url that could be a yardstick, nothing to hold up against one — plus a host that " +
                    "cannot repeat itself, and only that last one never recovers on its own. From outside the process " +
                    "all of them are the same silence: a url still being dialled beside eleven siblings that folded. " +
                    "`hosts` counts them and `examples` names a few, bounded with its own `omitted`.",
            )
            put(
                "lastPassAt",
                "When a processor's last pass ENDED, whatever it achieved and whether or not it threw. Beside " +
                    "`nextInSec` it is what separates a pass that is failing every time from one that stopped running " +
                    "at all: the first keeps this moving, the second freezes it while the phase stays `measuring`.",
            )
            put(
                "lastPassSec",
                "How long that pass took. Published beside the timestamp because these are not quick: a first fold " +
                    "over a polluted store dials thousands of relays and runs for a quarter of an hour, and a pass " +
                    "still running is the ordinary reason the fold has said nothing about a host yet.",
            )
            put(
                "nextInSec",
                "Seconds until the processor's next pass. The alias monitor's clock is six hours by default, so " +
                    "\"the fold has decided nothing about this host\" reads as broken until you know the next turn is " +
                    "four of them away. Measured from when the last pass finished, not from a constant — a pass with " +
                    "nothing submitted retries in a minute, and a long pass pushes the next one back by its own length.",
            )
            put(
                "queued",
                "Items waiting in a processor's queue right now — a DEPTH, on both of the queues that publish it. For " +
                    "ingest, read it against `capacity`: full means ingest is the limit and every download is " +
                    "backpressured behind it, empty means the limit is upstream of ingest. Those are opposite " +
                    "findings and the depth alone tells them apart only against the ceiling. The healer's queue has " +
                    "no ceiling to publish: it coalesces and drops instead of growing, which is what `dropped` counts.",
            )
            put(
                "dropped",
                "Repairs the healer's queue threw away rather than backpressure the sweep that found them — it " +
                    "coalesces and drops on purpose, because a dropped heal is a retry and a stalled sweep is not. " +
                    "Published because a silent drop is the one thing a bounded queue must never do.",
            )
            put(
                "capacity",
                "How deep the ingest queue can go before submitting blocks. Derived from the configured batch size, " +
                    "and the denominator `queued` only means something against.",
            )
            put(
                "accepted",
                "Events ingest wrote to the store since this router started. ALWAYS smaller than the streams' " +
                    "`received`, and not by a fault: a mirror is offered the same event once per relay holding it, " +
                    "and dropping the copies before verifying them is the point of the pipeline.",
            )
            put(
                "rejected",
                "Events ingest refused since boot — duplicates, superseded versions, bad signatures, events the " +
                    "store would not take. A large number here is the ordinary shape of a wide fan-out, not damage.",
            )
            put(
                "pushed",
                "Events this router SENT to somebody else's relay: repairs for the healer, and whatever an upstream " +
                    "configured `dir = up` was missing for the push. The only two paths that write outward.",
            )
            put(
                "observed",
                "Relays the NIP-66 monitor holds an observation for — every url this client has opened a socket to, " +
                    "whatever came of it. The set it could publish a record about, and the denominator `knownDead` is " +
                    "a part of.",
            )
            put(
                "reached",
                "THE ONE THAT SHOWS A DEEP WALK MOVING. The oldest created_at a stream's paged walks have reached so " +
                    "far, as a timestamp. On an unbounded walk `fraction` rounds to zero for hours while this date " +
                    "moves every page, so it is the only live evidence that a leg going back through years is going " +
                    "anywhere. It is the running counterpart of a band's floor and sits on the same axis: where the " +
                    "row under Walked ends is where this walk currently is.",
            )
            put(
                "fraction",
                "APPROXIMATE, and of the WINDOW rather than of the work. How much of the time span its paged relays " +
                    "are walking has been covered, 0 to 1, averaged over the walks still running. A finished walk " +
                    "stays in the denominator on purpose — removing it made the percentage run backwards as fast " +
                    "relays drained.",
            )
            put(
                "etaSec",
                "An estimate, from the rate the walks have averaged so far, of how long the paged half of this pass " +
                    "has left. It says nothing about the relays that reconcile rather than page, and nothing about " +
                    "how long the stragglers of the last pass will take.",
            )
            put(
                "running",
                "Relays this stream has a WORKER on right now — probing, in the guards, queued for a transfer slot " +
                    "or transferring — across every pass, including legs handed out by a pass that has ended. It is " +
                    "how much of the admission gate is committed, and it is much wider than `transferring`: a stream " +
                    "with 8 transfer slots routinely shows 128 workers, of which 120 are deciding whether a dead host " +
                    "is worth dialling. Reporting one as the other overstated the work five-fold.",
            )
            put(
                "transferring",
                "…and of those, how many hold a TRANSFER SLOT. The slot, not the socket: the websocket connect happens " +
                    "inside it, so a url that never connects at all counts here while it tries. The gap between this " +
                    "and `running` is where a fan-out's time actually goes.",
            )
            put(
                "slotsFree",
                "Transfer slots free while a stream is `holding` — the phase that most looks like a stall and could " +
                    "not say what it was waiting for. Read it against `slotsNeeded`: the next pass will not start " +
                    "until half the pool is free, because a pass started against a committed pool cannot download, " +
                    "it can only walk the relay list and queue.",
            )
            put(
                "slotsNeeded",
                "How many free slots this stream will not start a pass without — half its configured concurrency, " +
                    "rounded up.",
            )
            put(
                "collected",
                "Local ids walked so far while building the set a negentropy reconcile compares against, out of " +
                    "`collectedTotal` where the store could be counted. The most expensive thing this router builds " +
                    "and, for the minutes it takes, the phase that used to report nothing at all.",
            )
            put(
                "collectedTotal",
                "How many ids that snapshot expects, or absent when the store could not be counted — an unknown " +
                    "denominator is better than a wrong one.",
            )
            put(
                "retryInSec",
                "Seconds until a stream that is waiting or has FAILED tries again. On a failure it is a backoff that " +
                    "doubles up to the stream's refresh interval; on a wait it is fixed.",
            )
            put(
                "reason",
                "One cause, in the words the router's own log uses, so a reader meeting both does not have to work " +
                    "out that they are the same finding. On a stream it is what the last attempt threw — a `failed` " +
                    "stream published the word and nothing else. Inside `undecided` it is why a probe pass left hosts " +
                    "alone; inside `rejections` it is why ingest refused events.",
            )
            put(
                "pass",
                "Which walk handed a held url out. Passes overlap by design, so a stream routinely holds legs from " +
                    "two of them at once, and every clock on an in-flight row described the leg without ever saying " +
                    "which walk it belonged to. Absent from a router that predates the stamp.",
            )
            put(
                "fatals",
                "VirtualMachineErrors this router process has survived, almost always an OutOfMemoryError. It kills " +
                    "whichever thread allocates next and is caught by nobody, so the process carries on looking merely " +
                    "quiet — four of them once passed unnoticed while the phase lines still read healthy. Anything " +
                    "above zero means the router is DEGRADED and should be restarted; zero is published too, because " +
                    "a member that appears only on damage cannot be told from a router too old to say.",
            )
            put(
                "rejections",
                "What `rejected` is made of, biggest first. The largest number this document carries and the least " +
                    "readable without the split: a mirror is offered the same event once per relay holding it, so " +
                    "\"already have this\" dominating a 7.9M rejection count is the pipeline working exactly as " +
                    "designed. A bad-signature share that is not near zero is not.",
            )
            put(
                "lostToStore",
                "THE ONLY COUNTER HERE THAT MEANS DATA LOSS. Events that passed every check — new, verified, wanted — " +
                    "and could not be written: good events, gone. Anything above zero is a store problem to chase " +
                    "(most often a schema the events no longer fit), not a mirror one.",
            )
            put(
                "staleForSec",
                "How long ago the router last wrote its progress file, measured against THIS rollup's clock. The router " +
                    "rewrites it every tick whatever its streams are doing, so this is a heartbeat: a quiet mirror and a " +
                    "stopped one are the same document without it. Anything past a few minutes means the sync process is " +
                    "not running.",
            )
        }
}
