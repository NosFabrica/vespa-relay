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
package com.nosfabrica.vespa.relay.progress

import kotlinx.serialization.json.JsonArray
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
 * no term is the state this exists to end. `StatusVocabularyTest` pins both
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
object StatusVocabulary {
    /**
     * Every member either status document can publish, defined.
     *
     * Static — it describes the schema, not this deployment — so it is built
     * once and served from a `val`. Ship it through [termsFor] rather than
     * whole: it is ~2KB, and each document publishes about half of it.
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
                "inFlight",
                "WHICH relays a stream has a worker on right now, QUIETEST FIRST — the names behind `pending`, " +
                    "`busy` and the progress line's `running`, which were counts and nothing else. It spans passes, " +
                    "which is why it sits beside the cycle rather than inside it: a worker outlives the pass that " +
                    "handed it out, so the same url is this cycle's `pending` if this pass dialled it and its `busy` " +
                    "if an earlier one did. NOT \"the pending urls\": `pending` also counts urls the walk has not " +
                    "reached yet, which have no worker and are not here. WHOLE, not a top-N: a row is a worker " +
                    "holding a socket, so this list is bounded by the pool's `visitConcurrency` and \"what is this " +
                    "mirror connected to\" is answerable from it. Quietest first because that and not age is what a " +
                    "wedged leg looks like — a relay held an hour while it streams two million events is this router " +
                    "working. One visit serves every stream's asks in turn, so a relay appears under whichever " +
                    "stream it is on AT THIS INSTANT: a cheap stream showing few rows beside an expensive one is " +
                    "them sharing workers, not that stream running out of relays.",
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
                "HOW LONG SINCE THE LAST THING THIS ROW COUNTS ARRIVED, on both of the rows that publish it. On a " +
                    "stream leg it is events: how long since that leg last received one, or since it was claimed if " +
                    "it never has, and it is THE ONE THAT DECIDES what a long-held slot means — events still landing " +
                    "is a relay with a backlog and a slot well spent, this number climbing is a walk that is not " +
                    "going to end. On a processor\'s `measuring` it is units of the pass: how long since one last " +
                    "ENDED, which is what tells a pass about to finish from one that has stopped. `etaSec` cannot — " +
                    "it is honest arithmetic on the rate so far, so a pass whose last url has wedged reports `0` and " +
                    "every number on the row agrees with every other one. A production fitness pass read `12373 of " +
                    "12374, ~0s left` for 74 minutes.",
            )
            put(
                "stage",
                "WHICH STEP a probe pass is on for the url it is holding, in the pass\'s own words — `pre-probe`, " +
                    "`nip-11 document`, `ask ladder`, `neg-open` for fitness, `paired walk` for the stability gate, " +
                    "`fingerprint` for the fold. `heldForSec` beside it says how long; only this says what for, and " +
                    "the steps stall for unrelated reasons: a name that will not resolve stalls the pre-probe, a " +
                    "relay that never stops sending stalls the ladder, a full ingest queue stalls whichever step is " +
                    "delivering. It is the stream leg\'s `doing` for a job that is a ladder rather than a transfer.",
            )
            put(
                "doing",
                "WHAT THAT LEG IS ACTUALLY DOING, which the three clocks beside it cannot say. `held 2h 15m, 3 " +
                    "events` has four readings that want opposite responses: the worker is in the guards deciding " +
                    "whether the host is worth dialling; it cleared them and is queued behind OUR OWN transfer pool; " +
                    "it is reconciling, where a long silence is negentropy computing a difference; or it is paging, " +
                    "where the same silence is a walk that has stopped delivering. `transferringForSec` separates " +
                    "the first two from the rest and nothing separated the last two. Absent until a leg reaches a " +
                    "stage worth the word. A working leg's word names TWO things, because they do not imply each " +
                    "other: what for — `catching up` is everything new since this relay's last pass, `auditing` is " +
                    "the whole past re-checked on the stream's `negentropySyncThePastSeconds` clock to find what no " +
                    "catch-up ever " +
                    "saw — and, in the bracket, how: `paging` walks a REQ newest-first, `negentropy` compares " +
                    "reconciliation windows and downloads only the difference. Negentropy is NOT a synonym for the " +
                    "audit and paging is not a synonym for the catch-up: an audit pages the windows a peer will not " +
                    "reconcile, and a static stream backfills either way. This pool pages its catch-up and " +
                    "reconciles its audits, which is a fact about the pool and not about the words.",
            )
            put(
                "pagingUntil",
                "WHERE IN TIME a paging leg has REACHED — the oldest `created_at` it has actually received, walking " +
                    "newest-first towards the filter's floor. Updated per EVENT, not per page: a page boundary is " +
                    "the only other moment the cursor could be read, and a leg inside its first page has not " +
                    "crossed one, so this used to sit at the second the walk OPENED at for however long that page " +
                    "took — every fresh leg reporting `back to <today>` whether it was streaming a backlog or " +
                    "receiving nothing at all. It now moves with the events, so a leg that has been running for " +
                    "minutes and still names its own start date has received nothing in that window. " +
                    "`doing: catching up (paging)` beside a long `quietForSec` is two different legs from here: one deep in a " +
                    "real backlog and working its way down, and one whose cursor is not moving at all. Read this " +
                    "twice and they separate — it either advanced or it did not. The stream's own `reached` cannot " +
                    "answer it, being the MINIMUM over every live walk: one date describing the deepest leg, while " +
                    "the row beside it is named precisely because it is the exception. An AUDITING leg reports the " +
                    "same reading from a different mechanism: the OLDER edge of the negentropy window it is " +
                    "comparing right now, announced after the window has been cut to a size both sides will take, " +
                    "so it descends window by window the way the paged cursor descends event by event. Both are " +
                    "how far BACK the leg has got, never how far forward — the audit used to publish the window's " +
                    "newer edge here, which read as `back to <today>` for the whole of a sweep that had years left " +
                    "to compare. Absent on a leg that is neither paging nor auditing — one still in the guards has " +
                    "no cursor at all — which is a state and not a gap.",
            )
            put(
                "omitted",
                "How many rows a bounded list left out. Never silent and never zero by omission: these lists run to " +
                    "thousands of urls on a document fetched every poll, and a truncation that does not disclose " +
                    "itself reads as the whole answer. Present on unbounded lists too, reading 0 — `inFlight` is " +
                    "one — because a reader that finds the member missing cannot tell \"nothing was dropped\" from " +
                    "\"this router does not say\".",
            )
            put(
                "excluded",
                "Urls dropped by CONFIG — a stream's `exclude` list, or this relay's own url, which is in plenty of " +
                    "other people's relay lists. Its own member because an operator's instruction being obeyed and a " +
                    "duplicate the router worked out for itself are different facts with different fixes; they were " +
                    "one number while the fold count was inferred from a subtraction. Published in two places with the " +
                    "same meaning and different scopes: on a CYCLE it is after that stream's fold, and on a PROBE PASS " +
                    "it is over the union of every stream, where `sourced = excluded + heldOutDead + candidates`. " +
                    "`exclude` is per stream, so a url one stream excludes and another asks for counts as a candidate " +
                    "there — it is dialled, and counting it on both sides would break the one partition it belongs to.",
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
                    "is the case this cannot distinguish on its own, and the page's own liveness — it is served by this " +
                    "process — is what rules it out.",
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
                "processors",
                "The router's work that is NOT a stream, and the answer to \"what else is running\". Eight of them: " +
                    "the alias source (which walks the store for every url the relay lists name and hands the passes " +
                    "the set they work on — minutes, at the head of every sweep, and for all of them the rows below " +
                    "it read `idle`), the alias fold, the stability gate and the fitness pass (all three on the alias " +
                    "monitor's own six-hour clock, all three writing tags onto the same NIP-66 kind-30166 records), " +
                    "the rotating pool the visit-mode streams ride, ingest, the healer and the upstream push. A passive NIP-66 " +
                    "watcher used to be an eighth, signing a record per socket this client opened; the passes own the " +
                    "record now. A processor that is not registered is one this router does not run — a deployment " +
                    "with no signer has no fold and no monitor at all — so an absent row is a fact rather than " +
                    "missing data.",
            )
            put(
                "candidates",
                "Urls a stream handed a processor's pass, before anything was decided — the router's own word for the " +
                    "argument it passes (`measure(label, candidates, …)`). The denominator `unmeasured` is counted " +
                    "against, and NOT the number that was dialled: most of a candidate set already carries a current " +
                    "verdict and is never asked again until it ages out. Published in two places for one population: " +
                    "inside a pass's `streams` row it is what that pass was handed, and on the ALIAS SOURCE's row it " +
                    "is what the derivation yielded — `sourced - excluded - heldOutDead`, stated rather than left to " +
                    "a subtraction, because it is the number every count on the passes below is a share of.",
            )
            put(
                "newUrls",
                "…of those, how many arrived at the pass with NO verdict at all — the urls it exists to decide, and " +
                    "the number the card draws its position against (`143 of 1,754 new relay(s) checked for aliases`). " +
                    "`unmeasured` is the same population after the pass has run, so `newUrls - unmeasured` is what it " +
                    "decided. Counted against `candidates` instead, the position barely moves however much a pass " +
                    "achieves: most of a settled candidate set carries a verdict from weeks ago and is never asked " +
                    "again until it ages out. Absent on a pass that does not count it, where the card falls back to " +
                    "the whole candidate set.",
            )
            put(
                "unmeasured",
                "THE PROGRESS NUMBER for a processor: how many of its `candidates` still have no verdict after the pass " +
                    "that just ran. Falling pass over pass is the fold getting somewhere; standing still while " +
                    "`dialled` climbs is a set of hosts that cannot be decided, and `undecided` says which and why. " +
                    "Zero is the state both probe passes are working towards — every url measured, nothing left to " +
                    "ask — and it is reached and held for most of a monthly TTL. NOT the complement of `dialled`: that " +
                    "one counts what a single pass spent, this one what the whole candidate set still lacks. The CARD " +
                    "draws the count that HAS a verdict, which rises as the pass gets somewhere, so the two read in " +
                    "opposite directions and the number on screen is not this one. It also drops `foldedAway` from " +
                    "both halves where a row publishes it: a url the fold removed is one the stability gate never " +
                    "dials, so counting it as checked for consistency overstated that line by every duplicate in the " +
                    "corpus — 12,024 of 16,752 where the honest reading was 595 of 5,323.",
            )
            put(
                "dialled",
                "Dials that pass spent: fingerprints for the alias fold, paired walks for the self-consistency gate. " +
                    "A pass measures its whole candidate set — there is no per-pass total — so `unmeasured` falls to " +
                    "what could not be DECIDED rather than to what there was budget for. What is left after a pass " +
                    "is a host on a cooldown or one that cannot answer twice, and `undecided` names it. Urls a socket " +
                    "was actually opened for: one the transport declined costs no connection and is not counted here, " +
                    "though it IS counted under `unmeasured` and named in `undecided`.",
            )
            put(
                "sourced",
                "Every url the streams' relay lists yielded for a probe pass, before anything was held out — the " +
                    "widest number this router has about the network it can see, and the one `candidates` is a share " +
                    "of. Per PASS and over the union of every stream, so it is not the sum of the streams' own " +
                    "`discovered`: two streams routinely find the same url and it is one url here.",
            )
            put(
                "corpus",
                "The coverage tree's root: every relay url this router knows of, which is `sourced + recordedOnly` — " +
                    "what a relay list named this round, plus what only our own signed records still know about. Its " +
                    "own name because `sourced` means one of those two and the root means both: a row labelled " +
                    "\"everything this router knows of\" carrying `sourced`'s definition would document the wrong " +
                    "number for the widest row on the card. Not a published member — the page synthesises it from " +
                    "the two that are.",
            )
            put(
                "recordedOnly",
                "Urls this router holds a signed verdict record about that NO relay list named this round — the rest " +
                    "of the funnel's mouth, beside `sourced` rather than inside it, so the tree's root is " +
                    "`sourced + recordedOnly`. Not a drop and not a refusal: nothing was decided against these urls, " +
                    "they simply were not asked for, because the author who listed one revised their relay list or a " +
                    "source was reconfigured. The store still holds every measurement ever taken of them and the " +
                    "alias fold still groups a new url against them, which is why counting the corpus as one " +
                    "derivation's yield understated it. Zero on a router with no signer and no named monitors: it " +
                    "holds no records of its own.",
            )
            put(
                "heldOutDead",
                "…of those, how many carried a current signed unreachability record and were dropped before either " +
                    "probe pass saw them. `sourced - heldOutDead = candidates`. Not permanent: the record ages out " +
                    "(24h) or the host delivers something, and the url is back in the next derivation. Distinct from " +
                    "the monitor's own `knownDead`, which is the size of the whole dead set rather than its overlap " +
                    "with what the streams asked for.",
            )
            put(
                "foldedAway",
                "Urls of the candidate set an ALIAS FOLD has already taken out of the fan-out, and therefore the " +
                    "first slice of the partition: a folded url is never measured for stability, so it can carry no " +
                    "verdict here whatever it carried before it folded. First in precedence for that reason — a url " +
                    "that was measured and later folded counts once, here.",
            )
            put(
                "consistent",
                "Urls of the candidate set that currently carry a STABLE verdict: measured, and they answered one " +
                    "filter at a week-old anchor the same way twice. Standing state, not this pass's work — it " +
                    "includes every verdict read back from the store at boot, which is why it can be large beside a " +
                    "`decided` of zero. These are the urls the fan-out is allowed to hold a cursor against.",
            )
            put(
                "inconsistent",
                "…and the ones measured and FAILED: a url that answered the same filter two different ways. Every one " +
                    "of them is refused by every stream's fan-out, because a relay whose window is a fresh slice " +
                    "holds no stable cursor and every cycle re-serves what the last one took. The per-cycle count of " +
                    "the same finding is `refusedUnstable`. Expires with its record, so a server that is fixed " +
                    "rejoins on its own.",
            )
            put(
                "decided",
                "New verdicts that pass reached AND published. High `measured` with `decided` at zero is a pass that " +
                    "dialled and learned nothing, which is a real and recoverable state — see `undecided`.",
            )
            put(
                "undecided",
                "WHY a probe pass left urls with nothing written down, grouped by cause and summing to `unmeasured`. " +
                    "The FOLD ends a group five ways — cooling down from an earlier failed pass, declined by our own " +
                    "transport, no url that could be a yardstick, nothing to hold up against one, and a host that " +
                    "cannot repeat itself, of which only the last never recovers on its own. The STABILITY GATE ends " +
                    "a url seven ways, in two families: about us (declined by our own transport, the probe failed " +
                    "mid-walk) and about the far end (never answered a REQ, answered one of the two asks not both, " +
                    "refused our auth, answered but served no filter we know, too few events to judge on). " +
                    "From outside the process all of them are the same silence, which is what this exists to end — " +
                    "a corpus of dead urls being re-asked every six hours reads exactly like a gate that is stuck. " +
                    "`urls` counts them, `hosts` says how many servers those urls are, `examples` names a few, and " +
                    "`omitted` carries whatever either side dropped.",
            )
            put(
                "standing",
                "THE CORPUS's breakdown of `unmeasured`, where `undecided` is the last RUN's — two questions, and " +
                    "conflating them is what made this card unreadable. `undecided` is what the pass that just ran " +
                    "found: it covers the urls that run dialled, it is empty on a pass that ran over nothing, and a " +
                    "router running both a sweep and a fast lane publishes two of them over overlapping populations. " +
                    "This is one row per url with no verdict, read back from the kind-30166 records earlier passes " +
                    "signed — deduplicated by each record's own `d` tag, standing between passes for as long as the " +
                    "record does, and covering urls this run never reached. Sums to `unmeasured` like `undecided` " +
                    "does, with the urls the store knows nothing about under a row that says exactly that: it is not " +
                    "a reason, it is the absence of one, and it covers a url nothing has ever measured, one whose " +
                    "record aged out, and one whose only finding was about US rather than about the relay and was " +
                    "therefore never signed. Absent from a pass that reads no verdicts back — the alias fold.",
            )
            put(
                "whole",
                "Whether this row's standing counts — `foldedAway`, `consistent`, `inconsistent`, `unmeasured` — " +
                    "describe the WHOLE candidate set or a slice of it. The member that tells the two readings of a " +
                    "`streams` row apart: those four are counts over everything the pass was handed, while " +
                    "`dialled`, `decided` and `newUrls` are what one run of it spent. A SWEEP is handed the " +
                    "derivation's whole candidate set and says `true`; the FAST LANE is handed the urls named since " +
                    "its last look — a slice, every url of which the sweep also holds — and says `false`. Both rows " +
                    "are live at once, keyed by stream label, so a reader summing them counts the overlap twice: the " +
                    "card did, drawing 12,611 urls in reach under a round-up line reading 11,021 handed to the " +
                    "passes, and drawing every `undecided` reason twice. Merging them is no better — `hosts` cannot " +
                    "be added at all, since one server appears in both tallies. The corpus tree is drawn from the " +
                    "row that says `true`, and every row is drawn as its own pass block beside it.",
            )
            put(
                "tookSec",
                "How long THIS row's run took, where `lastPassSec` is how long the processor's last run took under " +
                    "whichever label it happened to be. The difference is the fast lane: a tick that finishes " +
                    "between two sweeps moves `lastPassSec` to describe the tick while the sweep's numbers are " +
                    "still the ones on the card. Absent from a pass that does not time itself.",
            )
            put(
                "accountedFor",
                "Do a probe pass's numbers still add up in THIS document — `candidates = foldedAway + consistent + " +
                    "inconsistent + unmeasured`, and the `undecided` rows covering every url with no verdict. " +
                    "Recomputed on this side rather than forwarded, so it describes what is being served rather " +
                    "than what the router believed, and a mismatch is published rather than hidden: the counts are " +
                    "still worth having and this is what stops a reader treating a broken partition as a whole one. " +
                    "Absent on a pass that publishes no partition — the alias fold measures no verdicts, and " +
                    "\"these add up\" is a claim about numbers that exist. The card draws the shortfall as its own " +
                    "`not accounted for` row.",
            )
            put(
                "parent",
                "The undecided reason this row REFINES, where it refines one. `never answered a REQ` is the largest " +
                    "thing a probe pass reports and it covers four findings with four different responses — a name " +
                    "that no longer resolves, a refused connection, a failed TLS handshake, and a window that lapsed " +
                    "in silence — so the rows for those name it here rather than sitting beside it as peers. The " +
                    "list stays FLAT and still sums to `unmeasured`: this is what lets a reader nest the rows " +
                    "without the arithmetic having to survive a tree on the wire, and a reader that ignores it still " +
                    "sees every url exactly once. Read off what the TRANSPORT said when it gave up; text the router " +
                    "cannot place is counted as unrecognised rather than forced into a bucket, and sampled to its " +
                    "log so the classification can be extended from real strings.",
            )
            put(
                "top",
                "The widest HOSTS under one undecided reason, ranked by how many of their urls ended there, " +
                    "with the name beside the count. It answers the question the pair `urls`/`hosts` raises and " +
                    "cannot settle: 3,902 urls on 2,201 hosts is either a dead network spread thin — no host above " +
                    "a dozen urls — or three servers wearing a thousand urls each, and those are opposite findings " +
                    "wanting opposite responses. It also answers the OTHER question these rows get asked, which " +
                    "the shape alone never could: WHICH servers, since the reason a url will not fold or settle is " +
                    "a property of the server and the names are the half you can act on. Long enough to be that " +
                    "inventory rather than a sample of it. Still DELIBERATELY DOES NOT SUM to the reason's `urls`: " +
                    "it is a ranked head, and the remainder is the tail it is a head OF. The card draws that " +
                    "remainder as its own slice rather than closing the level, so a list that was cut can never " +
                    "read as the whole one.",
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
                "measuring",
                "WHERE THE PASS RUNNING RIGHT NOW HAS GOT TO, and the only number on a processor's row that moves " +
                    "while one runs — every other one describes the pass that ENDED. Present exactly while a pass is " +
                    "dialling. On a monitor SWEEP it stands where `nextInSec` would be, which the sweep unsets while " +
                    "it runs: a pass takes as long as it takes, so nothing has computed when the next one is due " +
                    "until this one returns. A FAST LANE pass carries both, and both are true — the lane is measuring " +
                    "the urls named since its last look while the sweep is still due when it says. Before this, a " +
                    "stability pass spent hours saying `measuring` and nothing else — no size, no position, no end.",
            )
            put(
                "attempted",
                "Units of the current pass that are BEHIND it, however they ended — including a url our own transport " +
                    "declined and a host with nothing to compare against. Not a success count: what a pass LEARNED is " +
                    "`decided`, and on a discovered corpus most of a pass is spent on urls that cannot be measured at " +
                    "all, so a position that only moved on success would sit still while the pass worked hardest.",
            )
            put(
                "toProbe",
                "How many units the pass now running set out to walk — the denominator `attempted` is a share of. NOT " +
                    "`candidates`: both probe passes drop every url already carrying a current verdict before dialling " +
                    "anything, so on a settled corpus this is a small fraction of the candidate set, and it is the " +
                    "ratio to watch while a pass runs.",
            )
            put(
                "unit",
                "What `attempted` and `toProbe` are counts OF, because the passes do not decide the same thing: the " +
                    "stability gate and the fitness pass answer about a `url`, the alias fold answers about a `host` " +
                    "and dials every url of one to do it. A fold position counted in urls would jump by 55 for one " +
                    "verdict and by 1 for the next. The alias source counts neither — its unit is a `source`, one " +
                    "configured relay-list block at a time, since how many urls the walk yields is the thing it is " +
                    "finding out and cannot be a denominator until it has.",
            )
            put(
                "rotating",
                "A `phase` value, and the one that says least without its numbers. The stream does not walk a relay " +
                    "list at all: its world is the monitor's `prime` grades and its engine is the visit pool, " +
                    "which turns over that roster — a catch-up visit, a history audit where due, then a live tail on " +
                    "the open socket — so there is no pass to be a phase OF and the phase simply lasts. `roster` and " +
                    "`tails` are what it is doing; a roster of ZERO is a stream waiting on the fitness pass to certify " +
                    "its first relay, which is the state that looked identical to a busy one.",
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
                "etaSec",
                "An estimate, from the rate the walks have averaged so far, of how long the paged half of this pass " +
                    "has left. It says nothing about the relays that reconcile rather than page, and nothing about " +
                    "how long the stragglers of the last pass will take.",
            )
            put(
                "reason",
                "One cause, in the words the router's own log uses, so a reader meeting both does not have to work " +
                    "out that they are the same finding. On a stream it is what the last attempt threw — a `failed` " +
                    "stream published the word and nothing else. Inside `undecided` it is why a probe pass left hosts " +
                    "alone; inside `rejections` it is why ingest refused events.",
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
                "bottleneck",
                "WHERE THE CONSTRAINT IS, decided by the router itself: `ingest` (the queue is full, so every " +
                    "download is backpressured behind it — the mirror is as fast as the store), `downloads` (the " +
                    "queue drains as fast as it fills, so the relays are the limit), `upstream` (nothing is arriving " +
                    "at all) or `mixed`. A full queue and an empty one are opposite diagnoses that look identical " +
                    "from every other number here, which is why this is one word rather than something to infer.",
            )
            put(
                "eventsPerSec",
                "Events reaching ingest per second, averaged over the last minute, across every stream. NOT a " +
                    "stream's `received`, which is counted at the socket before dedup — the two are counted at " +
                    "different points on purpose and disagreeing is not a fault in either.",
            )
            put(
                "heapUsedMb",
                "The router process's own heap, against `heapMaxMb`. Past about 90% this router is spending its " +
                    "time collecting rather than mirroring, and the negentropy id snapshot — the largest thing it " +
                    "builds — is what usually gets it there.",
            )
            put(
                "sockets",
                "Websockets open right now, against `socketCeiling` — the OkHttp dispatcher budget, which is the " +
                    "real concurrency limit for the whole router. At the stock 64 every stream's `concurrency` " +
                    "silently stopped meaning anything; it is 1,024 here, and a fan-out pressed against it is a " +
                    "mirror whose configured widths are fiction.",
            )
            put(
                "heapMaxMb",
                "What `heapUsedMb` is measured against — the JVM's maximum heap, which in the compose deployment is " +
                    "a percentage of the container's memory limit rather than a figure anyone set directly.",
            )
            put(
                "socketCeiling",
                "What `sockets` is measured against — the OkHttp dispatcher budget shared by every stream. An open " +
                    "websocket holds a slot for its whole life, so this and not any stream's `concurrency` is the " +
                    "router's real ceiling.",
            )
            put(
                "servingMs",
                "The relay's mean client read latency, which this router YIELDS to: past its threshold ingest " +
                    "deliberately slows so that mirroring does not cost the people reading. A mirror that is " +
                    "throttling itself politely and one that is stuck look identical from throughput alone, and " +
                    "this is the only number that tells them apart. Absent where no pressure feed is configured.",
            )
            put(
                "at",
                "The clock each sample in `series` was taken at, in epoch seconds. Published beside the values rather " +
                    "than an interval being assumed: the rollup cadence is an operator's env var, a restart leaves a " +
                    "hole, and a reader spacing points evenly over an uneven series draws a smooth line through a gap.",
            )
            put(
                "heapPct",
                "`heapUsedMb` as a percentage of `heapMaxMb`, sampled once per rollup into `series`. Derived here " +
                    "rather than left to the reader because a percentage is what compares across samples, and " +
                    "publishing both halves sixty times over to let a page divide them would cost the document " +
                    "three times as much for the same line.",
            )
            put(
                "series",
                "THE LAST HOUR of the four process gauges, one sample per rollup, so the levels above can be read as " +
                    "trends. Every gauge here is an instant and no question asked of one is answerable from a single " +
                    "reading: `heapUsedMb` alone says nothing, and climbing three points a minute says everything; a " +
                    "full `queued` is the constraint if it has been full for ten minutes and noise if it filled this " +
                    "second. A gap is a null, never a zero and never the previous value — \"the router said nothing\" " +
                    "and \"the router said none\" are different facts. Per-stream and per-processor series are " +
                    "deliberately absent: the alias fold runs on a six-hour clock, so an hour of samples would not " +
                    "contain one of its passes.",
            )
            // THE FITNESS PASS — the monitor's verdict funnel. Each member is
            // one value of the NIP-32 label it signs onto a relay's kind-30166
            // record, so the funnel here and the certificate a stream selects
            // on are the same numbers by construction.
            put(
                "prime",
                "Relays the monitor currently grades prime: reachable AND answering AND canonical AND " +
                    "consistent AND pageable AND readable by us, measured on the socket in one pass. This is the whole " +
                    "admission decision — a stream's relay list is the store query `#l = prime` and nothing else. " +
                    "Slow is not a refusal, empty is not a refusal, and a small message cap is not a refusal.",
            )
            put(
                "dead",
                "The transport itself said no: the name does not resolve, the connection was refused, TLS failed, or " +
                    "the websocket upgrade was turned down. The one refusal about a relay that is actually absent.",
            )
            put(
                "silent",
                "Connected, then nothing — no EOSE, no CLOSED, the idle window lapsed. Alive and saying nothing, " +
                    "which is not the same finding as dead and retries on its own terms.",
            )
            put(
                "alias",
                "Works fine, and is another record's relay: the fold proved this url and its canonical are one " +
                    "server. Syncing it would double every event, so the certificate goes to the canonical alone.",
            )
            put(
                "unpageable",
                "Ignores the `until` cursor, so a paged walk against it cannot terminate — the measured failure mode " +
                    "is ~5.5 pages a second forever. Refused outright in v1 rather than given tail-only treatment.",
            )
            put(
                "auth-refused",
                "Requires NIP-42 and rejected OUR key. A relay whose challenge our signer satisfies is certified like " +
                    "any open one — an auth wall we can clear is no wall.",
            )
            put(
                "restricted",
                "Answers only shaped queries this router cannot generally send — a group host demanding an `#h` tag, " +
                    "a filter service minting per-user paths. Probed with the same shaped-ask ladder the fold climbs " +
                    "before the verdict is written.",
            )
            // THE ROTATING POOL — the visit-mode streams' engine. A slot IS a
            // socket here, which is what these numbers being small and equal
            // to each other is evidence of.
            put(
                "roster",
                "Relays currently graded prime and in rotation: the pool's whole world, rebuilt from the " +
                    "monitor's records on half the tightest freshness bound. A relay the monitor stops certifying " +
                    "leaves the roster, its tail and its socket on the next rebuild. On a STREAM's row it is that " +
                    "stream's share — the roster relays carrying at least one of its asks — and zero there is a " +
                    "stream with nothing certified for it yet, not a stream that has stopped.",
            )
            put(
                "awaitingVisit",
                "Roster relays queued for a worker right now. NOT ingest's `queued`, which is a depth of events — " +
                    "this is a count of relays between visits, and most of the roster sits here between top-ups.",
            )
            put(
                "visiting",
                "Visits in flight this instant — and therefore sockets held by workers, because a visit-pool slot is " +
                    "a socket by construction. The number the old engine's `transferring` could never truthfully be.",
            )
            put(
                "tails",
                "Live subscriptions left open after a visit's catch-up, one per relay, carrying every wanting " +
                    "stream's filter. This is what \"constantly connected\" means: new events arrive the moment they " +
                    "exist, and the revisit only covers what a dropped tail missed. Bounded by the tail budget: past " +
                    "it a tail is EARNED, and the relay with more content lately takes the socket of the one that " +
                    "has delivered least. On a STREAM's row it counts the tails held on that stream's `roster` " +
                    "share; on the pool's row, every tail this router holds.",
            )
            put(
                "evictedTails",
                "Tails closed to give their socket to a relay with more content lately — the tail budget's rotation, " +
                    "counted since boot. An evicted relay is requeued promptly and falls back to the untailed revisit " +
                    "cadence; nothing about its certificate changes.",
            )
            put(
                "visitsRun",
                "Visits completed since boot — catch-up, audit where due, heal drain, tail. The pool's odometer, " +
                    "beside `roster` for the rotation rate.",
            )
            put(
                "auditing",
                "Audits RUNNING right now — the gauge beside `auditsRun`'s odometer. A deep history's audit " +
                    "holds its worker for minutes, and without this it was one unit of `visiting` that could " +
                    "not be told from a catch-up. The relay under audit is named in its stream's in-flight " +
                    "rows, stage `auditing history (negentropy)` — or `auditing the provider's own records " +
                    "(negentropy)` for the retraction comparison, which is the same clock and the same full-past " +
                    "sweep — with how far BACK it has reached.",
            )
            put(
                "auditsRun",
                "History reconciles run since boot: the windowed negentropy pass a stream's " +
                    "`negentropySyncThePastSeconds` schedules when a relay's last verified full pass ages out. Each " +
                    "relay's clock is its own, so this climbs as a trickle — roster over the period — never a herd.",
            )
            put(
                "auditsSkipped",
                "Reconciles NOT attempted because the monitor measured that relay as refusing a NEG-OPEN — the " +
                    "`nip77` verdict on the same signed 30166 record the roster admits it by. Both audits are " +
                    "negentropy end to end, so against such a relay the attempt cannot succeed, and it was made " +
                    "every six hours per ask forever because a failed audit advances no clock. What re-checks their " +
                    "past instead is the stream's `refetchThePastSeconds`, so this number climbing beside a stream " +
                    "with no such period is the router saying that stream's history is never re-read on those " +
                    "relays. UNMEASURED relays are not counted here: no verdict means the ask tries and finds out.",
            )
            put(
                "retracted",
                "Records deleted because the upstream that owns them stopped serving them — a NIP-85 provider's " +
                    "retracted scores, and nothing else — a provider's own profile is another stream's record and " +
                    "stays. The only number in " +
                    "the router that goes DOWN, decided only by a completed negentropy comparison on the audit's " +
                    "clock; a failed reconcile deletes nothing.",
            )
            put(
                "abortedVisits",
                "Visits ended early because the relay refused with nothing delivered — a CLOSED, an auth wall, a " +
                    "dead subscription. One bounded visit is all a wedged relay can cost; whether it stays on the " +
                    "roster is the monitor's next sweep's decision, not a retry ladder's.",
            )
            put(
                "poolReceived",
                "Events the pool's visits and tails have delivered to ingest since boot, counted at the socket. The " +
                    "same larger-than-stored caveat as every `received` on this card: ingest drops the copies other " +
                    "relays already delivered.",
            )
        }

    /**
     * The definitions [document] actually needs — every member it publishes,
     * at any depth, and no others.
     *
     * ## Why a subset rather than the whole map
     *
     * There are two status documents now, one per plane, and they publish
     * disjoint halves of this vocabulary. Shipping the whole thing in both
     * would put ~1KB of definitions for the mirror's members in the monitor's
     * document and vice versa — but the real cost is the claim it makes. A
     * glossary is a promise that the reader will find these numbers here, and a
     * definition for a member the document does not carry is the way that
     * promise rots into fiction. `StatusVocabularyTest` holds the other
     * direction: no published member without a term, checked against both
     * documents at once, because a term that has left one of them may still be
     * earning its place in the other.
     *
     * Walks the document rather than taking a list, for the same reason the
     * test does: a list is a third thing to keep in step with the two it
     * describes.
     */
    fun termsFor(document: JsonObject): JsonObject {
        val published = LinkedHashSet<String>()

        fun walk(o: JsonObject) {
            for ((member, value) in o) {
                published += member
                when (value) {
                    is JsonObject -> walk(value)
                    is JsonArray -> value.filterIsInstance<JsonObject>().forEach(::walk)
                    else -> Unit
                }
            }
        }
        walk(document)
        return JsonObject(TERMS.filterKeys { it in published })
    }
}
