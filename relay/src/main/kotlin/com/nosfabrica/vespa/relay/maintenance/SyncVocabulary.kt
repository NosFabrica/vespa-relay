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
                    "thing to count and the most often misread. What became of each leg is in progress.streams[].cycle.taken.",
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
                    "is the smaller number and the only one that describes the network rather than the work.",
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
                    "strings, not the fold's verdict — available immediately, and honest about being that.",
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
                    "`discovered = foldedOntoAnother + excluded + taken` exactly.",
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
                "excluded",
                "Urls dropped by CONFIG after the fold — a stream's `exclude` list, or this relay's own url, which is in " +
                    "plenty of other people's relay lists. Its own member because an operator's instruction being obeyed " +
                    "and a duplicate the router worked out for itself are different facts with different fixes; they were " +
                    "one number while the fold count was inferred from a subtraction.",
            )
            put(
                "taken",
                "Urls the cycle is responsible for, and the total the nine outcomes under it sum to. `pending` is derived " +
                    "from the other eight, which is what keeps the partition closed while the cycle is still running.",
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
                    "when\" in two opposite ways under one label.",
            )
            put(
                "torUnavailable",
                "Not dialled because OUR Tor proxy was not answering. A fact about this container, never about their relay, " +
                    "and nothing is published about these.",
            )
            put(
                "pending",
                "READ IT AGAINST `outcome`. Derived, never counted: it is `taken` minus the eight terminal outcomes, which " +
                    "is what makes the partition add up mid-cycle. While the cycle is `running` these urls are genuinely in " +
                    "flight. On a cycle that is `completed` or `failed` — or on a router that was killed mid-fan-out, where " +
                    "the last file written still says `running` and `staleForSec` is what gives it away — a non-zero " +
                    "`pending` means the opposite: nothing is in flight, these urls never reached a verdict because the " +
                    "cycle ended first. They are dialled again on the next one.",
            )
            put(
                "received",
                "Events this stream received from upstreams this cycle, counted at the socket. NOT the same as the events " +
                    "the store gained: ingest drops the copies other relays already delivered and the versions it already " +
                    "holds, so this is always the larger number, and the two disagreeing is not a fault in either.",
            )
            put(
                "outcome",
                "`running`, `completed` or `failed` for the cycle. Published because a cycle that aborted at 80% and one " +
                    "that finished left the identical trace: both simply stopped saying anything.",
            )
            put(
                "accountedFor",
                "Whether the partition actually holds in THIS document — `discovered = foldedOntoAnother + excluded + " +
                    "taken`, and the nine outcomes summing to `taken`. False is published rather than hidden: the counts are still worth " +
                    "having, and this is what stops a reader treating a broken partition as a whole one. `balanced` beside " +
                    "it is the router's own check, kept separate so a disagreement localises the fault.",
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
