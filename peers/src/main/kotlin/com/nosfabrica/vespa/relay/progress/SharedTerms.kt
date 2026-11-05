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

import kotlinx.serialization.json.JsonObject

/** The members both the mirror's and the monitor's documents publish: a processor row, and the store section. */
internal val SHARED_TERMS: JsonObject =
    glossary {
        term(
            "store",
            "WHICH STORE CALLS THIS PROCESS HAS OUTSTANDING, and whose they are — the half of a wedge every " +
                "other number here was missing. `oldestBatchSec` says two ingest workers have been inside a " +
                "batch pass for 794 seconds and cannot say WHAT CALL they are in, and a batch makes three " +
                "different ones against three different engine paths with three different remedies. At the root " +
                "beside `health` because it is a fact about the PROCESS: one registry covers the mirror and the " +
                "monitor, since they are one process against one store. Absent — rather than empty — on a " +
                "router too old to book its calls, which is not the same claim as \"nothing is outstanding\".",
        )
        term(
            "calls",
            "In `store`: " +
                "The store calls that are out right now, LONGEST-RUNNING FIRST — the opposite of a stream's " +
                "`inFlight` order and the same as a probe pass's `inFlight`, for the same reason: holding a " +
                "relay for an hour is how this mirror works, while a store call that has not come back is by " +
                "construction the anomaly. Nothing in this router ends one — the store's query client sets no " +
                "read timeout on purpose, because it cannot tell \"engine still matching\" from \"connection " +
                "dead\" — so these rows are the router saying so rather than something it is about to fix. " +
                "On an ingest stage: " +
                "How many times the stage was ENTERED, where the store timed it as a call. Present only for the " +
                "stages that are calls: a lock's `wait`/`hold` pair is booked from one pair of timestamps and " +
                "has no call count, and a mean over that denominator would be a fiction. `ms` alone cannot " +
                "separate one pathological call from a hundred thousand ordinary ones — the same 24 minutes, " +
                "two different faults and two different fixes.",
        )
        term(
            "caller",
            "WHO ASKED, in this router's own subsystem names: `ingest.dedup` / `ingest.versions` / " +
                "`ingest.write` (a batch pass's three calls, told apart at last), `visit.negentropy`, " +
                "`heal.resolve`, `audit.retraction`, `push.upstream`, `monitor.verdicts`, `monitor.publish`, " +
                "`source.relayLists`, `health.verdictSupply`. Every one is greppable straight back to the code that makes the call, " +
                "which a category word like \"probe\" would not be. `other callers` is the overflow bucket — a " +
                "folded tally, never a subsystem.",
        )
        term(
            "op",
            "WHAT WAS ASKED, named for the store method verbatim: `existingIds`, `newestVersions`, " +
                "`batchInsert`, `query`, `count`, `snapshotIdsForNegentropy`, `insert` and `delete`. " +
                "Verbatim rather than bucketed, because telling `count` from " +
                "`snapshotIdsForNegentropy` is the whole difference between a cheap sizing query and a read of " +
                "gigabytes of ids — and because a bucket is a word nobody can grep their way back from.",
        )
        term(
            "asked",
            "A SUMMARY of the filter the call carries — kinds, how many authors, how many ids, the window's " +
                "width — never the filter itself. The two asks that matter most here are the two that cannot be " +
                "published whole: an `existingIds` probe carries two thousand ids and a negentropy window " +
                "carries the corpus. `asked` and not `filter` on purpose: the coverage card publishes `filter` " +
                "as the filter OBJECT, echoed verbatim, and one word over two shapes is how a reader looks up " +
                "the wrong one.",
        )
        term(
            "issuedAt",
            "When the call was issued, in epoch seconds — so a row here can be lined up against the " +
                "`store call SLOW` lines in the log, which is where a wedge that happened while nobody was " +
                "watching leaves its only timeline.",
        )
        term(
            "elapsedSec",
            "How long this store call has been running. THE NUMBER on the row: seconds is the ordinary shape, " +
                "minutes means a round trip that is not coming back and the events counted in ingest's " +
                "`queued` are waiting on a worker that will not return for them. There is no partial-progress " +
                "number beside it and there cannot be — a store call delivers nothing until it delivers " +
                "everything, so a leg's `quietForSec` has no counterpart here.",
        )
        term(
            "outstandingAtIssue",
            "How many store calls this process already had out when this one was issued — the client-side half " +
                "of \"is the store slow, or is my request waiting in line\". The other half is a service-start " +
                "timestamp only the store can stamp, and it is not published anywhere. What this one settles: " +
                "the store's own request dispatcher is 1,024 wide, far above anything this router runs, so a " +
                "slow call issued with a handful outstanding did NOT queue on our side of the wire — which puts " +
                "the wait at the engine. Same inference an operator was making by hand, now from a measurement.",
        )
        term(
            "slowAfterSec",
            "How long a store call may run before the router names it in the log — `SYNC_STORE_SLOW_SEC`, and the " +
                "bound the status page marks a row at. Published rather than left for a reader to know, because " +
                "it is an operator's to change: a page carrying its own copy of the default would go on marking " +
                "rows a router set to five minutes considers ordinary, which is the drift the page avoids " +
                "everywhere else by never re-deciding a threshold the router already decided. Zero means the " +
                "operator turned the LOG off; it says nothing about the report, which costs nothing to keep.",
        )
        term(
            "callers",
            "One row per subsystem, whoever is holding the most first — the answer to \"whose requests are " +
                "filling the queue\", which no number in this document could give while every plane hit one " +
                "anonymous store. TWO KINDS OF NUMBER share the row and only one of them is addable: `issued`, " +
                "`returned`, `failed` and `cancelled` are LIFETIME counters, while `outstanding` and " +
                "`oldestOutstandingSec` are LIVE and come off the same instant `calls` and `ages` do — so " +
                "the callers' `outstanding` sums to the section's, exactly, always. " +
                "`issued - returned - failed - cancelled` is that same quantity a moment earlier and need NOT " +
                "equal it on a busy router, because a call that finished mid-read lands on one side and not the " +
                "other; it settles when the router goes quiet. Every member is published including its zeroes.",
        )
        term(
            "outstanding",
            "Store calls out right now — for the whole process at the top of the section, and for one " +
                "subsystem on a `callers` row. Read the pair: a large total with one caller holding all of it " +
                "names the subsystem to look at, and a large total spread evenly is a router asking the engine " +
                "for more than it can answer.",
        )
        term(
            "oldestOutstandingSec",
            "How long this caller's longest-running call has been out. Absent when the caller has nothing " +
                "outstanding, which is most of them most of the time — a zero would read as a call that just " +
                "started.",
        )
        term(
            "issued",
            "Store calls this router has made since it started — the whole process at the top of the section, " +
                "one subsystem on a `callers` row. A rate rather than a level: difference two page loads to " +
                "see how hard this router is leaning on the engine, and compare the callers' shares to see " +
                "which part of it is doing the leaning.",
        )
        term(
            "returned",
            "Store calls that came back WITH AN ANSWER since this router started. Against `issued` it is the " +
                "throughput of the whole store seam; the gap is `failed`, `cancelled` and whatever is still out.",
        )
        term(
            "failed",
            "Store calls that THREW. A different fault from a call that hangs, and it wants the opposite next " +
                "move: a store whose schema the events no longer fit fails here in milliseconds, where a store " +
                "that has stopped answering shows up as `elapsedSec` climbing with nothing failing at all. " +
                "Anything above zero on a healthy-looking router is worth chasing — most of ingest's own " +
                "failures are caught and retried at a lower level, so a count here is one that got past them.",
        )
        term(
            "cancelled",
            "Store calls the process STOPPED — shutdown, and a per-write deadline the monitor sets on its own " +
                "verdict edits. Counted apart from `failed` because a clean stop reported as a failure reads as " +
                "a store refusing work; it is the same reason the ingest probes rethrow cancellation rather " +
                "than swallowing it.",
        )
        term(
            "ages",
            "The outstanding calls BANDED BY AGE, in bands that partition them and sum back to `outstanding` " +
                "— which is what makes them a shape rather than six numbers. A thousand calls all under a " +
                "second is a busy router; eight hundred under a second with two past fifteen minutes is the " +
                "finding, and the total alone cannot tell those apart. Logarithmic on purpose: the question is " +
                "which ORDER OF MAGNITUDE a call is in, and even bands would put every healthy call in one row.",
        )
        term(
            "fromSec",
            "The floor of one age band — calls at least this old and younger than the next band's floor. The " +
                "last band is open-ended, which is where a wedged call lives.",
        )
        term(
            "hosts",
            "Distinct authorities behind those urls. Most relay software answers on every path, so one server can wear " +
                "dozens of urls and every url-keyed count is inflated until the alias fold decides them. Arithmetic over " +
                "strings, not the fold's verdict — available immediately, and honest about being that. Inside " +
                "`undecided` it is the same unit for the same reason: what a probe pass decides is a SERVER, and one " +
                "row per url would say the same thing forty times.",
        )
        term(
            "heldForSec",
            "How long a worker has held that relay, measured from the CLAIM — before the strike checks, the TCP " +
                "pre-probe and the wait for a transfer slot, not just the download. On its own it says only that " +
                "the leg is long; read it with `transferringForSec` (is it even on a socket) and `quietForSec` " +
                "(is anything still arriving), which is what separates a relay with a real backlog from a walk " +
                "that will not end.",
        )
        term(
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
        term(
            "omitted",
            "How many rows a bounded list left out. Never silent and never zero by omission: these lists run to " +
                "thousands of urls on a document fetched every poll, and a truncation that does not disclose " +
                "itself reads as the whole answer. Present on unbounded lists too, reading 0 — `inFlight` is " +
                "one — because a reader that finds the member missing cannot tell \"nothing was dropped\" from " +
                "\"this router does not say\".",
        )
        term(
            "excluded",
            "Urls dropped by CONFIG — an `exclude` list, or this relay's own url, which is in plenty of " +
                "other people's relay lists. Its own member because an operator's instruction being obeyed and a " +
                "duplicate the router worked out for itself are different facts with different fixes; they were " +
                "one number while the fold count was inferred from a subtraction. Published in two places with the " +
                "same meaning and different scopes: on a mirror CYCLE it is the stream's own `exclude`, applied " +
                "after that stream's fold; on a monitor PROBE PASS it is monitor.conf's single `exclude` over the " +
                "union of its sources, where `sourced = excluded + heldOutDead + candidates`. The two planes " +
                "declare their exclusions separately, and neither number narrows the other.",
        )
        term(
            "passesRun",
            "How many passes a processor has run since this process started. Its own name because a stream's " +
                "`passes` is a LIST of walks, and one word for a list and a count is exactly the overload this " +
                "document exists to stop making.",
        )
        term(
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
        term(
            "lastPassAt",
            "When a processor's last pass ENDED, whatever it achieved and whether or not it threw. Beside " +
                "`nextInSec` it is what separates a pass that is failing every time from one that stopped running " +
                "at all: the first keeps this moving, the second freezes it while the phase stays `measuring`.",
        )
        term(
            "lastPassSec",
            "How long that pass took. Published beside the timestamp because these are not quick: a first fold " +
                "over a polluted store dials thousands of relays and runs for a quarter of an hour, and a pass " +
                "still running is the ordinary reason the fold has said nothing about a host yet.",
        )
        term(
            "nextInSec",
            "Seconds until the processor's next pass. The alias monitor's clock is six hours by default, so " +
                "\"the fold has decided nothing about this host\" reads as broken until you know the next turn is " +
                "four of them away. Measured from when the last pass finished, not from a constant — a pass with " +
                "nothing submitted retries in a minute, and a long pass pushes the next one back by its own length. " +
                "On a `schedule` row it is the same reading over a different clock: seconds until the NEAREST " +
                "ask of that stream comes due for its audit or its re-fetch. Absent there means nothing is " +
                "waiting — every ask is already due — which is a state and not a zero countdown.",
        )
        term(
            "measuring",
            "WHERE THE PASS RUNNING RIGHT NOW HAS GOT TO, and the only number on a processor's row that moves " +
                "while one runs — every other one describes the pass that ENDED. Present exactly while a pass is " +
                "dialling. On a monitor SWEEP it stands where `nextInSec` would be, which the sweep unsets while " +
                "it runs: a pass takes as long as it takes, so nothing has computed when the next one is due " +
                "until this one returns. A FAST LANE pass carries both, and both are true — the lane is measuring " +
                "the urls named since its last look while the sweep is still due when it says. Before this, a " +
                "stability pass spent hours saying `measuring` and nothing else — no size, no position, no end.",
        )
        term(
            "attempted",
            "Units of the current pass that are BEHIND it, however they ended — including a url our own transport " +
                "declined and a host with nothing to compare against. Not a success count: what a pass LEARNED is " +
                "`decided`, and on a discovered corpus most of a pass is spent on urls that cannot be measured at " +
                "all, so a position that only moved on success would sit still while the pass worked hardest.",
        )
        term(
            "toProbe",
            "How many units the pass now running set out to walk — the denominator `attempted` is a share of. NOT " +
                "`candidates`: both probe passes drop every url already carrying a current verdict before dialling " +
                "anything, so on a settled corpus this is a small fraction of the candidate set, and it is the " +
                "ratio to watch while a pass runs.",
        )
        term(
            "unit",
            "What `attempted` and `toProbe` are counts OF, because the passes do not decide the same thing: the " +
                "stability gate and the fitness pass answer about a `url`, the alias fold answers about a `host` " +
                "and dials every url of one to do it. A fold position counted in urls would jump by 55 for one " +
                "verdict and by 1 for the next. The alias source counts neither — its unit is a `source`, one " +
                "configured relay-list block at a time, since how many urls the walk yields is the thing it is " +
                "finding out and cannot be a denominator until it has. THE FITNESS PASS SHOWS TWO IN TURN: `url` " +
                "while it dials, then `verdict` while it writes. It writes more than it dialled — a url folded " +
                "onto another, or already failed by the stability gate, is graded without a socket — so one " +
                "position for both would sit full at the dial count for the whole write, with `quietForSec` " +
                "climbing on a pass doing thousands of writes.",
        )
        term(
            "etaSec",
            "An estimate, from the rate the walks have averaged so far, of how long the paged half of this pass " +
                "has left. It says nothing about the relays that reconcile rather than page, and nothing about " +
                "how long the stragglers of the last pass will take.",
        )
    }
