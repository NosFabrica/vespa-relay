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

/** The members of a processor row, which both the mirror's and the monitor's documents publish. */
internal val SHARED_TERMS: JsonObject =
    glossary {
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
