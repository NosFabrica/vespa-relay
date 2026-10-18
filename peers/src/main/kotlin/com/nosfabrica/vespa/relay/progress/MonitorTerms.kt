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

/** The monitor's own members: the probe passes' partitions and the fitness pass's verdict funnel. */
internal val MONITOR_TERMS: JsonObject =
    glossary {
        term(
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
        term(
            "candidates",
            "Urls a stream handed a processor's pass, before anything was decided — the router's own word for the " +
                "argument it passes (`measure(label, candidates, …)`). The denominator `unmeasured` is counted " +
                "against, and NOT the number that was dialled: most of a candidate set already carries a current " +
                "verdict and is never asked again until it ages out. Published in two places for one population: " +
                "inside a pass's `streams` row it is what that pass was handed, and on the ALIAS SOURCE's row it " +
                "is what the derivation yielded — `sourced - excluded - heldOutDead`, stated rather than left to " +
                "a subtraction, because it is the number every count on the passes below is a share of.",
        )
        term(
            "newUrls",
            "…of those, how many arrived at the pass with NO verdict at all — the urls it exists to decide, and " +
                "the number the card draws its position against (`143 of 1,754 new relay(s) checked for aliases`). " +
                "`unmeasured` is the same population after the pass has run, so `newUrls - unmeasured` is what it " +
                "decided. Counted against `candidates` instead, the position barely moves however much a pass " +
                "achieves: most of a settled candidate set carries a verdict from weeks ago and is never asked " +
                "again until it ages out. Absent on a pass that does not count it, where the card falls back to " +
                "the whole candidate set.",
        )
        term(
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
        term(
            "dialled",
            "Dials that pass spent: fingerprints for the alias fold, paired walks for the self-consistency gate. " +
                "A pass measures its whole candidate set — there is no per-pass total — so `unmeasured` falls to " +
                "what could not be DECIDED rather than to what there was budget for. What is left after a pass " +
                "is a host on a cooldown or one that cannot answer twice, and `undecided` names it. Urls a socket " +
                "was actually opened for: one the transport declined costs no connection and is not counted here, " +
                "though it IS counted under `unmeasured` and named in `undecided`.",
        )
        term(
            "sourced",
            "Every url the relay lists in monitor.conf's own `sources` yielded for a probe pass, before " +
                "anything was held out — the widest number this router has about the network it can see, and the " +
                "one `candidates` is a share of. Per PASS and over the union of those sources, so two sources " +
                "naming the same url make it one url here. Nothing the STREAMS discover reaches this number: " +
                "what the monitor measures is its own declaration, never inferred from what the mirror syncs.",
        )
        term(
            "corpus",
            "The coverage tree's root: every relay url this router knows of, which is `sourced + recordedOnly` — " +
                "what a relay list named this round, plus what only our own signed records still know about. Its " +
                "own name because `sourced` means one of those two and the root means both: a row labelled " +
                "\"everything this router knows of\" carrying `sourced`'s definition would document the wrong " +
                "number for the widest row on the card. Not a published member — the page synthesises it from " +
                "the two that are.",
        )
        term(
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
        term(
            "heldOutDead",
            "…of those, how many carried a current signed unreachability record and were dropped before either " +
                "probe pass saw them. `sourced - heldOutDead = candidates`. Not permanent: the record ages out " +
                "(24h) or the host delivers something, and the url is back in the next derivation. Distinct from " +
                "the monitor's own `knownDead`, which is the size of the whole dead set rather than its overlap " +
                "with what the streams asked for.",
        )
        term(
            "foldedAway",
            "Urls of the candidate set an ALIAS FOLD has already taken out of the fan-out, and therefore the " +
                "first slice of the partition: a folded url is never measured for stability, so it can carry no " +
                "verdict here whatever it carried before it folded. First in precedence for that reason — a url " +
                "that was measured and later folded counts once, here.",
        )
        term(
            "consistent",
            "Urls of the candidate set that currently carry a STABLE verdict: measured, and they answered one " +
                "filter at a week-old anchor the same way twice. Standing state, not this pass's work — it " +
                "includes every verdict read back from the store at boot, which is why it can be large beside a " +
                "`decided` of zero. These are the urls the fan-out is allowed to hold a cursor against.",
        )
        term(
            "inconsistent",
            "…and the ones measured and FAILED: a url that answered the same filter two different ways. Every one " +
                "of them is refused by every stream's fan-out, because a relay whose window is a fresh slice " +
                "holds no stable cursor and every cycle re-serves what the last one took. The per-cycle count of " +
                "the same finding is `refusedUnstable`. Expires with its record, so a server that is fixed " +
                "rejoins on its own.",
        )
        term(
            "decided",
            "New verdicts that pass reached AND published. High `measured` with `decided` at zero is a pass that " +
                "dialled and learned nothing, which is a real and recoverable state — see `undecided`.",
        )
        term(
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
        term(
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
        term(
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
        term(
            "stage",
            "WHICH STEP a probe pass is on for the url it is holding, in the pass's own words — `pre-probe`, " +
                "`nip-11 document`, `ask ladder`, `neg-open` for fitness, `paired walk` for the stability gate, " +
                "`fingerprint` for the fold. `heldForSec` beside it says how long; only this says what for, and " +
                "the steps stall for unrelated reasons: a name that will not resolve stalls the pre-probe, a " +
                "relay that never stops sending stalls the ladder, a full ingest queue stalls whichever step is " +
                "delivering. It is the stream leg's `doing` for a job that is a ladder rather than a transfer.",
        )
        // The fitness pass's verdict funnel: one member per NIP-32 label value.
        term(
            "prime",
            "Relays the monitor currently grades prime: reachable AND answering AND canonical AND " +
                "consistent AND pageable AND serving what was asked for AND readable by us, measured on the " +
                "socket in one pass. This is the whole " +
                "admission decision — a stream's relay list is the store query `#l = prime` and nothing else. " +
                "Slow is not a refusal, empty is not a refusal, and a small message cap is not a refusal.",
        )
        term(
            "dead",
            "The transport itself said no: the name does not resolve, the connection was refused, TLS failed, or " +
                "the websocket upgrade was turned down. The one refusal about a relay that is actually absent.",
        )
        term(
            "silent",
            "Connected, then nothing — no EOSE, no CLOSED, the idle window lapsed. Alive and saying nothing, " +
                "which is not the same finding as dead and retries on its own terms.",
        )
        term(
            "alias",
            "Works fine, and is another record's relay: the fold proved this url and its canonical are one " +
                "server. Syncing it would double every event, so the certificate goes to the canonical alone.",
        )
        term(
            "unpageable",
            "Ignores the `until` cursor, so a paged walk against it cannot terminate — the measured failure mode " +
                "is ~5.5 pages a second forever. Refused outright in v1 rather than given tail-only treatment.",
        )
        term(
            "noncompliant",
            "Answered with events the filter did not ask for — a kind that was not in the ask, a `created_at` " +
                "above the `until`. Measured by reading the events themselves, which is why it is a separate " +
                "refusal from `inconsistent`: that one compares two answers to EACH OTHER, so a relay that " +
                "serves the same wrong events to every REQ passes it perfectly.",
        )
        term(
            "auth-refused",
            "Requires NIP-42 and rejected OUR key. A relay whose challenge our signer satisfies is certified like " +
                "any open one — an auth wall we can clear is no wall.",
        )
        term(
            "restricted",
            "Answers only shaped queries this router cannot generally send — a group host demanding an `#h` tag, " +
                "a filter service minting per-user paths. Probed with the same shaped-ask ladder the fold climbs " +
                "before the verdict is written.",
        )
        term(
            "waitingOnFold",
            "Urls the last fitness batch set aside because the alias fold has decided nothing about them and " +
                "another url of the same server stands in: a decided twin of the same endpoint, else the " +
                "server's canonical, else its preferred url. Not dialled and not graded, and a standing `prime` " +
                "on one is taken back, since grading every path of a path-ignoring server put it on the roster " +
                "once per path. Not a refusal: they rejoin as soon as the fold proves one its own relay.",
        )
        term(
            "pendingWrites",
            "Fitness verdicts and `prime` withdrawals already earned that the store has not taken: a write " +
                "hit its deadline, or the batch stopped after several in a row did. They are replayed every few " +
                "minutes from memory, without dialling again, and a later measurement of the same url replaces " +
                "one. Above zero for long means the store is refusing this monitor's writes, not that relays " +
                "changed; the signed records lag the roster until it drains. Lost on restart.",
        )
        term(
            "sharedHosts",
            "The last SWEEP's census of servers graded `prime` under more than one url — the shape that puts " +
                "one server on the mirror's roster several times. Each server's prime urls are listed with the " +
                "fold's word on each. Many servers here is not by itself a fault: a path-routing relay (an " +
                "inbox beside an outbox) is several relays on one name. `unproven` is the fault.",
        )
        term(
            "unproven",
            "Servers in `sharedHosts` holding two or more `prime` urls the fold has NOT measured as their own " +
                "relays (`fold` other than `canonical` or `distinct`). Fitness grades at most one such url per " +
                "server, so anything above zero is a regression in that rule or a grade written before it.",
        )
        term(
            "fold",
            "The alias fold's current word on one url: `alias` (folds onto another url, and should never be " +
                "prime), `canonical` (other urls fold onto it), `distinct` (measured and matched nothing it " +
                "was compared to), `stale` (an answer exists, under older rules or past its TTL, so it counts " +
                "for nothing) or `none` (never decided).",
        )
    }
