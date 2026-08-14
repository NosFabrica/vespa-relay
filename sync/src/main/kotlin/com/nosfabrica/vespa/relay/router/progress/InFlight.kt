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
package com.nosfabrica.vespa.relay.router.progress

/**
 * WHICH relays a stream has a worker on right now, and for how long — longest
 * held first.
 *
 * ## The question this exists to answer
 *
 * A production `sync.progress` reported `pending = 2` on a stream that had
 * received two events in eleven and a half hours. Two relays had held their
 * slots since the small hours and there was no way, anywhere in the system, to
 * learn which two: [CycleTally.pending] is a bare count derived by subtraction,
 * the coverage card only draws relays that have EARNED a band (a stalled leg
 * never does), the diagnostic log line names urls only for the stream
 * `SYNC_DIAGNOSE` points at, and container stderr rotates inside the hour. The
 * router knew both urls perfectly well the whole time — `RelayRotation` was
 * holding them — and published nothing but their number.
 *
 * So this is the counts' missing half, on the same terms as
 * [CycleTally.foldedOnto]: not the count, the NAMES.
 *
 * ## What it is a set of, exactly
 *
 * Urls with a live worker, across every pass. Passes overlap — the walk ends
 * when the last url is handed out, not when the last worker returns — so this
 * deliberately spans them, which is why it is published beside the cycle rather
 * than inside it. A url here is counted in the current cycle's `pending` if this
 * pass handed it out, and in its `busy` if an earlier one did, and the whole
 * point of naming a wedged leg is that it outlives the pass that dialled it.
 *
 * The containment runs one way only: **`pending` also counts urls that have no
 * worker yet**, because the walk has not reached them. Read this as "what is
 * actually running", never as "the list of pending urls".
 *
 * ## Bounded, QUIETEST first, and it says what it left out
 *
 * A fan-out's admission gate is far wider than its transfer pool — 128 workers
 * against 8 slots is ordinary, the other 120 being connect timeouts to hosts
 * that will never answer — so the whole set is neither small nor interesting.
 * The few rows worth keeping are the legs nothing is arriving on, and
 * [RelayRotation.held] sorts on exactly that; the tail is the ordinary churn.
 *
 * It was sorted by how long each had been HELD, and that is a different set.
 * Held is not risk — the healthiest thing this router does is hold one relay
 * for an hour while it streams two million events — so the twenty rows were
 * routinely twenty healthy long-haulers with the wedged leg cut into `omitted`.
 * See [RelayRotation.held] for the whole argument.
 *
 * `omitted` says how much tail there was: a truncated list that does not
 * disclose the truncation reads as the whole answer.
 */
class InFlight(
    /** The quietest relays, at most [RelayRotation.DEFAULT_IN_FLIGHT_ROWS] of them. */
    val relays: List<Relay>,
    /** How many more had a worker and are not named here. Never silently dropped. */
    val omitted: Int,
) {
    /**
     * One relay a worker is holding, and the clocks that say what it is doing
     * with it.
     *
     * Three questions, in the order an operator asks them: has it got a transfer
     * slot at all ([transferringForSec]), has it given us anything ([events]),
     * and is it still giving ([quietForSec]). Naming the relay without them only
     * moves the guesswork — a relay held for eleven hours is doing something
     * reasonable about as often as it is not.
     */
    class Relay(
        val relay: String,
        /**
         * The walk that handed this url out, or null where nothing numbers its
         * passes.
         *
         * A leg outlives the pass that started it, so a stream with two live
         * walks holds legs from both — and every clock on this row described the
         * leg without ever saying which walk it belonged to.
         */
        val pass: Long? = null,
        /**
         * Since the rotation CLAIMED it — which is before the guards, the TCP
         * pre-probe and the wait for a transfer slot, not just the transfer.
         * A relay held for hours with this the only clock running never got a
         * slot, and that is a different fault from a slow download.
         */
        val heldForSec: Long,
        /**
         * …and since it took a TRANSFER SLOT, or null when it has not got one.
         *
         * The slot, not the socket, and the difference was measured rather than
         * assumed: `InFlightReportProbe` watched a url that could not be
         * connected to at all report `transferring 0s` for its whole life and
         * end `CANNOT_CONNECT`. The clock starts when the worker is admitted to
         * the pool and the connect happens INSIDE it, so a leg stuck on a
         * websocket handshake is `transferring` and not absent.
         *
         * Absent is the ordinary answer for most of the set and is not missing
         * data: a stream with 8 slots routinely has 128 workers, and the other
         * 120 are in the guards (strikes, our Tor proxy, the TCP pre-probe) or
         * queued for a slot. Absent with a large [heldForSec] therefore says the
         * POOL is saturated — this worker is waiting behind other legs — which
         * is a fact about our own capacity. Present and large is the other one:
         * a slot committed to a transfer that is not ending.
         */
        val transferringForSec: Long?,
        /**
         * Events this leg has received off the wire so far.
         *
         * The leg's own count, not the stream's: `cycle.received` is every leg
         * added together and cannot single one out. Counted before ingest, like
         * every other `received` here, so it is the larger number.
         */
        val events: Long,
        /**
         * …and how long since the last one arrived — or since the claim, if none
         * ever did.
         *
         * THE ONE THAT DECIDES. A leg holding a slot for hours with events still
         * landing is a relay with a real backlog and the slot is well spent; the
         * same two durations with this number climbing is a walk that is not
         * going to end. Both were "held for hours, transferring" before this
         * existed, and only one of them is worth an operator's attention.
         */
        val quietForSec: Long,
        /**
         * WHAT THE LEG IS DOING, which the clocks above cannot say: in the
         * guards, queued behind our own pool, reconciling (where a long silence
         * is negentropy computing), or paging (where it is a walk that has
         * stopped delivering). [transferringForSec] separates the first two from
         * the rest; nothing separated the last two. Null before a leg reaches a
         * stage worth the word.
         */
        val doing: String? = null,
        /**
         * WHERE IN TIME a paging leg has got to: the `created_at` second its
         * cursor is reading now, or null when it is not paging.
         *
         * [doing] says a leg is `paging` and [quietForSec] says nothing has
         * arrived, and between them they still cannot separate the two walks
         * that look identical from here — one deep in a real backlog, working
         * its way down, and one whose cursor is not moving. This is the number
         * that does: read twice, it either advanced or it did not.
         *
         * The stream already publishes `reached`, but that is the MINIMUM over
         * its live walks — one date for a hundred legs, describing the deepest.
         * A row is drawn because it is the exception, so it carries its own.
         *
         * Null on every non-paging leg and on a router that predates the member,
         * which is a state rather than a gap: reconciling legs have no cursor,
         * and neither has one still in the guards.
         */
        val pagingUntil: Long? = null,
    )

    companion object {
        /** Nothing is running — the honest answer for a stream between passes. */
        val NONE = InFlight(emptyList(), 0)
    }
}
