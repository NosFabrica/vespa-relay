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
 * learn which two: a bare pending count is derived by subtraction,
 * the coverage card only draws relays that have EARNED a band (a stalled leg
 * never does), the diagnostic log line names urls only for the stream
 * `SYNC_DIAGNOSE` points at, and container stderr rotates inside the hour. The
 * router knew both urls perfectly well the whole time — `RelayRotation` was
 * holding them — and published nothing but their number.
 *
 * So this is the counts' missing half, on the same terms as
 * the fold's own report: not the count, the NAMES.
 *
 * ## What it is a set of, exactly
 *
 * Urls this stream has a live worker on, right now — nothing else. Not the
 * roster (most of it is between visits), not what is queued (a queued url has
 * no worker yet, which is the whole difference). Read it as "what is actually
 * running", and read the roster count beside the phase for the rest.
 *
 * ## WHOLE, quietest first, and it still says what it left out
 *
 * It used to be cut to twenty rows, and the cut was wrong twice over.
 *
 * The sizing argument was that a fan-out's admission gate is far wider than
 * its transfer pool — 128 workers against 8 slots, the other 120 being connect
 * timeouts to hosts that will never answer — so the whole set was neither
 * small nor interesting. The pool killed that premise: a row here IS a worker
 * holding a socket, so the list is bounded by `visitConcurrency` and the whole
 * set is exactly the interesting thing.
 *
 * The ordering was the second half. Sorted by how long each had been HELD, the
 * twenty rows were routinely twenty healthy long-haulers with the wedged leg
 * cut into `omitted` — held is not risk, since the healthiest thing this
 * router does is hold one relay for an hour while it streams two million
 * events. Quietest-first fixed which twenty, and publishing all of them
 * retires the question.
 *
 * What the cut cost in the end was not a wedged leg but the plain reading: an
 * operator asking "what is this mirror connected to" got a sixth of the answer
 * on a card that looked complete, and one stream showing a single row was a
 * truncation artifact rather than a mirror down to one relay.
 *
 * `omitted` survives as the schema's promise — a list that does not disclose
 * its truncation reads as the whole answer, and a reader finding the member
 * absent cannot tell "nothing dropped" from "does not say".
 */
class InFlight(
    /** Every relay with a worker on it, quietest first. */
    val relays: List<Relay>,
    /** How many more had a worker and are not named here. Zero from the pool; never silently dropped. */
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
         *
         * A working leg's word names the JOB and then the TRANSPORT — `catching
         * up (paging)`, `auditing history (negentropy)` — because neither
         * implies the other: the audit is the full-past pass, whatever it uses
         * to download with, and an audit does page the windows a peer will not
         * reconcile. See `VisitPool.STAGE_PAGING` and its neighbours for the
         * pool's own set.
         */
        val stage: String? = null,
        /**
         * HOW FAR BACK the leg has got — a second, always read the same
         * direction whichever stage set it: the `created_at` the paged cursor
         * is reading now, or the older edge of the negentropy window an audit
         * is comparing now. Never the newer end of either range; an audit
         * publishing the window's `until` here read as `back to <today>` for
         * the whole of a sweep with years still to compare.
         *
         * [doing] `catching up (paging)` beside a large [quietForSec] is two legs that look
         * identical here: one deep in a real backlog and one whose cursor has
         * stopped. Read twice, this separates them. The stream's `reached` cannot
         * — it is the MINIMUM over every live walk, one date describing the
         * deepest, while a row is drawn because it is the exception.
         *
         * Null when neither is running for this url: in the guards, queued, or
         * a retraction pass, whose reconcile publishes no window.
         */
        val pagingUntil: Long? = null,
    )

    companion object {
        /** Nothing is running — the honest answer for a stream between passes. */
        val NONE = InFlight(emptyList(), 0)
    }
}
