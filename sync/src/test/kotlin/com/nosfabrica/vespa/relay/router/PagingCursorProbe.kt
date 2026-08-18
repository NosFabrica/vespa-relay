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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.relay.router.progress.PagingProgress
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.OkHttpClient
import java.time.Duration
import java.time.Instant
import kotlin.test.Test

/**
 * HOW LONG DOES A PAGING LEG GO WITHOUT SAYING WHERE IT IS?
 *
 * `pagingUntil` is what separates the two legs a long `quietForSec` cannot tell
 * apart — one deep in a real backlog and working down, one whose cursor is not
 * moving at all. It was written only from `onNewPage`, which fires at page
 * BOUNDARIES, so a leg inside its first page had reported nothing and sat at the
 * second the walk opened at. On the live `/stats.json` that started this, 38 of
 * 40 legs marked `doing: paging` (the word is `catching up (paging)` now)
 * published no cursor at all.
 *
 * That gap cannot be measured hermetically. Its size is a property of REAL
 * relays — how long a first page takes, how many events it carries, whether the
 * relay serves them newest-first at all — so this runs both feeds side by side
 * against the public network and prints the difference:
 *
 *  - **per event** — what the callers now do, from the same `created_at` the
 *    band's span is folded from;
 *  - **per page** — the old behaviour, `onNewPage` only, kept running beside it.
 *
 * Two things are worth reading in the output. `first move` is the gap this
 * closes: the milliseconds each feed took to say ANYTHING after the walk opened,
 * and on the narrow control the page feed never says anything at all. `agree at
 * end` is the safety property, with one subtlety: both feeds make the same
 * statement, so a walk left to DRAIN must converge on the identical second —
 * while a walk cut mid-page ends with the per-event feed legitimately deeper,
 * because it has consumed events from a page the boundary feed has not closed.
 * This probe cuts every walk at its own ceiling, so both readings appear.
 *
 * OFF by default and not a gate: it dials the public internet, so it is neither
 * hermetic nor reproducible, and a relay being down is not a code regression. It
 * asserts nothing — it REPORTS, and a human reads it.
 *
 * ```
 * ./gradlew :sync:test --tests '*PagingCursorProbe*' -DpagingCursorProbe=true --rerun -i
 * ```
 *
 * `--rerun` is load-bearing — the task is up-to-date-checked, so a second
 * identical run is SKIPPED and prints nothing, which reads as a silent pass.
 */
class PagingCursorProbe {
    /**
     * Relays picked for the shape of their FIRST page, which is the only part
     * of a walk this measures. A deep corpus asked with no floor is the case
     * the change is for; the narrow ask is the control, where a page boundary
     * arrives so fast that neither feed has anything to say about it.
     */
    private val legs =
        listOf(
            Leg("wss://directory.yabu.me", Filter(kinds = listOf(0)), "deep kind 0 corpus"),
            Leg("wss://nos.lol", Filter(kinds = listOf(1)), "busy general relay"),
            Leg("wss://relay.damus.io", Filter(kinds = listOf(1)), "busy general relay"),
            Leg("wss://nos.lol", Filter(kinds = listOf(1), limit = 50), "narrow — control"),
        )

    private class Leg(
        val url: String,
        val filter: Filter,
        val shape: String,
    )

    /** One feed's account of when it first moved and where it ended. */
    private class Feed(
        val name: String,
        val progress: PagingProgress,
        val key: PagingProgress.Walked,
    ) {
        /** The handle the router now marks through — see [PagingProgress.Walk]. */
        var walk: PagingProgress.Walk? = null

        var firstMoveMs: Long? = null
        var updates = 0L

        /** Sampled by the watcher, not by the feed — the walk must not pay for being observed. */
        fun poll(
            startedMs: Long,
            top: Long,
        ) {
            if (firstMoveMs == null && (progress.cursorOf(key) ?: top) < top) {
                firstMoveMs = System.currentTimeMillis() - startedMs
            }
        }
    }

    @Test
    fun reportHowLongEachFeedTakesToSayWhereTheWalkIs() {
        if (System.getProperty("pagingCursorProbe") != "true") {
            println("[skip] PagingCursorProbe — set -DpagingCursorProbe=true to dial the public internet")
            return
        }
        val okhttp =
            OkHttpClient
                .Builder()
                .connectTimeout(Duration.ofSeconds(20))
                .pingInterval(Duration.ofSeconds(120))
                .build()
        val scope = CoroutineScope(SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)

        println("=".repeat(96))
        println("How long before a paging leg says where it is? (per-event feed vs the page-boundary feed)")
        println("=".repeat(96))
        try {
            for (leg in legs) {
                val relay = RelayUrlNormalizer.normalize(leg.url)
                // The window the router would open: the filter's own edges, or
                // now down to the plausible floor. `top` is what both feeds sit
                // at until something moves them, and is the whole subject here.
                val top = leg.filter.until ?: (System.currentTimeMillis() / 1000)
                val bottom = leg.filter.since ?: SyncCoverage.PLAUSIBLE_FLOOR
                val key = PagingProgress.Walked("probe", relay.url)
                val perEvent = Feed("per event", PagingProgress(), key)
                val perPage = Feed("per page", PagingProgress(), key)
                for (f in listOf(perEvent, perPage)) f.walk = f.progress.begin(key, top, bottom)

                var events = 0L
                var pages = 0L
                val startedMs = System.currentTimeMillis()
                // Sampled off the walk's own thread at the cadence an operator
                // refreshes at, floored so a fast relay still produces a reading:
                // polling inside the callbacks would measure the callback, not
                // what the card would have shown.
                val watcher =
                    scope.launch {
                        while (isActive) {
                            perEvent.poll(startedMs, top)
                            perPage.poll(startedMs, top)
                            delay(50)
                        }
                    }
                val outcome =
                    runCatching {
                        runBlocking {
                            withTimeoutOrNull(TERMINATION_MS) {
                                client.fetchAllPages(
                                    relay,
                                    listOf(leg.filter),
                                    idleTimeoutMs = 20_000L,
                                    onNewPage = { until ->
                                        pages++
                                        perPage.updates++
                                        perPage.walk?.reached(until)
                                    },
                                ) { event ->
                                    events++
                                    // The caller's guard, reproduced: a relay
                                    // serving `created_at = 0` may no more date
                                    // this row than it may date a band.
                                    if (SyncCoverage.isPlausible(event.createdAt)) {
                                        perEvent.updates++
                                        perEvent.walk?.reached(event.createdAt)
                                    }
                                }
                            }
                        }
                    }
                watcher.cancel()
                perEvent.poll(startedMs, top)
                perPage.poll(startedMs, top)

                val ranForMs = System.currentTimeMillis() - startedMs
                val eventCursor = perEvent.progress.cursorOf(key)
                val pageCursor = perPage.progress.cursorOf(key)
                println()
                println("${leg.url}  —  ${leg.shape}")
                println(
                    "  ran ${ranForMs / 1000}s · $events event(s) · $pages page boundary(ies) · " +
                        (outcome.exceptionOrNull()?.let { "threw ${it::class.simpleName}" } ?: "ok"),
                )
                for (f in listOf(perEvent, perPage)) {
                    val moved = f.firstMoveMs?.let { "${it}ms" } ?: "NEVER — still at the second the walk opened"
                    println("  %-10s first move %-46s %,d update(s)".format(f.name, moved, f.updates))
                }
                println("  cursor now  per event ${stamp(eventCursor)}   per page ${stamp(pageCursor)}")
                // The safety property. Both feeds say "the walk got this far",
                // so the finer one may be AHEAD of the page cursor mid-page but
                // must never claim ground the coarse one never covered by the
                // end of the walk.
                val agree =
                    when {
                        eventCursor == null || pageCursor == null -> "one feed has no live walk"

                        // Until a boundary fires, the page feed's cursor is still
                        // `top` — the second the walk opened at, not a position
                        // it reached. Comparing against it would read the whole
                        // point of the change as a disagreement.
                        pages == 0L -> "n/a — no page boundary, so the page feed never left the walk's start"

                        eventCursor >= pageCursor -> "yes — per-event never overtook the page cursor"

                        else -> "NO — per event is ${pageCursor - eventCursor}s BELOW the page cursor"
                    }
                println("  agree at end: $agree")
            }
        } finally {
            scope.cancel()
            okhttp.dispatcher.executorService.shutdown()
            okhttp.connectionPool.evictAll()
        }
        println()
        println("=".repeat(96))
    }

    private fun stamp(t: Long?): String = t?.let { Instant.ofEpochSecond(it).toString() } ?: "—"

    companion object {
        /**
         * A hard ceiling per relay, which `fetchAllPages` deliberately does not
         * have — a walk is bounded by a `limit` or by cancelling the caller, and
         * this is the caller cancelling. Without it one relay that keeps
         * answering hangs the whole probe.
         */
        private const val TERMINATION_MS = 90_000L
    }
}
