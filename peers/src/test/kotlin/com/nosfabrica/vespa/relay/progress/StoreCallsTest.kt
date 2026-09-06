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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.util.concurrent.Executors
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The store-call registry: which caller is in which store method, for how long.
 * The clock is driven through [StoreCalls]'s `now` seam, so a call's age is stated, not waited for.
 */
class StoreCallsTest {
    private class Clock(
        var ms: Long = 1_000_000,
    ) : () -> Long {
        override fun invoke(): Long = ms
    }

    /**
     * Holds [calls] open, runs [body] while they are out, then lets them return. Real
     * coroutines, because booking through the context element is half of what is under test.
     */
    private fun StoreCalls.whileOut(
        vararg calls: Triple<String, String, String?>,
        body: () -> Unit,
    ) = runBlocking {
        val hold = CompletableDeferred<Unit>()
        val started = calls.map { CompletableDeferred<Unit>() }
        val scope = CoroutineScope(Dispatchers.Default + this@whileOut)
        val jobs =
            calls.mapIndexed { i, (caller, op, asked) ->
                scope.launch {
                    storeCall(caller, op, asked) {
                        started[i].complete(Unit)
                        hold.await()
                    }
                }
            }
        withTimeout(5_000) { started.forEach { it.await() } }
        try {
            body()
        } finally {
            hold.complete(Unit)
            jobs.forEach { it.join() }
        }
    }

    @Test
    fun `a call is booked while it runs and gone the moment it returns`() {
        val clock = Clock()
        val calls = StoreCalls(now = clock)

        calls.whileOut(Triple(StoreCalls.CALLER_INGEST_DEDUP, StoreCalls.OP_EXISTING_IDS, StoreCalls.ids(2_048))) {
            clock.ms += 794_000
            val held = calls.snapshot()
            assertEquals(1, held.outstanding)
            val row = held.calls.single()
            assertEquals(StoreCalls.CALLER_INGEST_DEDUP, row.caller)
            assertEquals(StoreCalls.OP_EXISTING_IDS, row.op)
            assertEquals("2048 id(s)", row.asked)
            assertEquals(794, row.elapsedSec)
            assertEquals(0, row.outstandingAtIssue)
            assertEquals(794, held.callers.single().oldestOutstandingSec)
        }

        val after = calls.snapshot()
        assertEquals(0, after.outstanding, "a returned call must not keep a row — it would name work that is not happening")
        assertTrue(after.calls.isEmpty())
        val caller = after.callers.single()
        assertEquals(1, caller.issued)
        assertEquals(1, caller.returned)
        assertEquals(0, caller.outstanding)
        assertNull(caller.oldestOutstandingSec, "nothing out is no age — a zero would read as a call that just started")
    }

    @Test
    fun `a throw is failed, a cancellation is not, and both release the row`() =
        runBlocking {
            val calls = StoreCalls()
            val scope = CoroutineScope(Dispatchers.Default + calls)

            scope
                .launch {
                    runCatching { storeCall(StoreCalls.CALLER_INGEST_WRITE, StoreCalls.OP_BATCH_INSERT) { error("schema drift") } }
                }.join()

            // Folded into `failed`, a clean shutdown would read as a store refusing work.
            scope
                .launch {
                    runCatching {
                        storeCall(StoreCalls.CALLER_INGEST_WRITE, StoreCalls.OP_BATCH_INSERT) {
                            throw CancellationException("shutting down")
                        }
                    }
                }.join()

            val snapshot = calls.snapshot()
            val caller = snapshot.callers.single()
            assertEquals(2, caller.issued)
            assertEquals(0, caller.returned)
            assertEquals(1, caller.failed)
            assertEquals(1, caller.cancelled)
            assertEquals(0, snapshot.outstanding, "a call that threw has stopped being outstanding either way")
            // Exact only with nothing in flight; the live partition, below, is what always holds.
            assertEquals(caller.issued, caller.returned + caller.failed + caller.cancelled)
        }

    @Test
    fun `a dispatcher hop keeps the registry, so ingest's own worker pool books its calls`() =
        runBlocking {
            val calls = StoreCalls()
            // The element surviving ingest's own dispatcher and `Dispatchers.IO` beneath it is the claim.
            val pool = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
            try {
                CoroutineScope(Dispatchers.Default + calls)
                    .launch(pool) {
                        withContext(Dispatchers.IO) {
                            storeCall(StoreCalls.CALLER_INGEST_WRITE, StoreCalls.OP_BATCH_INSERT, StoreCalls.events(512)) { }
                        }
                    }.join()
            } finally {
                pool.close()
            }

            val caller = calls.snapshot().callers.single()
            assertEquals(StoreCalls.CALLER_INGEST_WRITE, caller.caller)
            assertEquals(1, caller.returned)
        }

    @Test
    fun `a call outside a scope carrying the registry is untracked, never misattributed`() =
        runBlocking {
            val calls = StoreCalls()

            val answer = storeCall(StoreCalls.CALLER_HEAL_RESOLVE, StoreCalls.OP_QUERY) { 42 }

            assertEquals(42, answer, "the block still runs — an absent registry may cost a report, never an answer")
            assertEquals(0, calls.snapshot().issued, "nothing may be booked into a registry this coroutine never carried")
        }

    @Test
    fun `the rows are longest-running first and the age bands partition them`() {
        val clock = Clock()
        val calls = StoreCalls(now = clock)
        val hold = CompletableDeferred<Unit>()
        val scope = CoroutineScope(Dispatchers.Default + calls)

        runBlocking {
            for (
            (caller, at) in
            listOf(
                StoreCalls.CALLER_VISIT_NEGENTROPY to 0L,
                StoreCalls.CALLER_HEAL_RESOLVE to 749_000L,
                StoreCalls.CALLER_INGEST_DEDUP to 793_800L,
            )
            ) {
                clock.ms = 1_000_000 + at
                val started = CompletableDeferred<Unit>()
                scope.launch {
                    storeCall(caller, StoreCalls.OP_QUERY, "kinds 1") {
                        started.complete(Unit)
                        hold.await()
                    }
                }
                withTimeout(5_000) { started.await() }
            }
            clock.ms = 1_000_000 + 794_000

            val snapshot = calls.snapshot()

            assertEquals(
                listOf(StoreCalls.CALLER_VISIT_NEGENTROPY, StoreCalls.CALLER_HEAL_RESOLVE, StoreCalls.CALLER_INGEST_DEDUP),
                snapshot.calls.map { it.caller },
                "longest-running first — a call that has not come back is the anomaly, which a held relay is not",
            )
            assertEquals(listOf(794L, 45L, 0L), snapshot.calls.map { it.elapsedSec })
            // The second and third found calls already out: the client-side half of "slow store, or waiting in line".
            assertEquals(listOf(0, 1, 2), snapshot.calls.map { it.outstandingAtIssue }.sorted())

            // All three come off one read of the row set, so the partition closes whatever the router is doing.
            assertEquals(snapshot.outstanding, snapshot.ages.sumOf { it.calls })
            assertEquals(snapshot.outstanding, snapshot.callers.sumOf { it.outstanding })
            assertEquals(StoreCalls.AGE_BANDS, snapshot.ages.map { it.fromSec }, "every band is published, the empty ones included")
            assertEquals(1, snapshot.ages.single { it.fromSec == 0L }.calls, "the 200ms call is under a second")
            assertEquals(1, snapshot.ages.single { it.fromSec == 10L }.calls, "…the 45s one is in 10s-60s")
            assertEquals(1, snapshot.ages.single { it.fromSec == 300L }.calls, "…and the 794s one is in 5m-15m")

            // Ties on `outstanding` fall back to lifetime traffic and then to the name.
            assertEquals(calls.snapshot().callers.map { it.caller }, snapshot.callers.map { it.caller })

            hold.complete(Unit)
        }
    }

    @Test
    fun `a slow call is named once, then again on its own clock`() {
        val clock = Clock()
        val calls = StoreCalls(slowAfterMs = 60_000, rewarnAfterMs = 300_000, now = clock)

        calls.whileOut(Triple(StoreCalls.CALLER_INGEST_DEDUP, StoreCalls.OP_EXISTING_IDS, StoreCalls.ids(2_048))) {
            clock.ms += 30_000
            assertTrue(calls.warnSlow().isEmpty(), "a call younger than the bound is not news")

            clock.ms += 40_000
            val line = calls.warnSlow().single()
            assertTrue(StoreCalls.CALLER_INGEST_DEDUP in line, "the line must name the caller: $line")
            assertTrue(StoreCalls.OP_EXISTING_IDS in line, "…and the store method: $line")
            assertTrue("2048 id(s)" in line, "…and what it asked for: $line")

            clock.ms += 60_000
            assertTrue(
                calls.warnSlow().isEmpty(),
                "a call already named must not be named again on the next pass — ten stuck calls would bury the health line",
            )

            clock.ms += 300_000
            assertEquals(1, calls.warnSlow().size, "…and must be named again once the re-warn clock is up: a wedge is watched over hours")
        }
    }

    @Test
    fun `the wedge line names the longest call, and says so when there is none`() {
        val clock = Clock()
        val calls = StoreCalls(now = clock)

        assertNull(calls.describeOldest(), "no store call out is not a fault — it says the workers are held elsewhere")

        calls.whileOut(
            Triple(StoreCalls.CALLER_INGEST_WRITE, StoreCalls.OP_BATCH_INSERT, StoreCalls.events(2_000)),
            Triple(StoreCalls.CALLER_VISIT_NEGENTROPY, StoreCalls.OP_COUNT, "kinds 1985"),
        ) {
            clock.ms += 794_000
            val line = calls.describeOldest()!!
            assertTrue(StoreCalls.OP_BATCH_INSERT in line || StoreCalls.OP_COUNT in line, "the line must name the call: $line")
            assertTrue("13:14" in line, "…and how long it has been in it: $line")
        }

        assertNull(calls.describeOldest(), "and nothing outstanding once they return")
    }

    @Test
    fun `the warning can be turned off without turning off the report`() {
        val clock = Clock()
        val calls = StoreCalls(slowAfterMs = 0, now = clock)

        calls.whileOut(Triple(StoreCalls.CALLER_VISIT_NEGENTROPY, StoreCalls.OP_SNAPSHOT_IDS, "kinds 1")) {
            clock.ms += 3_600_000
            assertTrue(calls.warnSlow().isEmpty(), "zero is the operator saying they are tired of the log")
            assertEquals(1, calls.snapshot().outstanding, "…and it must not cost them the page, which nothing has to read")
        }
    }

    @Test
    fun `a filter is summarised, never echoed`() {
        val summary = StoreCalls.summarise(Filter(kinds = listOf(1), authors = listOf("a", "b"), since = 100, until = 700))

        assertEquals("kinds 1, 2 author(s), window 10:00", summary)
        // The window is a width, not two epoch seconds: a negentropy window's cost is mostly its width.
        assertTrue("100" !in summary && "700" !in summary, "the raw stamps are not the reading: $summary")
        // An unbounded ask says so rather than rendering blank.
        assertEquals("everything", StoreCalls.summarise(Filter()))
        // Ids are counted, not listed.
        assertEquals("2048 id(s)", StoreCalls.ids(2_048))
        // A tag ask names its keys and their widths, none of their values.
        assertEquals(
            "kinds 30166, 1 author(s), #d x500",
            StoreCalls.summarise(
                Filter(kinds = listOf(30166), authors = listOf("self"), tags = mapOf("d" to List(500) { "wss://r$it.example" })),
            ),
        )
    }

    @Test
    fun `a bad threshold is refused rather than silently defaulted`() {
        val clock = Clock()

        // A mistyped value that quietly reverts is a setting an operator believes is in effect.
        runCatching { StoreCalls.fromEnv(mapOf("SYNC_STORE_SLOW_SEC" to "a minute")) }
            .onSuccess { fail("a non-numeric threshold must stop the process, not default") }
            .onFailure { assertTrue("SYNC_STORE_SLOW_SEC" in (it.message ?: ""), "the refusal must name the variable: ${it.message}") }
        runCatching { StoreCalls.fromEnv(mapOf("SYNC_STORE_REWARN_SEC" to "-1")) }
            .onSuccess { fail("a negative re-warn period must stop the process too") }
            .onFailure { assertTrue("SYNC_STORE_REWARN_SEC" in (it.message ?: "")) }

        // Straddled rather than hit exactly, because a row's `issuedAt` is truncated to the second.
        val parsed = StoreCalls.fromEnv(mapOf("SYNC_STORE_SLOW_SEC" to "90"))
        assertTrue(!parsed.namesAt(80_000), "a 90-second bound must not fire at 80 — the value was read as something other than seconds")
        assertTrue(parsed.namesAt(100_000), "…and must fire at 100")
        // The page is told, so a row is marked at the operator's bound and not at the page's copy of the default.
        assertEquals(90L, parsed.snapshot().slowAfterSec)
        assertEquals(0L, StoreCalls.fromEnv(mapOf("SYNC_STORE_SLOW_SEC" to "0")).snapshot().slowAfterSec, "off is published as off")
        val tuned = StoreCalls(slowAfterMs = 90_000, now = clock)
        tuned.whileOut(Triple(StoreCalls.CALLER_INGEST_WRITE, StoreCalls.OP_BATCH_INSERT, null)) {
            clock.ms += 80_000
            assertTrue(tuned.warnSlow().isEmpty(), "80s is inside a 90s bound")
            clock.ms += 20_000
            assertEquals(1, tuned.warnSlow().size, "…and 100s is past it")
        }
    }

    /** Whether this registry names a call [ageMs] old, from a fixed instant so only the bound decides. */
    private fun StoreCalls.namesAt(ageMs: Long): Boolean {
        var named = false
        whileOut(Triple(StoreCalls.CALLER_INGEST_WRITE, StoreCalls.OP_BATCH_INSERT, null)) {
            // `issuedAt` is published in seconds, so this instant is the row's own to within a second.
            val issued = snapshot().calls.single().issuedAt * 1_000
            named = warnSlow(issued + ageMs).isNotEmpty()
        }
        return named
    }
}
