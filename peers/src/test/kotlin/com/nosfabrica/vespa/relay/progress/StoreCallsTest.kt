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
 * The store-call registry — the report that turns "two workers have been in a
 * batch for 794 seconds" into "both on ingest.dedup, 2,048 ids, thirteen
 * minutes ago".
 *
 * What is under test is everything about it that can be wrong SILENTLY. A row
 * that outlives its call is a fault report about work that is not happening; a
 * cancelled call counted as a failure reads as a store refusing work; a caller
 * tally that does not close cannot be checked by the reader it is published
 * for; an age histogram that does not sum looks reasonable row by row. None of
 * those throws, and every one of them is read as a finding.
 *
 * The clock is driven rather than waited on — [StoreCalls]'s `now` seam exists
 * for exactly that — so "a call thirteen minutes old" is a fact this file can
 * state instead of thirteen minutes it would have to spend.
 */
class StoreCallsTest {
    /** A clock the test moves by hand, so a call can be any age at no cost. */
    private class Clock(
        var ms: Long = 1_000_000,
    ) : () -> Long {
        override fun invoke(): Long = ms
    }

    /**
     * Hold [caller]'s call open, run [body] while it is out, and let it return.
     *
     * A real coroutine under a real registry, because the wiring is half of what
     * is under test: a call is booked because it is running inside a scope
     * carrying the element, and a helper that reached into the map directly
     * would assert nothing about that.
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
            // The three facts three investigations had to guess at, in one row.
            assertEquals(StoreCalls.CALLER_INGEST_DEDUP, row.caller)
            assertEquals(StoreCalls.OP_EXISTING_IDS, row.op)
            assertEquals("2048 id(s)", row.asked)
            assertEquals(794, row.elapsedSec)
            // Nothing else was out when it went — the reading that says this
            // call did not queue behind us.
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

            // The store refusing a batch: fast, loud, and a different remedy
            // from a call that never comes back.
            scope
                .launch {
                    runCatching { storeCall(StoreCalls.CALLER_INGEST_WRITE, StoreCalls.OP_BATCH_INSERT) { error("schema drift") } }
                }.join()

            // …and the process stopping, which is not a fault: folded into
            // `failed` it would report a clean shutdown as a store refusing
            // work — the reason the ingest probes rethrow cancellation rather
            // than swallowing it.
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
            // WITH NOTHING IN FLIGHT the lifetime counters account for every
            // call, which is the only moment that identity is exact — see
            // [StoreCalls.Caller]. On a busy router `issued` and the terminal
            // counters are stamped either side of the row's own insertion and
            // removal, so a call that finished mid-snapshot lands on one side
            // and not the other; what holds ALWAYS is the live half, asserted
            // in the partition test below.
            assertEquals(caller.issued, caller.returned + caller.failed + caller.cancelled)
        }

    @Test
    fun `a dispatcher hop keeps the registry, so ingest's own worker pool books its calls`() =
        runBlocking {
            val calls = StoreCalls()
            // THE WIRING RISK, asserted rather than assumed. `SyncEngine`
            // installs the registry on one scope and ingest launches its
            // workers onto a dispatcher of their own
            // (`Executors.newFixedThreadPool(...).asCoroutineDispatcher()`),
            // with the store reaching for `Dispatchers.IO` underneath. A
            // context element survives all of that — but "survives" is the
            // whole claim this design rests on, and a report that quietly
            // booked nothing would look exactly like a router with nothing
            // outstanding.
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
            // Three calls of very different ages, issued against a clock this
            // test moves — so the order and the bands are facts about the
            // registry rather than about how fast the machine ran.
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
            // The second and third rows found calls already out, which is the
            // client-side half of "slow store, or waiting in line".
            assertEquals(listOf(0, 1, 2), snapshot.calls.map { it.outstandingAtIssue }.sorted())

            // THE LIVE HALF IS ONE PARTITION, THREE WAYS — and it closes
            // whatever the router is doing, because all three come off one read
            // of the row set. A histogram that does not sum is the one failure
            // a reader cannot see, since every row of it looks reasonable
            // alone; `accountedFor` on the card reports it, so a raced read
            // here would have the router accusing itself.
            assertEquals(snapshot.outstanding, snapshot.ages.sumOf { it.calls })
            assertEquals(snapshot.outstanding, snapshot.callers.sumOf { it.outstanding })
            assertEquals(StoreCalls.AGE_BANDS, snapshot.ages.map { it.fromSec }, "every band is published, the empty ones included")
            assertEquals(1, snapshot.ages.single { it.fromSec == 0L }.calls, "the 200ms call is under a second")
            assertEquals(1, snapshot.ages.single { it.fromSec == 10L }.calls, "…the 45s one is in 10s-60s")
            assertEquals(1, snapshot.ages.single { it.fromSec == 300L }.calls, "…and the 794s one is in 5m-15m")

            // Ties on `outstanding` fall back to lifetime traffic and then to
            // the name, so one state rolls up the same way twice.
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
            // What makes the line actionable: who, which call, and what it asked
            // for. A warning naming only a duration is the state this whole
            // file exists to leave behind.
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

        // A WEDGE IS TEN MINUTES OF EVERY WORKER HELD, which is why this is a
        // unit test: a live run against a frozen store proves the SLOW lines in
        // a minute and cannot reach `IngestPipeline.WEDGE_AFTER_MS` without
        // waiting out the threshold that exists to stop the router crying wolf.
        assertNull(calls.describeOldest(), "no store call out is not a fault — it says the workers are held elsewhere")

        calls.whileOut(
            Triple(StoreCalls.CALLER_INGEST_WRITE, StoreCalls.OP_BATCH_INSERT, StoreCalls.events(2_000)),
            Triple(StoreCalls.CALLER_VISIT_NEGENTROPY, StoreCalls.OP_COUNT, "kinds 1985"),
        ) {
            clock.ms += 794_000
            val line = calls.describeOldest()!!
            // The LONGEST one, and enough of it to act on: the health line has
            // already decided something is wrong, so what it needs from here is
            // which call — a batch pass makes three, against three engine
            // paths, with three remedies.
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
        // The window is a WIDTH, not two epoch seconds: a negentropy window's
        // cost is mostly its width, and nobody subtracts ten-digit numbers at a
        // glance.
        assertTrue("100" !in summary && "700" !in summary, "the raw stamps are not the reading: $summary")
        // An unbounded ask says so rather than rendering blank, which would read
        // as a report that declined to answer.
        assertEquals("everything", StoreCalls.summarise(Filter()))
        // The ids are COUNTED. Two thousand of them per row is a document
        // nobody can open, and WHICH ids answers nothing the count does not.
        assertEquals("2048 id(s)", StoreCalls.ids(2_048))
        // A tag ask names its KEYS and their widths — the `#d` chunks the
        // monitor's verdict reads are made of — and none of their values.
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

        // A mistyped value that quietly reverts is a setting an operator
        // believes is in effect. Every other knob in this process refuses.
        runCatching { StoreCalls.fromEnv(mapOf("SYNC_STORE_SLOW_SEC" to "a minute")) }
            .onSuccess { fail("a non-numeric threshold must stop the process, not default") }
            .onFailure { assertTrue("SYNC_STORE_SLOW_SEC" in (it.message ?: ""), "the refusal must name the variable: ${it.message}") }
        runCatching { StoreCalls.fromEnv(mapOf("SYNC_STORE_REWARN_SEC" to "-1")) }
            .onSuccess { fail("a negative re-warn period must stop the process too") }
            .onFailure { assertTrue("SYNC_STORE_REWARN_SEC" in (it.message ?: "")) }

        // …a good one is taken, and taken in SECONDS: the variable is named in
        // seconds and held in millis, and a factor of a thousand in either
        // direction is a threshold that never fires or fires at once. Straddled
        // rather than hit exactly, because a row's published `issuedAt` is
        // truncated to the second and the bound is not the thing in doubt.
        val parsed = StoreCalls.fromEnv(mapOf("SYNC_STORE_SLOW_SEC" to "90"))
        assertTrue(!parsed.namesAt(80_000), "a 90-second bound must not fire at 80 — the value was read as something other than seconds")
        assertTrue(parsed.namesAt(100_000), "…and must fire at 100")
        // …AND THE PAGE IS TOLD, so a row is marked at the operator's bound
        // rather than at the page's copy of the default. Without this the log
        // and the colour mean two different things by the same word.
        assertEquals(90L, parsed.snapshot().slowAfterSec)
        assertEquals(0L, StoreCalls.fromEnv(mapOf("SYNC_STORE_SLOW_SEC" to "0")).snapshot().slowAfterSec, "off is published as off")
        // …and it is in effect, checked through the only thing that reveals a
        // threshold: whether a call of a known age is due to be named.
        val tuned = StoreCalls(slowAfterMs = 90_000, now = clock)
        tuned.whileOut(Triple(StoreCalls.CALLER_INGEST_WRITE, StoreCalls.OP_BATCH_INSERT, null)) {
            clock.ms += 80_000
            assertTrue(tuned.warnSlow().isEmpty(), "80s is inside a 90s bound")
            clock.ms += 20_000
            assertEquals(1, tuned.warnSlow().size, "…and 100s is past it")
        }
    }

    /**
     * Whether this registry names a call [ageMs] old — the one observable a
     * threshold has, and how the parsed value is checked without a field.
     *
     * Its own clock, driven from a fixed instant, so the answer is about the
     * bound and not about how long the assertion took to run.
     */
    private fun StoreCalls.namesAt(ageMs: Long): Boolean {
        var named = false
        whileOut(Triple(StoreCalls.CALLER_INGEST_WRITE, StoreCalls.OP_BATCH_INSERT, null)) {
            // `issuedAt` is published in seconds, so this instant is the row's
            // own to within a second — which is why the caller straddles the
            // bound rather than sitting on it.
            val issued = snapshot().calls.single().issuedAt * 1_000
            named = warnSlow(issued + ageMs).isNotEmpty()
        }
        return named
    }
}
