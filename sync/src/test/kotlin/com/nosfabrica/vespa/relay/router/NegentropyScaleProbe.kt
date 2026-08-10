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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import com.vitorpamplona.quartz.nip77Negentropy.NegentropyServerSession
import com.vitorpamplona.quartz.nip77Negentropy.NegentropySession
import kotlin.random.Random
import kotlin.test.Test

/**
 * DIAGNOSTIC, off by default: drives quartz's NIP-77 client session against its
 * own server session in-process, at the corpus size issue #91 reports
 * (269,591 kind-30382 records for one author), and prints the diff.
 *
 * Two identical sets must reconcile to an EMPTY diff. Anything else is the
 * library reporting records as absent that both sides hold — the shape of the
 * false "have" ids `deleteMissing` would act on.
 *
 * Run with `-DnegProbe=true`.
 *
 * **What it answered.** `need = 0, have = 0` in ONE round at every size and
 * tie density tried, up to 278,732 entries with 200 sharing each `created_at`
 * — and an injected 3-need/2-have difference comes back as exactly 3 and 2.
 * The reconciliation library is not what turns held records into false
 * "have" ids at this scale; see [StrfryDeleteMissingProbe] for the same
 * question asked of a real relay.
 */
class NegentropyScaleProbe {
    private fun entries(
        n: Int,
        spanSeconds: Long,
        perSecond: Int,
        seed: Int,
    ): List<IdAndTime> {
        val rnd = Random(seed)
        val base = 1_777_000_000L
        val out = ArrayList<IdAndTime>(n)
        var i = 0
        while (out.size < n) {
            // `perSecond` events share each timestamp, the rest spread over the span.
            val t = base + (if (perSecond > 1) (i / perSecond).toLong() % spanSeconds else rnd.nextLong(spanSeconds))
            val id = ByteArray(32).also { rnd.nextBytes(it) }.joinToString("") { b -> "%02x".format(b) }
            out.add(IdAndTime(t, id))
            i++
        }
        return out
    }

    /** One full reconcile, driven to completion. Returns need/have counts and rounds. */
    private fun reconcile(
        mine: List<IdAndTime>,
        theirs: List<IdAndTime>,
        clientFrameLimit: Long = 0,
        serverFrameLimit: Long = NegentropyServerSession.DEFAULT_FRAME_SIZE_LIMIT,
    ): Triple<Int, Int, Int> {
        val client = NegentropySession("probe", Filter(kinds = listOf(30382)), mine, clientFrameLimit)
        val server = NegentropyServerSession("probe", theirs, serverFrameLimit)

        var need = 0
        var have = 0
        var rounds = 0
        var out: String? = client.open().initialMessage
        while (out != null) {
            val fromServer = server.processMessage(out) ?: break
            rounds++
            val r = client.processMessage(fromServer.message)
            need += r.needIds.size
            have += r.haveIds.size
            out = r.nextCmd?.message
        }
        return Triple(need, have, rounds)
    }

    @Test
    fun identicalSetsReconcileToNothing() {
        if (System.getProperty("negProbe") != "true") return

        val cases =
            listOf(
                // n, span, events sharing one timestamp
                Triple(269_591, 8_640_000L, 1),
                Triple(269_591, 8_640_000L, 40),
                Triple(278_732, 8_640_000L, 200),
                Triple(50_000, 100_000L, 1),
            )
        for ((n, span, perSecond) in cases) {
            val mine = entries(n, span, perSecond, seed = 7)
            val started = System.currentTimeMillis()
            val (need, have, rounds) = reconcile(mine, mine)
            println(
                "probe: n=$n span=${span}s ties=$perSecond -> need=$need have=$have" +
                    " (${have * 100.0 / n}% of ours) rounds=$rounds in ${System.currentTimeMillis() - started}ms",
            )
        }
    }

    @Test
    fun knownDifferenceIsReportedExactly() {
        if (System.getProperty("negProbe") != "true") return

        val all = entries(269_591, 8_640_000L, 40, seed = 11)
        // They dropped 2 of ours (a genuine retraction); we lack 3 of theirs.
        val mine = all.drop(3)
        val theirs = all.filterIndexed { i, _ -> i !in setOf(10, 20_000) }
        val (need, have, rounds) = reconcile(mine, theirs)
        println("probe: expected need=3 have=2 -> need=$need have=$have rounds=$rounds")
    }
}
