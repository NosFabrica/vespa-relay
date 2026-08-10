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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcileIds
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.io.File
import java.time.Duration
import kotlin.test.Test

/**
 * DIAGNOSTIC for issue #91, off by default: the `deleteMissing` reconcile,
 * driven against a REAL strfry holding exactly the records we hold.
 *
 * The dry run there claims 55% of one provider's kind-30382 are no longer
 * served, while querying the relay back says 97% of them still are. Everything
 * between our store and the relay is the same code path this probe runs, minus
 * the store: one author, one owned kind, one unbounded `negentropyReconcileIds`
 * — the same call `DeleteMissingSync` makes, with the same defaults
 * (`targetWindow = 0`, so one window; client `frameSizeLimit = 0`).
 *
 * Two phases, because the corpus has to reach strfry between them:
 *
 * ```
 * ./gradlew :sync:test --tests '*StrfryDeleteMissingProbe*' -DstrfryProbe=gen  --rerun
 *   (writes $DIR/corpus.jsonl and $DIR/entries.tsv)
 * docker run ... strfry import < corpus.jsonl && docker run -d ... strfry relay
 * ./gradlew :sync:test --tests '*StrfryDeleteMissingProbe*' -DstrfryProbe=sync \
 *     -DstrfryUrl=ws://127.0.0.1:7777 --rerun
 * ```
 *
 * **What it answered** (strfry 1.1.1-119-g9acdaeb, `ghcr.io/hoytech/strfry`):
 * with the relay holding exactly what we hold, `need = 0, have = 0,
 * windows = 1` at 269,591 records in ~1s. The `1-window` in every report is
 * not a symptom: [negentropyReconcileIds] passes `targetWindow = 0`, so
 * nothing splits a window unless the RELAY refuses it. Drop that relay's
 * `maxSyncEvents` to 100,000 and it does refuse — `windows` goes to 11 and
 * the answer stays `need = 0, have = 0`. So a report saying `1-window` says
 * the relay never hit its cap, and a relay that does hit it is handled.
 */
class StrfryDeleteMissingProbe {
    private val dir = File(System.getProperty("strfryDir") ?: "/tmp/strfry-probe")
    private val count = System.getProperty("strfryCount")?.toInt() ?: 269_591
    private val span = 8_640_000L
    private val base = 1_777_000_000L

    @Test
    fun generateCorpus() {
        if (System.getProperty("strfryProbe") != "gen") return
        dir.mkdirs()
        val signer = NostrSignerSync()
        val corpus = File(dir, "corpus.jsonl").bufferedWriter()
        val entries = File(dir, "entries.tsv").bufferedWriter()
        var pubkey = ""
        corpus.use { c ->
            entries.use { e ->
                for (i in 0 until count) {
                    // Spread over the span the issue reports, in order, so the
                    // shape matches a provider publishing scores over months.
                    val createdAt = base + (i.toLong() * span) / count
                    val event: Event =
                        signer.sign(
                            createdAt,
                            30382,
                            arrayOf(arrayOf("d", "subject-$i"), arrayOf("rank", "${i % 100}")),
                            "",
                        )
                    pubkey = event.pubKey
                    c.write(event.toJson())
                    c.write("\n")
                    e.write("${event.createdAt}\t${event.id}\n")
                }
            }
        }
        File(dir, "author.txt").writeText(pubkey)
        println("probe: wrote $count kind-30382 event(s) for $pubkey to ${dir.absolutePath}")
    }

    @Test
    fun reconcileAgainstStrfry() {
        if (System.getProperty("strfryProbe") != "sync") return
        val url = RelayUrlNormalizer.normalize(System.getProperty("strfryUrl") ?: "ws://127.0.0.1:7777")
        val author = File(dir, "author.txt").readText().trim()
        val mine =
            File(dir, "entries.tsv").readLines().filter { it.isNotBlank() }.map {
                val (t, id) = it.split("\t")
                IdAndTime(t.toLong(), id)
            }
        val ask = Filter(kinds = listOf(30382), authors = listOf(author))

        val okhttp =
            OkHttpClient
                .Builder()
                .connectTimeout(Duration.ofSeconds(20))
                .pingInterval(Duration.ofSeconds(120))
                .build()
        val scope = CoroutineScope(SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
        try {
            val started = System.currentTimeMillis()
            val diff =
                runBlocking {
                    client.negentropyReconcileIds(url, ask, mine, idleTimeoutMs = 120_000L)
                }
            val share = diff.haveIds.size * 100.0 / mine.size
            println(
                "probe: mine=${mine.size} need=${diff.needIds.size} have=${diff.haveIds.size} ($share%)" +
                    " windows=${diff.windows} in ${System.currentTimeMillis() - started}ms",
            )
        } catch (e: Exception) {
            println("probe: reconcile FAILED — ${e.javaClass.simpleName}: ${e.message}")
        } finally {
            runCatching { runBlocking { client.disconnect() } }
        }
    }
}
