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

import com.nosfabrica.vespa.relay.config.RelayIdentity
import com.nosfabrica.vespa.relay.router.discovery.RelayFacts
import com.nosfabrica.vespa.relay.router.discovery.RelayVerdictRecord
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.publishAndCollectResults
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.time.Duration
import kotlin.test.Test

/**
 * SEED A LOCAL RELAY WITH A MONITOR CORPUS, so the stats page's verdicts panel
 * can be driven against a real store instead of a fixture.
 *
 * The panel is the one part of `stats.html` that speaks the protocol: it pages
 * kind 30166 off the relay's own websocket, scoped to the relay's own key, and
 * renders whatever comes back. Every unit test of it hands the parser events
 * from an array — which is exactly the shape that has been wrong twice, because
 * what breaks a panel at scale is the record it did not expect on page nine.
 *
 * So this writes the corpus a monitor would: [count] urls across a realistic
 * spread of hosts, each carrying a NIP-32 grade under
 * [RelayVerdictRecord.FITNESS_NAMESPACE], the descriptive facts a fitness pass
 * publishes beside it, and — for [legacyShare] of them — the OLD grade still on
 * `s`, which is what a store looks like before the boot migration has run.
 *
 * It publishes THROUGH THE RELAY'S OWN WEBSOCKET rather than writing to the
 * store directly, so the seed also exercises the accept path the records really
 * arrive by.
 *
 * ```bash
 * ./gradlew :sync:test --tests '*VerdictPanelSeedProbe*' -DseedVerdicts=true \
 *   -DseedVerdictsNsec=nsec1… --rerun -i
 * ```
 *
 * The nsec must be the relay's own `RELAY_NSEC`: the panel scopes its read to
 * the key the relay publishes as `self` in its NIP-11 document, so records
 * signed by anything else are correctly counted as another monitor's and drawn
 * on no row at all. Asserts nothing — the page is the verdict.
 */
class VerdictPanelSeedProbe {
    @Test
    fun seedVerdicts() {
        if (System.getProperty("seedVerdicts") != "true") {
            println("[skip] VerdictPanelSeedProbe — set -DseedVerdicts=true to seed a LOCAL relay's monitor corpus")
            return
        }
        val target = RelayUrlNormalizer.normalize(System.getProperty("seedVerdictsUrl") ?: "ws://localhost:7777")
        val count = System.getProperty("seedVerdictsCount")?.toIntOrNull() ?: 600
        val legacyShare = System.getProperty("seedVerdictsLegacy")?.toIntOrNull() ?: 15
        // The relay's OWN key, through the same decoder the relay reads
        // RELAY_NSEC with — a seed signed by anything else is correctly drawn
        // as another monitor's and lands on no row.
        val signer =
            System.getProperty("seedVerdictsNsec")?.let { RelayIdentity.signerFor(it) }
                ?: NostrSignerInternal(KeyPair()).also {
                    println("  no -DseedVerdictsNsec: signing as a STRANGER, the panel will count these as another monitor's")
                }

        val okhttp = OkHttpClient.Builder().connectTimeout(Duration.ofSeconds(10)).build()
        val scope = CoroutineScope(SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
        val now = System.currentTimeMillis() / 1000
        var ok = 0
        var failed = 0
        try {
            runBlocking {
                for (i in 0 until count) {
                    val event = record(signer, i, now, legacy = i % 100 < legacyShare)
                    val result = client.publishAndCollectResults(event, setOf(target), 15_000L)[target]
                    if (result?.accepted == true) ok++ else failed++
                    if ((i + 1) % 100 == 0) println("  ${i + 1}/$count published")
                }
            }
            println("seeded $ok record(s) into ${target.url} ($failed refused), signed by ${signer.pubKey.take(12)}…")
            println("  open http://localhost:7777/stats.html and press “Read verdicts from this relay”")
        } finally {
            scope.cancel()
        }
    }

    /**
     * One record, in the shape the fitness pass writes — grade, evidence, the
     * measured facts — with the host spread and the path shapes a real corpus
     * has: a few hosts wearing dozens of minted paths, most wearing one.
     */
    private suspend fun record(
        signer: NostrSignerInternal,
        i: Int,
        now: Long,
        legacy: Boolean,
    ): Event {
        // A handful of crowded hosts, then a long tail — the distribution the
        // panel's grouping exists for, and the one that makes a group box worth
        // drawing at all.
        val host = if (i % 5 == 0) "crowded${i % 4}.example" else "relay$i.example"
        val url = if (i % 5 == 0) "wss://$host/minted-$i" else "wss://$host/"
        val grade = GRADES[i % GRADES.size]
        val facts =
            RelayFacts(
                network = if (i % 40 == 0) "tor" else "clearnet",
                rttOpenMs = (20 + (i * 7) % 400).toLong(),
                rttReadMs = (30 + (i * 13) % 900).toLong(),
                requirements = if (i % 9 == 0) listOf("auth", "!payment") else listOf("!auth"),
                software = SOFTWARE[i % SOFTWARE.size],
                version = "1.${i % 9}.0",
                supportedNips = listOf(1, 11, 50),
            )
        val at = (now - (i % 20) * 3600).toString()
        val tags =
            buildList {
                add(arrayOf("d", url))
                if (legacy) {
                    // What a record signed BEFORE the grade move looks like:
                    // the grade squatting the software field, no label at all.
                    add(arrayOf(RelayVerdictRecord.LEGACY_STATUS_TAG, if (grade == "prime") "syncable" else grade, "an older build", at, "1"))
                } else {
                    add(
                        arrayOf(
                            RelayVerdictRecord.LABEL_TAG,
                            grade,
                            RelayVerdictRecord.FITNESS_NAMESPACE,
                            "answered ${20 + i % 30} events at a settled anchor",
                            at,
                            RelayVerdictRecord.FITNESS_EPOCH,
                        ),
                    )
                    add(arrayOf(RelayVerdictRecord.LABEL_NAMESPACE_TAG, RelayVerdictRecord.FITNESS_NAMESPACE))
                    addAll(facts.tags())
                }
                // A foreign labeller on the same record — the case the namespace
                // check exists for, and the one a panel matching on tag NAME
                // would draw as a grade.
                if (i % 7 == 0) {
                    add(arrayOf("l", "CA", "countryCode"))
                    add(arrayOf("L", "countryCode"))
                }
                // Some urls also carry the fold's verdict, so the panel's three
                // verdicts are drawn together the way they are in production.
                if (i % 5 == 0 && i % 10 != 0) {
                    add(arrayOf(RelayVerdictRecord.SAME_AS_TAG, "wss://$host/", "500 newest events, 498 shared", at, RelayVerdictRecord.FOLD_EPOCH))
                }
                if (i % 11 == 0) {
                    add(
                        arrayOf(
                            RelayVerdictRecord.SELF_CONSISTENT_TAG,
                            if (i % 33 == 0) "false" else "true",
                            "500 + 500 events at a 7d anchor",
                            at,
                            RelayVerdictRecord.CONSISTENCY_EPOCH,
                        ),
                    )
                }
                // A tag no reader here knows, so the panel's "+N other tag(s)"
                // counter has something honest to count.
                if (i % 23 == 0) add(arrayOf("something-new", "42"))
            }.toTypedArray()
        return signer.sign(now - (i % 20) * 3600, 30166, tags, "")
    }

    private companion object {
        /** Weighted the way a real sweep comes out: mostly prime, then the refusals. */
        val GRADES =
            listOf("prime", "prime", "prime", "prime", "prime", "prime", "dead", "dead", "alias", "silent", "inconsistent", "unpageable", "auth-refused", "restricted")

        val SOFTWARE =
            listOf(
                "git+https://github.com/hoytech/strfry.git",
                "https://git.sr.ht/~gheartsfield/nostr-rs-relay",
                "https://github.com/barrydeen/haven",
                "https://github.com/fiatjaf/pyramid",
                "https://github.com/Spl0itable/nosflare",
            )
    }
}
