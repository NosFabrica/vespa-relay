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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.peers.TorSettings
import com.nosfabrica.vespa.relay.peers.TorTransport
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.EmptyNostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The batch guards withhold what our own side may have caused, and only that. */
class FitnessGuardTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()
    private val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }

    private fun urls(
        prefix: String,
        n: Int,
    ) = (0 until n).map { RelayUrlNormalizer.normalize("wss://$prefix$it.example") }

    /**
     * Measures [urls] in one batch and returns each url's published grade, or null for none. [said]
     * is a url's terminal reason, or null for a relay that answers with [corpus].
     */
    private fun grades(
        urls: List<NormalizedRelayUrl>,
        reach: (NormalizedRelayUrl) -> Reach = { Reach.REACHABLE },
        tor: TorTransport? = null,
        tlsCheck: (NormalizedRelayUrl) -> TlsAnswer = { TlsAnswer.UNCHECKED },
        said: (NormalizedRelayUrl) -> String?,
    ): Map<NormalizedRelayUrl, String?> =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            FitnessPass(
                record = RelayVerdictRecord(store, signer),
                probe =
                    AliasProbe(
                        fetch = { url, want, until, _ ->
                            val reason = said(url)
                            if (reason == null) {
                                AliasProbe.Page(corpus.filter { until == null || it.createdAt <= until }.take(want))
                            } else {
                                AliasProbe.Page(events = null, reason = reason)
                            }
                        },
                        target = 40,
                        page = 40,
                        fallbackPage = 40,
                        idleMs = { 20L },
                    ),
                client = EmptyNostrClient(),
                progress = Processors().of("fitness"),
                tor = tor,
                tlsCheck = { tlsCheck(it) },
            ).measure("guard", urls, reach = { reach(it) }, onEvent = {}, sockets = Sockets.NONE)
            urls.associateWith { gradeOf(store, it) }
        }

    @Test
    fun `a server answering 5xx is not this router failing to dial`() {
        // A CDN incident answers for many relays at once; the relays that did answer are still graded.
        val erroring = urls("cdn", 40)
        val fine = urls("fine", 20)
        val graded = grades(erroring + fine) { if (it in erroring) "cannot:WebSocket Failure: Expected HTTP 101 response but was '503 Service Unavailable'" else null }
        assertTrue(erroring.all { graded[it] == null }, "a 5xx is a moment, not a verdict")
        assertTrue(fine.all { graded[it] == Verdict.PRIME.value }, "a batch of 5xx answers must not trip the guard against our dialling")
    }

    @Test
    fun `our Tor proxy being down holds back no clearnet verdict`() {
        val tor = TorTransport(TorSettings(socksHost = "127.0.0.1", socksPort = 1, routeAll = false, connectTimeoutSec = 5, maxSockets = 4), OkHttpClient())
        val onions = (0 until 60).map { RelayUrlNormalizer.normalize("ws://vespa${it}examplehiddenserviceaddressthatisnotrealabcdefghijklmn.onion") }
        val clearnet = urls("clear", 60)
        val graded = grades(onions + clearnet, reach = { if (tor.routes(it)) Reach.TRANSPORT_DOWN else Reach.REACHABLE }, tor = tor) { null }
        assertTrue(onions.all { graded[it] == null }, "our proxy being down was signed onto a hidden service")
        assertTrue(clearnet.all { graded[it] == Verdict.PRIME.value }, "Tor being down must not count as our clearnet dials failing")
    }

    @Test
    fun `a clearnet transport failing still counts against our dialling`() {
        // Our resolver or route failing across the batch is the case the guard exists for.
        val down = urls("down", 40)
        val fine = urls("fine", 20)
        val graded = grades(down + fine, reach = { if (it in down) Reach.TRANSPORT_DOWN else Reach.REACHABLE }) { null }
        assertTrue((down + fine).all { graded[it] == null }, "a batch our network could not dial must publish nothing")
    }

    @Test
    fun `a 5xx counts as a server reached for the dark-network guard`() {
        val gone = urls("gone", 1)
        val erroring = urls("cdn", 3)
        val graded =
            grades(gone + erroring, reach = { if (it in gone) Reach.PROVED_UNREACHABLE else Reach.REACHABLE }) {
                "cannot:WebSocket Failure: Expected HTTP 101 response but was '502 Bad Gateway'"
            }
        assertEquals(Verdict.DEAD.value, graded[gone.single()], "servers answering at all is our network working")
    }

    @Test
    fun `one server reached does not vouch for a small batch the pre-probe mostly failed`() {
        // Our resolver answering NXDOMAIN for everything still lets an IP literal or an onion through.
        val gone = urls("gone", 5)
        val lucky = urls("lucky", 1)
        val withheld = grades(gone + lucky, reach = { if (it in gone) Reach.PROVED_UNREACHABLE else Reach.REACHABLE }) { null }
        assertTrue(gone.all { withheld[it] == null }, "five urls signed dead beside one that answered")
        assertEquals(Verdict.PRIME.value, withheld[lucky.single()])

        // More servers reached than proved gone is a working network, and the proof stands.
        val few = urls("few", 2)
        val live = urls("live", 3)
        val published = grades(few + live, reach = { if (it in few) Reach.PROVED_UNREACHABLE else Reach.REACHABLE }) { null }
        assertTrue(few.all { published[it] == Verdict.DEAD.value })
    }

    @Test
    fun `a lookup the probe could not read is set aside, not counted as blind`() {
        // Many urls on one host share the JVM's cached failure; they must not trip the guard on the rest.
        val cached = urls("cached", 40)
        val fine = urls("fine", 20)
        val graded = grades(cached + fine, reach = { if (it in cached) Reach.UNEXPLAINED else Reach.REACHABLE }) { null }
        assertTrue(cached.all { graded[it] == null })
        assertTrue(fine.all { graded[it] == Verdict.PRIME.value })
    }

    private val pkix = "cannot:WebSocket Failure: PKIX path building failed (SSLHandshakeException)"

    @Test
    fun `a TLS failure our own direct handshake does not share is our transport and earns nothing`() {
        // An intercepting proxy on our egress fails every dial's handshake while the relay is fine.
        val proxied = urls("proxied", 3)
        val fine = urls("fine", 10)
        val graded = grades(proxied + fine, tlsCheck = { TlsAnswer.ACCEPTED }) { if (it in proxied) pkix else null }
        assertTrue(proxied.all { graded[it] == null }, "our own transport's TLS failure was signed onto someone else's relay")
        assertTrue(fine.all { graded[it] == Verdict.PRIME.value })
        // And a TLS failure nothing could check from here earns nothing either.
        val unchecked = grades(proxied + fine, tlsCheck = { TlsAnswer.UNCHECKED }) { if (it in proxied) pkix else null }
        assertTrue(proxied.all { unchecked[it] == null })
    }

    @Test
    fun `a certificate our own handshake also rejects is published dead`() {
        val expired = urls("expired", 1)
        val fine = urls("fine", 10)
        val graded = grades(expired + fine, tlsCheck = { TlsAnswer.REJECTED }) { if (it in expired) pkix else null }
        assertEquals(Verdict.DEAD.value, graded[expired.single()])
    }

    @Test
    fun `TLS failing for a large share of the batch is withheld as our truststore or clock`() {
        // A stale truststore or a skewed clock fails our direct handshake too, for every relay at once.
        val failing = urls("skewed", 10)
        val fine = urls("fine", 20)
        val graded = grades(failing + fine, tlsCheck = { TlsAnswer.REJECTED }) { if (it in failing) pkix else null }
        assertTrue(failing.all { graded[it] == null }, "a third of a batch failing TLS at once is our side")
        assertTrue(fine.all { graded[it] == Verdict.PRIME.value }, "only the TLS verdicts are withheld")
    }

    @Test
    fun `a TLS failure is never the evidence that our network reaches servers`() {
        // An interceptor answers the handshake itself, so it says nothing about reaching the relays.
        val gone = urls("gone", 1)
        val expired = urls("expired", 1)
        val refusing = urls("refusing", 4)
        val graded =
            grades(gone + expired + refusing, reach = { if (it in gone) Reach.PROVED_UNREACHABLE else Reach.REACHABLE }, tlsCheck = { TlsAnswer.REJECTED }) {
                if (it in expired) pkix else "cannot: java.net.ConnectException: Connection refused"
            }
        assertNull(graded[gone.single()], "a batch where only a TLS failure answered is a dark network")
        assertNull(graded[expired.single()])
    }

    /** The fitness grade the store carries for [url], or null for no record. */
    private suspend fun gradeOf(
        store: NostrSemanticsStore,
        url: NormalizedRelayUrl,
    ): String? =
        store
            .query<Event>(
                Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(signer.pubKey), tags = mapOf("d" to listOf(url.url))),
            ).flatMap { it.tags.toList() }
            .firstOrNull { it.size >= 3 && it[0] == "l" && it[2] == RelayVerdictRecord.FITNESS_NAMESPACE }
            ?.get(1)
}
