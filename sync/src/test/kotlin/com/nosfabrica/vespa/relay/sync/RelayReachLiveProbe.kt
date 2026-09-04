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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.relay.config.RelayIdentity
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.authSuccessMark
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.hasAuthResponder
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.OkHttpClient
import java.time.Duration
import kotlin.test.Test

/**
 * Dials every relay issue 185 names with the full `contentViaOutbox` ask and
 * prints, per relay, how the walk ended, what the relay said, whether our
 * NIP-42 AUTH was accepted, and where the refusal was about width, the cap
 * learned and whether the narrowed ask is served. Asserts nothing. Selected
 * by `-DrelayReachProbe=true`; `-DreachNsec`, `-DreachUrls`, `-DreachNoAuth` tune it.
 */
class RelayReachLiveProbe {
    @Test
    fun whichOfTheseRelaysCanThisRouterSync() {
        if (System.getProperty("relayReachProbe") != "true") {
            println("[skip] RelayReachLiveProbe — set -DrelayReachProbe=true to dial the public internet")
            return
        }
        val urls =
            System
                .getProperty("reachUrls")
                ?.split(",")
                ?.map { it.trim() to "given" }
                ?.filter { it.first.isNotEmpty() }
                ?: ISSUE_185
        val signer =
            System
                .getProperty("reachNsec")
                ?.trim()
                ?.takeIf { it.isNotEmpty() }
                ?.let { RelayIdentity.signerFor(it) }
                ?: NostrSignerInternal(KeyPair())

        val okhttp =
            OkHttpClient
                .Builder()
                .connectTimeout(Duration.ofSeconds(CONNECT_SEC))
                .pingInterval(Duration.ofSeconds(120))
                .build()
        val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
        // Exactly what `PeerClient` attaches when `RELAY_NSEC` is set; `-DreachNoAuth` is the control arm.
        val anonymous = System.getProperty("reachNoAuth") == "true"
        val authenticator =
            if (anonymous) null else RelayAuthenticator(client, scope) { _, template, _ -> listOf(signer.sign(template)) }
        val complaints = ClientRelayComplaints(client)
        val widths = FilterWidths()

        println("=".repeat(120))
        println(
            "RELAY-REACH — ${urls.size} url(s), ${CONTENT_KINDS.size}-kind ask, " +
                (if (anonymous) "ANONYMOUS (no NIP-42 responder)" else "as ${signer.pubKey.take(12)}…") +
                ", hasAuthResponder=${client.hasAuthResponder()}",
        )
        println("=".repeat(120))

        val rows =
            runBlocking {
                val gate = Semaphore(WIDTH)
                urls
                    .map { (raw, label) ->
                        async {
                            gate.withPermit {
                                withTimeoutOrNull(PER_URL_MS) { probe(client, complaints, widths, raw, label) }
                                    ?: Row(raw, label, "TIMED-OUT", 0, null, null, false, "no answer in ${PER_URL_MS / 1000}s")
                            }
                        }
                    }.awaitAll()
            }

        report(rows)

        runCatching { authenticator?.destroy() }
        runCatching { complaints.close() }
        runCatching { client.disconnect() }
        scope.cancel()
    }

    /** One relay's answer, and what this router could do about it. */
    private class Row(
        val url: String,
        /** Which of the issue's lists put it here. */
        val label: String,
        /** quartz's walk ending, or `SKIPPED`/`TIMED-OUT`. */
        val end: String,
        val events: Int,
        /** The width learned from the refusal, if any. */
        val cap: Int?,
        /** How the narrowed ask ended; null when nothing was narrowed. */
        val narrowedEnd: String?,
        /** Our NIP-42 AUTH was accepted on this connection. */
        val authed: Boolean,
        /** What the relay said for itself. */
        val said: String?,
        /** How many halvings it took to get under the relay's limit. See [PROBE_NARROWINGS]. */
        val narrowings: Int = 0,
    ) {
        /** `syncs` and `RESCUED` are one verdict reached two ways; only the second is evidence about narrowing. */
        val verdict: String
            get() =
                when {
                    end == "SKIPPED" || end == "TIMED-OUT" -> end.lowercase()
                    narrowedEnd != null && narrowedEnd !in REFUSALS -> "RESCUED"
                    end !in REFUSALS -> "syncs"
                    end == "AUTH_REQUIRED" -> if (authed) "auth-refused" else "auth-unanswered"
                    else -> "refused"
                }
    }

    /** The ask, then the narrowing where the relay refused on width, as `VisitPool.catchUp` does it. */
    private suspend fun probe(
        client: NostrClient,
        complaints: RelayComplaints,
        widths: FilterWidths,
        raw: String,
        label: String,
    ): Row {
        val url = RelayUrlNormalizer.normalize(raw)
        if (url.url.contains(".onion")) return Row(raw, label, "SKIPPED", 0, null, null, false, "hidden service, no Tor transport in this JVM")

        val markBefore = client.authSuccessMark(url)
        var events = 0
        val askedAtMs = System.currentTimeMillis()
        val walked = client.fetchAllPages(url, listOf(ASK), IDLE_MS) { events++ }
        val said = complaints.since(url, askedAtMs)
        val authed = client.authSuccessMark(url) > markBefore

        if (!VisitPool.refusedOutright(walked)) return Row(raw, label, walked.end.name, events, null, null, authed, said)

        // `learn` refuses every sentence that is not about kinds, which is most of this list.
        var narrowings = 0
        var narrowedEnd: String? = null
        var kindsAsked = ASK.kinds!!.size
        var complaint = said
        var lastAskedAt = askedAtMs
        while (narrowings < PROBE_NARROWINGS && widths.learn(url, complaint, kindsAsked)) {
            narrowings++
            narrowedEnd = null
            for (chunk in widths.chunk(url, ASK).take(CHUNKS_WALKED)) {
                lastAskedAt = System.currentTimeMillis()
                val again = client.fetchAllPages(url, listOf(chunk), IDLE_MS) { events++ }
                kindsAsked = chunk.kinds!!.size
                narrowedEnd = again.end.name
                if (VisitPool.refusedOutright(again)) break
            }
            complaint = complaints.since(url, lastAskedAt)
            if (narrowedEnd != null && narrowedEnd !in REFUSALS) break
        }
        return Row(raw, label, walked.end.name, events, widths.capFor(url), narrowedEnd, authed || client.authSuccessMark(url) > markBefore, complaint ?: said, narrowings)
    }

    private fun report(rows: List<Row>) {
        val byVerdict = rows.groupBy { it.verdict }
        for (verdict in listOf("syncs", "RESCUED", "auth-refused", "auth-unanswered", "refused", "timed-out", "skipped")) {
            val group = byVerdict[verdict].orEmpty().sortedBy { it.url }
            if (group.isEmpty()) continue
            println()
            println("--- $verdict (${group.size}) ".padEnd(120, '-'))
            for (r in group) {
                println(
                    "%-56s %-16s %-14s %5d ev  %s%s".format(
                        r.url.take(56),
                        r.label,
                        r.end,
                        r.events,
                        r.cap?.let { "cap=$it after ${r.narrowings} narrowing(s) -> ${r.narrowedEnd} " } ?: "",
                        r.said?.take(72)?.let { "“$it”" } ?: "",
                    ),
                )
            }
        }
        println()
        println("=".repeat(120))
        println(
            "TOTALS  " +
                listOf("syncs", "RESCUED", "auth-refused", "auth-unanswered", "refused", "timed-out", "skipped")
                    .mapNotNull { v -> byVerdict[v]?.size?.let { "$v=$it" } }
                    .joinToString("  "),
        )
        println("=".repeat(120))
    }

    companion object {
        /** quartz's endings that end a visit. See [VisitPool.refusedOutright]. */
        private val REFUSALS =
            setOf(
                PagedFetchResult.End.IDLE,
                PagedFetchResult.End.CLOSED,
                PagedFetchResult.End.AUTH_REQUIRED,
                PagedFetchResult.End.CANNOT_CONNECT,
                PagedFetchResult.End.UNPAGEABLE,
            ).map { it.name }.toSet()

        /** `contentViaOutbox`'s own kinds; the width is under test, so a stand-in list would not do. */
        private val CONTENT_KINDS =
            listOf(
                0,
                1,
                5,
                9,
                11,
                14,
                20,
                21,
                22,
                24,
                40,
                41,
                42,
                54,
                62,
                1010,
                1063,
                1065,
                1068,
                1111,
                1163,
                1301,
                1311,
                1312,
                1313,
                1315,
                1337,
                1617,
                1618,
                1621,
                1622,
                1630,
                1631,
                1632,
                1633,
                1808,
                1985,
                2003,
                2004,
                2473,
                3302,
                5050,
                5100,
                5129,
                5250,
                5302,
                5303,
                6969,
                8333,
                9002,
                9041,
                9321,
                9734,
                9735,
                9736,
                9737,
                9802,
                10002,
                10003,
                10009,
                10040,
                10100,
                10154,
                11871,
                12473,
                15128,
                15129,
                30000,
                30001,
                30002,
                30003,
                30004,
                30005,
                30006,
                30009,
                30015,
                30017,
                30018,
                30019,
                30020,
                30023,
                30030,
                30054,
                30055,
                30063,
                30175,
                30176,
                30177,
                30267,
                30296,
                30297,
                30298,
                30311,
                30312,
                30313,
                30315,
                30382,
                30383,
                30384,
                30385,
                30392,
                30393,
                30394,
                30395,
                30402,
                30617,
                30620,
                30817,
                30818,
                31337,
                31871,
                31872,
                31873,
                31890,
                31922,
                31923,
                31924,
                31925,
                31990,
                32267,
                33401,
                33863,
                34139,
                34235,
                34236,
                34550,
                35128,
                35129,
                36787,
                38000,
                38192,
                38383,
                39000,
                39089,
                39092,
                39701,
                40002,
                40100,
                45001,
                45003,
                48106,
            )

        /** Bounded, unlike the real leg: a relay refuses on the REQ, before an event moves, and `LIMIT_REACHED` is not a refusal. */
        private val ASK = Filter(kinds = CONTENT_KINDS, limit = LIMIT)

        private const val LIMIT = 5

        /** Shorter than the engine's idle window: most of these relays will refuse. */
        private const val IDLE_MS = 10_000L
        private const val CONNECT_SEC = 15L
        private const val PER_URL_MS = 90_000L

        /** Relays dialled at once. */
        private const val WIDTH = 12

        /** Chunks of a narrowed ask actually walked; two answer whether the width is accepted. */
        private const val CHUNKS_WALKED = 2

        /**
         * Further than the engine's [MAX_NARROWINGS], which bounds one visit's cost
         * and has revisits to finish the job. A probe has one visit and must reach
         * the relay's actual limit; eight halvings take 141 kinds to one.
         */
        private const val PROBE_NARROWINGS = 8
        private val ISSUE_185 =
            listOf(
                "wss://5vth22fdrkaxoeb75sehq2mjcl2gitmfhxwmq3cyar352hr6rmgnxfad.onion/chat" to "auth",
                "wss://aggr.nostr.land" to "auth+blocked",
                "wss://anon.computer" to "auth+restricted",
                "wss://armada.dreamith.to" to "blocked",
                "wss://auth.nostr1.com" to "auth",
                "wss://autisticos.spaces.coracle.social" to "auth+restricted",
                "wss://barcelona.bitcoinwalk.org" to "auth+restricted",
                "wss://bitstack.app" to "auth",
                "wss://boka-flotilla.spaces.coracle.social" to "auth+restricted",
                "wss://bostr.erechorse.com" to "restricted",
                "wss://bothy-relay.sybenx.workers.dev" to "auth+restricted",
                "wss://budabit.nostr1.com" to "auth",
                "wss://bunker.vanderwarker.family" to "blocked",
                "wss://buzz.ac2n-share.kozow.com" to "auth",
                "wss://buzz.cashu.space" to "rate",
                "wss://buzz.chrisdoc.dev" to "auth",
                "wss://buzz.pongsakorn.dev" to "auth",
                "wss://chat.bitcoinwalk.org" to "auth+restricted",
                "wss://chat.fujilegend.xyz" to "auth",
                "wss://comm.uat.qol.world" to "auth+restricted",
                "wss://comrelay.nostrdvm.com" to "auth+restricted",
                "wss://creatr.nostr.wine" to "auth",
                "wss://cyberspace.nostr1.com" to "auth",
                "wss://cynthia-swiftsfx1441g.tail7c1b50.ts.net:8443" to "restricted",
                "wss://david.nostr1.com" to "auth",
                "wss://dev-premium.nostreon.com" to "auth",
                "wss://devrelay.azzamo.net" to "auth+restricted",
                "wss://discovery.eu.nostria.app" to "width",
                "wss://discovery.us.nostria.app" to "width",
                "wss://dkkc.nostr1.com" to "auth",
                "wss://dweb.spaces.flotilla.social" to "auth+restricted",
                "wss://ehh2z745fqesutggjmru6o27yahq2hlwsvvegyz4bjzofa6c7c7e5nid.onion" to "auth",
                "wss://episessi-relay.triob.com" to "auth+restricted",
                "wss://futarchyhub.com/relay" to "auth",
                "wss://git.cloistr.xyz" to "width",
                "wss://git.vps.satsnode.xyz" to "auth",
                "wss://greensoul.space" to "auth+blocked",
                "wss://group.einundzwanzig.space" to "auth+restricted",
                "wss://groups.0xchat.com" to "blocked",
                "wss://groups.fiatjaf.com" to "blocked",
                "wss://groups.hzrd149.com" to "blocked",
                "wss://groups.lexingtonbitcoin.org" to "blocked",
                "wss://groups.satsdisco.com" to "blocked",
                "wss://groups.yugoatobe.com" to "blocked",
                "wss://h.codingarena.top/chat" to "auth+restricted",
                "wss://haven.calva.dev/chat" to "blocked",
                "wss://haven.eternal.gdn/chat" to "blocked",
                "wss://haven.girino.org/chat" to "auth+restricted",
                "wss://haven.girino.org/private" to "auth+restricted",
                "wss://haven.relayted.de/chat" to "blocked",
                "wss://haven.ronniesamuel.com/chat" to "blocked",
                "wss://haven.zanderhom.com/private" to "auth+restricted",
                "wss://hbr.coracle.social/chat" to "blocked",
                "wss://holyfit.scuba323.com/relay" to "restricted",
                "wss://hsuite-nostr-relay.hbarsuite.workers.dev" to "auth+width+blocked",
                "wss://inbox.nostr.wine" to "auth+blocked",
                "wss://indexer.nostrarchives.com" to "rate",
                "wss://infinity-radio-relay.digitalforlifeagency.workers.dev" to "blocked",
                "wss://inner.sebastix.social/inbox" to "rate",
                "wss://internal.coracle.social" to "auth+restricted",
                "wss://kanagrovv.kozow.com" to "auth+restricted",
                "wss://kasztanowa.bieda.it/internal" to "auth+restricted",
                "wss://laboratory.spaces.flotilla.social" to "auth+restricted",
                "wss://london.bitcoinwalk.org" to "auth+restricted",
                "wss://mercury-relay.imwald.eu" to "blocked",
                "wss://meta.bitcoinwalk.org" to "auth+restricted",
                "wss://meta.spaces.coracle.social" to "auth+restricted",
                "wss://mostro-p2p.tech" to "width",
                "wss://myvoiceourstory.org/internal" to "auth+restricted",
                "wss://namgoongjiwoo.nostr1.com" to "auth",
                "wss://nip17.com" to "auth",
                "wss://nostr-bridge.spaces.coracle.social" to "auth+restricted",
                "wss://nostr-relay.derekross.me/chat" to "blocked",
                "wss://nostr-relay.moctane.net" to "auth",
                "wss://purplerelay.com" to "width",
                "wss://relay.eu.whitenoise.chat" to "width",
                "wss://relay.getalby.com/v1" to "blocked",
                "wss://relay.internationalright-wing.org" to "width",
                "wss://relay.nostr.build" to "auth",
                "wss://relay.us.whitenoise.chat" to "width",
                "wss://support.flotilla.social" to "auth",
            )
    }
}
