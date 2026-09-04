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
package com.nosfabrica.vespa.relay.server

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import com.vitorpamplona.quartz.nip42RelayAuth.tags.ChallengeTag
import com.vitorpamplona.quartz.nip42RelayAuth.tags.RelayTag
import com.vitorpamplona.quartz.utils.TimeUtils
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * NIP-42 when the same relay answers at a clearnet url and a hidden service: a Tor client signs the
 * address it dialled. [MultiAddressAuthPolicy] re-states quartz's checks, so each rejection is pinned too.
 */
class RelayOnionAuthTest {
    private val clearnet = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val onion = RelayUrlNormalizer.normalize("ws://${"n".repeat(56)}.onion")
    private val someoneElse = RelayUrlNormalizer.normalize("wss://relay.example.com")
    private val signer = NostrSignerSync()

    /** A relay whose second address is whatever [alsoAt] answers when asked. */
    private fun relayServing(alsoAt: () -> Set<NormalizedRelayUrl>) =
        NostrRelayServer(
            NostrSemanticsStore(InMemoryEventIndex(), relay = clearnet),
            clearnet,
            alsoServedAt = alsoAt,
        )

    /** Runs one connection: sends [authFor] against the relay's challenge and returns the relay's `OK` line for it. */
    private fun okFor(
        server: NostrRelayServer,
        authFor: (challenge: String) -> RelayAuthEvent,
    ): String =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                val challenge =
                    awaitMessage(out) { it.startsWith("""["AUTH",""") }
                        .substringAfter("""["AUTH","""")
                        .substringBefore('"')
                val auth = authFor(challenge)
                session.receive("""["AUTH",${auth.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${auth.id}"""") }
            } finally {
                session.close()
            }
        }

    @Test
    fun `an auth signed for the onion address authenticates`() {
        val server = relayServing { setOf(onion) }
        try {
            val ok = okFor(server) { signer.sign(RelayAuthEvent.build(onion, it)) }
            assertTrue(ok.contains(",true"), "the address Tor clients dial must authenticate: $ok")
        } finally {
            server.close()
        }
    }

    @Test
    fun `the clearnet address still authenticates while a second one is served`() {
        val server = relayServing { setOf(onion) }
        try {
            val ok = okFor(server) { signer.sign(RelayAuthEvent.build(clearnet, it)) }
            assertTrue(ok.contains(",true"), "RELAY_URL must keep working: $ok")
        } finally {
            server.close()
        }
    }

    /** The set is asked per connection: Tor mints the service's key minutes after this process starts serving. */
    @Test
    fun `an address published after boot is accepted by the next connection`() {
        var known = emptySet<NormalizedRelayUrl>()
        val server = relayServing { known }
        try {
            val before = okFor(server) { signer.sign(RelayAuthEvent.build(onion, it)) }
            assertTrue(before.contains(",false"), "nothing published yet, so nothing to accept: $before")

            known = setOf(onion)

            val after = okFor(server) { signer.sign(RelayAuthEvent.build(onion, it)) }
            assertTrue(after.contains(",true"), "the address Tor published mid-run must be accepted: $after")
        } finally {
            server.close()
        }
    }

    /** Quartz's own `RelayAuthEvent.create(relays, …)` names several relays; the per-connection challenge keeps that safe. */
    @Test
    fun `an auth naming several relays authenticates if any of them is us`() {
        val server = relayServing { setOf(onion) }
        try {
            val ok =
                okFor(server) { challenge ->
                    signer.sign(
                        TimeUtils.now(),
                        RelayAuthEvent.KIND,
                        arrayOf(
                            RelayTag.assemble(someoneElse),
                            RelayTag.assemble(onion),
                            ChallengeTag.assemble(challenge),
                        ),
                        "",
                    )
                }
            assertTrue(ok.contains(",true"), "one tag naming this relay is enough: $ok")
        } finally {
            server.close()
        }
    }

    /** A hidden service is published on port 80, and the normalizer keeps `…onion:80` and `…onion` apart. */
    @Test
    fun `the default port spelled out is the same address`() {
        val server = relayServing { setOf(onion) }
        try {
            val explicit = RelayUrlNormalizer.normalize("${onion.url.removeSuffix("/")}:80")
            val ok = okFor(server) { signer.sign(RelayAuthEvent.build(explicit, it)) }
            assertTrue(ok.contains(",true"), "ws://…onion:80 is ws://…onion: $ok")
        } finally {
            server.close()
        }
    }

    /** Only the default port folds; another port is another endpoint. */
    @Test
    fun `a different port on our own host is not our address`() {
        val server = relayServing { setOf(onion) }
        try {
            val elsewhere = RelayUrlNormalizer.normalize("${onion.url.removeSuffix("/")}:7777")
            val ok = okFor(server) { signer.sign(RelayAuthEvent.build(elsewhere, it)) }
            assertTrue(ok.contains(",false"), "we do not answer there: $ok")
        } finally {
            server.close()
        }
    }

    @Test
    fun `an auth for a relay we do not serve is rejected`() {
        val server = relayServing { setOf(onion) }
        try {
            val ok = okFor(server) { signer.sign(RelayAuthEvent.build(someoneElse, it)) }
            assertTrue(ok.contains(",false"), "an auth event for another relay is not ours to accept: $ok")
        } finally {
            server.close()
        }
    }

    @Test
    fun `a wrong challenge is still rejected on the second address`() {
        val server = relayServing { setOf(onion) }
        try {
            val ok = okFor(server) { signer.sign(RelayAuthEvent.build(onion, "not-the-challenge-we-sent")) }
            assertTrue(ok.contains(",false"), "the challenge check must survive the widening: $ok")
        } finally {
            server.close()
        }
    }

    /** NIP-40 on an auth event: signed now, but already dead. */
    @Test
    fun `an expired auth event is still rejected on the second address`() {
        val server = relayServing { setOf(onion) }
        try {
            val ok =
                okFor(server) { challenge ->
                    signer.sign(
                        TimeUtils.now(),
                        RelayAuthEvent.KIND,
                        arrayOf(
                            RelayTag.assemble(onion),
                            ChallengeTag.assemble(challenge),
                            arrayOf("expiration", (TimeUtils.now() - 60).toString()),
                        ),
                        "",
                    )
                }
            assertTrue(ok.contains(",false"), "an expired auth event must not authenticate: $ok")
        } finally {
            server.close()
        }
    }

    @Test
    fun `a stale created_at is still rejected on the second address`() {
        val server = relayServing { setOf(onion) }
        try {
            val stale = TimeUtils.now() - 3_600
            val ok = okFor(server) { signer.sign(RelayAuthEvent.build(onion, it, createdAt = stale)) }
            assertTrue(ok.contains(",false"), "an hour-old auth event must not authenticate: $ok")
        } finally {
            server.close()
        }
    }

    /**
     * The login hook fires once per identity per connection. Quartz accepts every copy of a
     * still-fresh AUTH, and each copy would otherwise start another walk of the store on a scope
     * the socket's close does not cancel.
     */
    @Test
    fun `a replayed auth authenticates again but is only acted on once`() =
        runBlocking {
            val seen = Collections.synchronizedList(mutableListOf<String>())
            val policy = MultiAddressAuthPolicy(clearnet, emptySet()) { pubkey, _ -> seen.add(pubkey) }
            policy.onConnect(
                object : RequestContext {
                    override val connectionId = 1L
                    override val policy = policy
                    override val authenticatedUsers = emptySet<String>()
                },
                {},
            )

            val auth = signer.sign(RelayAuthEvent.build(clearnet, policy.challenge))
            repeat(3) { policy.authorize(auth) }
            assertEquals(listOf(signer.pubKey), seen.toList(), "three identical frames, one walk of the store")

            // A different identity on the same connection is still owed one.
            val other = NostrSignerSync()
            policy.authorize(other.sign(RelayAuthEvent.build(clearnet, policy.challenge)))
            assertEquals(listOf(signer.pubKey, other.pubKey), seen.toList())
        }

    private fun awaitMessage(
        out: List<String>,
        match: (String) -> Boolean,
    ): String {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            synchronized(out) { out.firstOrNull(match) }?.let { return it }
            Thread.sleep(20)
        }
        fail("timed out waiting for a matching relay message; got: $out")
    }
}
