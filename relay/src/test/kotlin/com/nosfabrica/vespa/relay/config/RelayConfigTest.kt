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
package com.nosfabrica.vespa.relay.config

import com.nosfabrica.vespa.eventstore.search.SearchExpansionLimits
import com.vitorpamplona.quartz.nip19Bech32.toNpub
import com.vitorpamplona.quartz.nip77Negentropy.NegentropySettings
import com.vitorpamplona.quartz.utils.Hex
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RelayConfigTest {
    @Test
    fun `limits default when env is empty`() {
        val d = relayLimitsFromEnv(emptyMap())
        assertEquals(20, d.maxFilters)
        assertEquals(50, d.maxSubscriptions)
        assertEquals(5_000, d.maxLimit)
        // A REQ naming no `limit` gets the same window as one asking for the ceiling.
        assertEquals(5_000, d.defaultLimit)
        assertEquals(false, d.authRequired)
    }

    @Test
    fun `limits override only the fields set, ignoring garbage`() {
        val limits =
            relayLimitsFromEnv(
                mapOf(
                    "MAX_FILTERS" to "7",
                    "MAX_LIMIT" to "999",
                    "MAX_SUBSCRIPTIONS" to "not-a-number",
                    "CREATED_AT_UPPER_LIMIT" to "1900000000",
                ),
            )
        assertEquals(7, limits.maxFilters)
        assertEquals(999, limits.maxLimit)
        assertEquals(50, limits.maxSubscriptions)
        assertEquals(1_900_000_000L, limits.createdAtUpperLimit)
    }

    @Test
    fun `an unreadable expansion cap keeps the default rather than disabling the splice`() {
        val d = SearchExpansionLimits.Default
        // A negative coerced to zero reads like a corpus with no trust records.
        for (bad in listOf("-1", "-1000", "many", "", "  ")) {
            val got = searchExpansionFromEnv(mapOf("SEARCH_EXPAND_MAX_PER_EVENT" to bad, "SEARCH_EXPAND_MAX_TOTAL" to bad))
            assertEquals(d.maxPerEvent, got.maxPerEvent, "SEARCH_EXPAND_MAX_PER_EVENT=$bad")
            assertEquals(d.maxPerRequest, got.maxPerRequest, "SEARCH_EXPAND_MAX_TOTAL=$bad")
            assertTrue(got.enabled, "a bad cap is not a way to turn the feature off")
        }
    }

    @Test
    fun `zero is honoured as zero, and off is its own switch`() {
        // Zero is a real cap; off is `SEARCH_EXPAND_REFERENCES`, spelled differently.
        val zero = searchExpansionFromEnv(mapOf("SEARCH_EXPAND_MAX_PER_EVENT" to "0"))
        assertEquals(0, zero.maxPerEvent)
        assertTrue(zero.enabled)

        for (off in listOf("false", "0", "no", "off", "OFF")) {
            assertFalse(searchExpansionFromEnv(mapOf("SEARCH_EXPAND_REFERENCES" to off)).enabled, off)
        }
        assertTrue(searchExpansionFromEnv(mapOf("SEARCH_EXPAND_REFERENCES" to "flase")).enabled, "a typo leaves it ON")
        assertEquals(SearchExpansionLimits.Default, searchExpansionFromEnv(emptyMap()))
    }

    @Test
    fun `negentropy defaults to the strfry-parity Default`() {
        assertEquals(NegentropySettings.Default, negentropySettingsFromEnv(emptyMap()))
    }

    @Test
    fun `negentropy overrides the caps that are set`() {
        val neg =
            negentropySettingsFromEnv(
                mapOf(
                    "NEG_FRAME_SIZE_LIMIT" to "120000",
                    "NEG_MAX_SYNC_EVENTS" to "42",
                ),
            )
        assertEquals(120_000L, neg.frameSizeLimit)
        assertEquals(42, neg.maxSyncEvents)
        assertEquals(NegentropySettings.Default.maxSessionsPerConnection, neg.maxSessionsPerConnection)
    }

    @Test
    fun `admin pubkeys decode to lowercase hex, deduped`() {
        val a = "a".repeat(64)
        val c = "c".repeat(64)
        val keys =
            adminPubkeysFromEnv(
                mapOf("RELAY_ADMIN_PUBKEYS" to "${Hex.decode(a).toNpub()}, ${Hex.decode(a).toNpub()}, ${Hex.decode(c).toNpub()}"),
            )
        assertEquals(setOf(a, c), keys)
    }

    @Test
    fun `a key that cannot be read stops the relay instead of being dropped`() {
        // A dropped admin or ban looks exactly like one never configured.
        val good = "npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqshp52w2"
        listOf("not-hex", "b".repeat(63), "b".repeat(64), "npub1nope").forEach { junk ->
            assertFailsWith<IllegalArgumentException>("'$junk' must not be silently dropped") {
                adminPubkeysFromEnv(mapOf("RELAY_ADMIN_PUBKEYS" to "$good, $junk"))
            }
        }
    }

    @Test
    fun `bare hex is refused without echoing the value back`() {
        // Hex has no checksum, and the error must not carry the value in any form: 64 hex
        // characters may be a secret pasted into the wrong slot.
        val hex = "0000000000000000000000000000000000000000000000000000000000000001"
        val npub = "npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqshp52w2"
        val e =
            assertFailsWith<IllegalArgumentException> {
                adminPubkeysFromEnv(mapOf("RELAY_ADMIN_PUBKEYS" to hex))
            }
        assertEquals(true, e.message!!.contains("bare hex"), "got: ${e.message}")
        assertEquals(false, e.message!!.contains(npub), "the error must not re-encode the value: ${e.message}")
        assertEquals(false, e.message!!.contains(hex), "the error must not echo the value: ${e.message}")
        assertEquals(setOf(hex), adminPubkeysFromEnv(mapOf("RELAY_ADMIN_PUBKEYS" to npub)))
    }

    @Test
    fun `an all-uppercase npub is valid bech32 and decodes`() {
        // BIP-173 allows the all-caps spelling; QR exports produce it.
        val hex = "0000000000000000000000000000000000000000000000000000000000000001"
        val npub = "npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqshp52w2"
        assertEquals(setOf(hex), adminPubkeysFromEnv(mapOf("RELAY_ADMIN_PUBKEYS" to npub.uppercase())))
    }

    @Test
    fun `an npub with a typo is called an npub, not a character count`() {
        val npub = "npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqshp52w3"
        val e =
            assertFailsWith<IllegalArgumentException> {
                adminPubkeysFromEnv(mapOf("RELAY_ADMIN_PUBKEYS" to npub))
            }
        assertEquals(true, e.message!!.contains("recopy"), "got: ${e.message}")
    }

    @Test
    fun `an nsec in a pubkey setting is called out by name`() {
        val e =
            assertFailsWith<IllegalArgumentException> {
                adminPubkeysFromEnv(mapOf("RELAY_ADMIN_PUBKEYS" to "nsec1${"q".repeat(58)}"))
            }
        assertEquals(true, e.message!!.contains("PRIVATE key"), "got: ${e.message}")
    }

    @Test
    fun `no admin pubkeys when unset`() {
        assertEquals(emptySet(), adminPubkeysFromEnv(emptyMap()))
    }

    @Test
    fun `write allow and deny lists parse pubkeys and kinds`() {
        val a = "a".repeat(64)
        val b = "b".repeat(64)
        val env =
            mapOf(
                "ALLOW_PUBKEYS" to "${Hex.decode(a).toNpub()} ${Hex.decode(b).toNpub()}",
                "DENY_PUBKEYS" to "",
                "ALLOW_KINDS" to "0,1, 30023",
                "DENY_KINDS" to "4",
            )
        assertEquals(setOf(a, b), allowPubkeysFromEnv(env))
        assertEquals(emptySet(), denyPubkeysFromEnv(env))
        assertEquals(setOf(0, 1, 30023), allowKindsFromEnv(env))
        assertEquals(setOf(4), denyKindsFromEnv(env))
    }

    @Test
    fun `a kind list with junk fails loudly like a pubkey list does`() {
        // `DENY_KINDS=4;5` denying nothing is a ban that is not enforced.
        assertFailsWith<IllegalArgumentException> {
            denyKindsFromEnv(mapOf("DENY_KINDS" to "4, junk"))
        }
        assertFailsWith<IllegalArgumentException> {
            allowKindsFromEnv(mapOf("ALLOW_KINDS" to "4;5"))
        }
    }

    @Test
    fun `reject-future defaults off and clamps negatives`() {
        assertEquals(0, rejectFutureSecondsFromEnv(emptyMap()))
        assertEquals(900, rejectFutureSecondsFromEnv(mapOf("REJECT_FUTURE_SECONDS" to "900")))
        assertEquals(0, rejectFutureSecondsFromEnv(mapOf("REJECT_FUTURE_SECONDS" to "-5")))
    }

    @Test
    fun `the read-lens gate is on unless it is turned off in as many words`() {
        assertTrue(requireReadLensFromEnv(emptyMap()), "unset is ON — the relay's default before AUTH")
        for (off in listOf("false", "0", "no", "off", "FALSE", " no ")) {
            assertFalse(requireReadLensFromEnv(mapOf("REQUIRE_READ_LENS" to off)), "\"$off\" turns it off")
        }
        // Anything else is on, typos included: a typo that opened the corpus would go unnoticed.
        for (on in listOf("true", "1", "yes", "treu", "", "  ")) {
            assertTrue(requireReadLensFromEnv(mapOf("REQUIRE_READ_LENS" to on)), "\"$on\" leaves it on")
        }
    }

    @Test
    fun `expiration sweep interval defaults to an hour`() {
        assertEquals(3_600L, expirationSweepSecondsFromEnv(emptyMap()))
        assertEquals(60L, expirationSweepSecondsFromEnv(mapOf("EXPIRATION_SWEEP_SECONDS" to "60")))
    }
}
