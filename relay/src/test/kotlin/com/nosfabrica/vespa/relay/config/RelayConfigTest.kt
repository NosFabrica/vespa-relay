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

import com.vitorpamplona.quartz.nip19Bech32.toNpub
import com.vitorpamplona.quartz.nip77Negentropy.NegentropySettings
import com.vitorpamplona.quartz.utils.Hex
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RelayConfigTest {
    @Test
    fun `limits default when env is empty`() {
        val d = relayLimitsFromEnv(emptyMap())
        assertEquals(20, d.maxFilters)
        assertEquals(50, d.maxSubscriptions)
        assertEquals(5_000, d.maxLimit)
        // A REQ that names no `limit` gets the same window as one asking for the
        // ceiling: the default is not a smaller, quieter cap.
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
                    "MAX_SUBSCRIPTIONS" to "not-a-number", // ignored -> default stands
                    "CREATED_AT_UPPER_LIMIT" to "1900000000",
                ),
            )
        assertEquals(7, limits.maxFilters)
        assertEquals(999, limits.maxLimit)
        assertEquals(50, limits.maxSubscriptions) // default kept
        assertEquals(1_900_000_000L, limits.createdAtUpperLimit)
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
        // This used to filter silently, which is the worst possible outcome for an
        // admin list: the relay starts, reports the key count it was given minus
        // the ones it threw away, and the missing admin only finds out when a
        // NIP-86 call is refused. Same for a deny list — a ban that is not
        // enforced looks exactly like a ban that was never configured.
        val good = "npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqshp52w2"
        listOf("not-hex", "b".repeat(63), "b".repeat(64), "npub1nope").forEach { junk ->
            assertFailsWith<IllegalArgumentException>("'$junk' must not be silently dropped") {
                adminPubkeysFromEnv(mapOf("RELAY_ADMIN_PUBKEYS" to "$good, $junk"))
            }
        }
    }

    @Test
    fun `bare hex is refused without echoing the value back`() {
        // Hex has no checksum, so one mistyped character is a different valid
        // key. The error explains the fix but must NOT carry the value in any
        // form: 64 hex characters might equally be a hex-encoded SECRET pasted
        // into the wrong slot, and boot logs are shipped to aggregators.
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
        // `DENY_KINDS=4;5` silently denying nothing is a ban that is not
        // enforced — the exact silent-inert failure the pubkey lists refuse.
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
    fun `expiration sweep interval defaults to an hour`() {
        assertEquals(3_600L, expirationSweepSecondsFromEnv(emptyMap()))
        assertEquals(60L, expirationSweepSecondsFromEnv(mapOf("EXPIRATION_SWEEP_SECONDS" to "60")))
    }
}
