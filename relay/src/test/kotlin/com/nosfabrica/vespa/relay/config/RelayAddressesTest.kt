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

import kotlin.io.path.createTempDirectory
import kotlin.io.path.deleteExisting
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * How the relay learns the address of the hidden service in front of it.
 *
 * The property that matters is timing: Tor mints the address the first time
 * the service starts, so it can appear long after this process began serving
 * — and a relay that only looked once, at boot, would reject every AUTH from
 * Tor until someone restarted it for reasons no log explained.
 */
class RelayAddressesTest {
    private val hostname = "${"q".repeat(56)}.onion"

    /**
     * The file is looked at once a second at most, so every assertion about
     * what a LATER look sees has to move the clock rather than sleep through
     * it. Starts at a negative reading on purpose: `System.nanoTime()` has no
     * origin, and code that only works on positive readings is code that works
     * on this machine.
     */
    private class MovableClock(
        var nanos: Long = -5_000_000_000L,
    ) : () -> Long {
        override fun invoke() = nanos

        fun advanceASecond() {
            nanos += 1_100_000_000L
        }
    }

    @Test
    fun `no configuration means no second address`() {
        assertEquals(emptySet(), relayAddressesFromEnv(emptyMap()).alternates())
    }

    @Test
    fun `a declared onion url is accepted, normalized as a relay url`() {
        val addresses = relayAddressesFromEnv(mapOf("RELAY_ONION_URL" to hostname))
        assertEquals(setOf("ws://$hostname/"), addresses.alternates().map { it.url }.toSet())
    }

    /**
     * Fatal, like a malformed `RELAY_URL`. An address the operator typed and we
     * then ignored is a Tor endpoint whose clients silently lose their ranking
     * lens — the failure mode this whole file exists to prevent.
     */
    @Test
    fun `a malformed onion url stops the boot`() {
        assertFailsWith<IllegalArgumentException> {
            relayAddressesFromEnv(mapOf("RELAY_ONION_URL" to "not a relay"))
        }
    }

    @Test
    fun `an address published after the first look is picked up on the next`() {
        val dir = createTempDirectory("onion")
        val file = dir.resolve("hostname")
        val said = mutableListOf<String>()
        val clock = MovableClock()
        val addresses = RelayAddresses(hostnameFile = file, announce = { said += it }, nanoTime = clock)

        assertEquals(emptySet(), addresses.alternates(), "nothing to report before Tor writes the file")
        assertTrue(said.isEmpty(), "a missing file is the normal state of a relay with no hidden service: $said")

        file.writeText("$hostname\n")
        clock.advanceASecond()

        assertEquals(setOf("ws://$hostname/"), addresses.alternates().map { it.url }.toSet())
        assertEquals(1, said.size, "the address is announced exactly once, when it appears: $said")
        assertTrue(hostname in said.first(), "the operator must be able to read the address off the log: $said")

        // Cached: the announcement does not repeat on every later connection.
        addresses.alternates()
        assertEquals(1, said.size, "announced once, not once per connection: $said")
    }

    /**
     * `docker compose down -v` on the key volume mints a NEW service, and the
     * container republishes. A relay that had cached the first address would
     * reject every auth event from the second one — the same downgrade this
     * class exists to prevent, in the shape hardest to diagnose, since the
     * endpoint itself works.
     */
    @Test
    fun `a rotated address replaces the one it replaced`() {
        val dir = createTempDirectory("onion")
        val file = dir.resolve("hostname")
        val clock = MovableClock()
        val addresses = RelayAddresses(hostnameFile = file, announce = {}, nanoTime = clock)

        file.writeText(hostname)
        assertEquals(setOf("ws://$hostname/"), addresses.alternates().map { it.url }.toSet())

        val rotated = "${"r".repeat(56)}.onion"
        file.writeText(rotated)
        clock.advanceASecond()
        // Explicit, so the assertion does not depend on the filesystem's
        // timestamp resolution: two writes in the same second can share an
        // mtime, and the re-read is keyed on the mtime moving.
        file.toFile().setLastModified(System.currentTimeMillis() + 2_000)

        assertEquals(setOf("ws://$rotated/"), addresses.alternates().map { it.url }.toSet())
    }

    /** A hidden service does not stop existing because a mount blinked. */
    @Test
    fun `a hostname file that disappears leaves the address in place`() {
        val dir = createTempDirectory("onion")
        val file = dir.resolve("hostname")
        val clock = MovableClock()
        val addresses = RelayAddresses(hostnameFile = file, announce = {}, nanoTime = clock)

        file.writeText(hostname)
        assertEquals(setOf("ws://$hostname/"), addresses.alternates().map { it.url }.toSet())

        file.deleteExisting()
        clock.advanceASecond()
        assertEquals(setOf("ws://$hostname/"), addresses.alternates().map { it.url }.toSet())
    }

    @Test
    fun `a file holding something that is not an address is reported, not used`() {
        val dir = createTempDirectory("onion")
        val file = dir.resolve("hostname")
        val said = mutableListOf<String>()
        val clock = MovableClock()
        val addresses = RelayAddresses(hostnameFile = file, announce = { said += it }, nanoTime = clock)

        file.writeText("Nov 05 12:00:00 [warn] Something went wrong")

        assertEquals(emptySet(), addresses.alternates())
        assertEquals(1, said.size, "said once…: $said")
        clock.advanceASecond()
        addresses.alternates()
        assertEquals(1, said.size, "…and not again for the same content: $said")
    }

    /**
     * The first ask always looks, whatever the clock happens to read. The
     * rate limit was seeded with `Long.MIN_VALUE` — "due since forever" — and
     * `now - Long.MIN_VALUE` overflows negative for any positive reading, so
     * the first look was skipped and an already-published address stayed
     * invisible until something asked a second time.
     */
    @Test
    fun `an address published before the relay started is seen on the first ask`() {
        val dir = createTempDirectory("onion")
        val file = dir.resolve("hostname")
        file.writeText(hostname)

        listOf(-5_000_000_000L, 0L, 4_000_000_000_000L).forEach { reading ->
            val addresses = RelayAddresses(hostnameFile = file, announce = {}, nanoTime = MovableClock(reading))
            assertEquals(setOf("ws://$hostname/"), addresses.alternates().map { it.url }.toSet(), "clock at $reading")
        }
    }

    /**
     * The look is rate-limited, so the cost of asking does not scale with
     * traffic — the header put this question on every http response, not just
     * every websocket connect.
     */
    @Test
    fun `the file is not re-read on every ask`() {
        val dir = createTempDirectory("onion")
        val file = dir.resolve("hostname")
        val clock = MovableClock()
        val addresses = RelayAddresses(hostnameFile = file, announce = {}, nanoTime = clock)

        assertEquals(emptySet(), addresses.alternates(), "the first look is eager")

        file.writeText(hostname)
        repeat(100) { addresses.alternates() }
        assertEquals(emptySet(), addresses.alternates(), "a hundred asks inside one interval are one look")

        clock.advanceASecond()
        assertEquals(setOf("ws://$hostname/"), addresses.alternates().map { it.url }.toSet())
    }

    /**
     * What goes in the `Onion-Location` header. `http://`, not `ws://`: the
     * clients that read it parse the value as an http url (okhttp's
     * `toHttpUrl()` in Amethyst), and a ws scheme parses to null there — an
     * advertisement dropped without a word.
     */
    @Test
    fun `the advertised value is an http url`() {
        assertEquals("http://$hostname/", relayAddressesFromEnv(mapOf("RELAY_ONION_URL" to hostname)).onionLocation())
    }

    @Test
    fun `a relay with no hidden service advertises nothing`() {
        assertNull(relayAddressesFromEnv(emptyMap()).onionLocation())
    }

    /** The header names a hidden service or nothing — a clearnet alias is not one. */
    @Test
    fun `a second clearnet address is accepted for auth but never advertised`() {
        val addresses = relayAddressesFromEnv(mapOf("RELAY_ONION_URL" to "wss://relay2.example.com"))
        assertEquals(setOf("wss://relay2.example.com/"), addresses.alternates().map { it.url }.toSet())
        assertNull(addresses.onionLocation())
    }

    /**
     * Knowing an address and publishing it are different decisions. An
     * unlisted onion still has to authenticate the clients that dial it —
     * turning off the advertisement must not turn off the AUTH that made the
     * endpoint worth having.
     */
    @Test
    fun `an unlisted onion still authenticates, it is just not named`() {
        val addresses =
            relayAddressesFromEnv(
                mapOf("RELAY_ONION_URL" to hostname, "RELAY_ONION_ADVERTISE" to "false"),
            )

        assertEquals(setOf("ws://$hostname/"), addresses.alternates().map { it.url }.toSet())
        assertNull(addresses.onionLocation())
    }

    /** The service this deployment runs wins over one someone else holds open. */
    @Test
    fun `the published address is the one advertised`() {
        val dir = createTempDirectory("onion")
        val file = dir.resolve("hostname")
        file.writeText(hostname)
        val addresses =
            relayAddressesFromEnv(
                mapOf(
                    "RELAY_ONION_URL" to "ws://${"z".repeat(56)}.onion/",
                    "RELAY_ONION_HOSTNAME_FILE" to file.toString(),
                ),
            )

        assertEquals("http://$hostname/", addresses.onionLocation())
    }

    /** Both sources are addresses of this relay; neither replaces the other. */
    @Test
    fun `a declared url and a published one are both accepted`() {
        val dir = createTempDirectory("onion")
        val file = dir.resolve("hostname")
        file.writeText(hostname)
        val declared = "ws://${"z".repeat(56)}.onion/"
        val addresses =
            relayAddressesFromEnv(
                mapOf(
                    "RELAY_ONION_URL" to declared,
                    "RELAY_ONION_HOSTNAME_FILE" to file.toString(),
                ),
            )

        assertEquals(setOf(declared, "ws://$hostname/"), addresses.alternates().map { it.url }.toSet())
    }
}
