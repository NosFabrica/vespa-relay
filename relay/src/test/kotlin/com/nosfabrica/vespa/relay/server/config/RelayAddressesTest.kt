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
package com.nosfabrica.vespa.relay.server.config

import kotlin.io.path.createTempDirectory
import kotlin.io.path.deleteExisting
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * How the relay learns its hidden service's address, which Tor can mint long after this
 * process began serving.
 */
class RelayAddressesTest {
    private val hostname = "${"q".repeat(56)}.onion"

    /** Starts negative on purpose: `System.nanoTime()` has no origin. */
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

    /** Fatal, like a malformed `RELAY_URL`: an address typed and ignored loses its clients their lens. */
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

        addresses.alternates()
        assertEquals(1, said.size, "announced once, not once per connection: $said")
    }

    /** Recreating the key volume mints a new service; a cached first address would reject its AUTH. */
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
        // Two writes in one second can share an mtime, and the re-read is keyed on the mtime moving.
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

    /** A `Long.MIN_VALUE` seed makes `now - last` overflow and skip the first look. */
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

    /** The header puts this question on every http response, so the look is rate-limited. */
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

    /** `http://`, not `ws://`: okhttp's `toHttpUrl()` in Amethyst parses a ws scheme to null. */
    @Test
    fun `the advertised value is an http url`() {
        assertEquals("http://$hostname/", relayAddressesFromEnv(mapOf("RELAY_ONION_URL" to hostname)).onionLocation())
    }

    @Test
    fun `a relay with no hidden service advertises nothing`() {
        assertNull(relayAddressesFromEnv(emptyMap()).onionLocation())
    }

    /** The header names a hidden service or nothing; a clearnet alias is not one. */
    @Test
    fun `a second clearnet address is accepted for auth but never advertised`() {
        val addresses = relayAddressesFromEnv(mapOf("RELAY_ONION_URL" to "wss://relay2.example.com"))
        assertEquals(setOf("wss://relay2.example.com/"), addresses.alternates().map { it.url }.toSet())
        assertNull(addresses.onionLocation())
    }

    /** Turning off the advertisement must not turn off the AUTH for the clients that dial the onion. */
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
