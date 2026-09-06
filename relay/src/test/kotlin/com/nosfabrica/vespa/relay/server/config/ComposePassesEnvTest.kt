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

import java.io.File
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Every setting `.env.example` documents has to reach a container. Compose injects only
 * what a service maps, and an unmapped setting is ignored rather than refused. The check
 * is loose about how a name reaches compose and strict about it appearing at all.
 */
class ComposePassesEnvTest {
    /** Documented settings that deliberately never reach a container, each with its reason. */
    private val exempt =
        mapOf(
            "RELAY_HTTP_URL" to "derived from RELAY_URL; only set on a deployment that terminates TLS elsewhere",
            "RELAY_VERSION" to "read at build time into the NIP-11 document, not at run time",
            "VESPA_ACCESS_LOG" to "a setting for the Vespa image's own configuration, not for ours",
            "VESPA_CONFIG_URL" to "derived from VESPA_URL, which compose sets to the service name",
        )

    private fun repoFile(name: String): File =
        assertNotNull(
            listOf(File("../$name"), File(name)).firstOrNull { it.isFile },
            "$name is not where this test can read it",
        )

    @Test
    fun `every documented setting is mapped, referenced, or exempt`() {
        val documented = DOCUMENTED.findAll(repoFile(".env.example").readText()).map { it.groupValues[1] }.toSet()
        val compose = repoFile("docker-compose.yml").readText()
        val reaching =
            MAPPED.findAll(compose).map { it.groupValues[1] }.toSet() +
                REFERENCED.findAll(compose).map { it.groupValues[1] }.toSet()

        val unreachable = (documented - reaching - exempt.keys).sorted()

        assertTrue(
            unreachable.isEmpty(),
            "documented in .env.example and never reaching a container: $unreachable — add each to the " +
                "service's `environment:` block, or to this test's `exempt` map with the reason it does nothing.",
        )
        // An exemption for a deleted setting is a note nobody will read again.
        val stale = exempt.keys.filterNot { it in documented }.sorted()
        assertTrue(stale.isEmpty(), "exempt but no longer documented in .env.example: $stale")
    }

    @Test
    fun `the pulse settings reach both services`() {
        val compose = repoFile("docker-compose.yml").readText()

        // Named outright because both halves fail silently: an unmapped port means no
        // page, an unmapped admin list means a boot that stops.
        for (name in listOf("PULSE_PORT", "PULSE_PUBLIC_URL", "PULSE_CLIENT_DETAIL", "PULSE_SLOW_READ_MS")) {
            assertTrue(compose.contains("$name: \${$name"), "$name is not passed to the relay service")
        }
        for (name in listOf("SYNC_PULSE_PORT", "SYNC_PULSE_PUBLIC_URL", "SYNC_PULSE_CLIENT_DETAIL", "SYNC_PULSE_SLOW_READ_MS")) {
            assertTrue(compose.contains("$name: \${$name"), "$name is not passed to the sync service")
        }
        assertTrue(
            Regex("""RELAY_ADMIN_PUBKEYS: \$\{RELAY_ADMIN_PUBKEYS""").findAll(compose).count() == 2,
            "RELAY_ADMIN_PUBKEYS must reach both services — the mirror's pulse page checks the same list",
        )
    }

    @Test
    fun `the pulse ports are published on loopback only`() {
        val compose = repoFile("docker-compose.yml").readText()

        // The status pages beside it are public on purpose; the pulse document is not.
        for (port in listOf("PULSE_PORT", "SYNC_PULSE_PORT")) {
            assertTrue(
                compose.contains("\"127.0.0.1:\${$port"),
                "$port is published on every interface — this document names the observer lenses and " +
                    "search terms driving the load and can quote slow queries",
            )
        }
    }

    private companion object {
        /** A setting named in `.env.example`, set or shown commented-out. */
        val DOCUMENTED = Regex("""^#?\s*([A-Z][A-Z0-9_]{2,})=""", RegexOption.MULTILINE)

        /** `NAME: ${…}` inside a service's `environment:` block. */
        val MAPPED = Regex("""^\s{6}([A-Z][A-Z0-9_]{2,}):""", RegexOption.MULTILINE)

        /** `${NAME` anywhere in the file: a port, a memory limit, a mounted path. */
        val REFERENCED = Regex("""\$\{([A-Z][A-Z0-9_]{2,})""")
    }
}
