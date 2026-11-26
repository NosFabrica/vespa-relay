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
package com.nosfabrica.vespa.relay.arch

import java.io.File
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Every setting the code reads is documented in `.env.example`, which `ComposePassesEnvTest` then
 * forces into compose, and each process's own settings reach its own service. Compose injects only
 * what a service maps, so an unmapped setting is ignored rather than refused.
 */
class EnvReadsAreDocumentedTest {
    /** Names the code reads that are not settings an operator sets, each with its reason. */
    private val exempt: Map<String, String> =
        listOf(
            "HTTP_RELAY_CLIENT_HEADER",
            "HTTP_RELAY_PER_CLIENT",
            "HTTP_RELAY_TRUSTED_PROXIES",
            "MONITOR_STATUS_FILE",
            "STATS_COUNTERS_INTERVAL_SECONDS",
            "STATS_FILE",
            "SYNC_MANIFEST_FILE",
            "SYNC_STATUS_FILE",
        ).associateWith { "removed; read only so the boot can say it does nothing" } +
            listOf(
                "SYNC_FULL_RESYNC_SECONDS",
                "SYNC_INGEST",
                "SYNC_NEG_MIN_EVENTS",
                "SYNC_PROGRESS_FILE",
                "SYNC_REFETCH_THE_PAST_SECONDS",
            ).associateWith { "removed; read only so the boot can refuse it" } +
            mapOf(
                "SYNC_CONFIG" to "the streams inline; compose mounts SYNC_CONFIG_LOCAL's file and passes this to the relay only as a tripwire",
                "MONITOR_CONFIG" to "monitor.conf inline; compose mounts MONITOR_CONFIG_LOCAL's file and passes this to the relay only as a tripwire",
                "SYNC_CONFIG_FILE" to "compose points it at the mount SYNC_CONFIG_LOCAL picks",
                "MONITOR_CONFIG_FILE" to "compose sets it when MONITOR_CONFIG_LOCAL names a file",
            )

    /** Whole families of exempt names. */
    private val exemptPrefixes = mapOf("ROUTER_" to "a pre-rename spelling of a SYNC_* setting, refused at boot")

    /** Settings a process reads that its compose service deliberately leaves unset. */
    private val unpassed = mapOf("VESPA_CONFIG_URL" to "derived from VESPA_URL, which compose sets to the service name")

    /** Each process's module and the compose service that runs it. */
    private val services = mapOf("relay" to "relay", "sync" to "sync", "monitor" to "monitor")

    private fun isExempt(name: String) = name in exempt || exemptPrefixes.keys.any { name.startsWith(it) }

    private fun readBy(module: String): Set<String> = Repo.sources(module, "main").flatMap { f -> SETTING.findAll(f.readText()).map { it.groupValues[1] } }.toSet()

    @Test
    fun `every setting the code reads is documented in env example or exempt`() {
        val read = Repo.modules.flatMap(::readBy).toSet()
        val documented = DOCUMENTED.findAll(File(Repo.root, ".env.example").readText()).map { it.groupValues[1] }.toSet()

        val undocumented = (read - documented).filterNot(::isExempt).sorted()
        assertTrue(
            undocumented.isEmpty(),
            "read by the code and missing from .env.example, so nothing forces them into docker-compose.yml: $undocumented — " +
                "document each, commented out at its default, or add it to this test's `exempt` map with the reason no operator sets it.",
        )
        val stale = (exempt.keys - read).sorted()
        assertTrue(stale.isEmpty(), "exempt but no longer read by any module: $stale")
    }

    @Test
    fun `each process's own settings reach its compose service`() {
        val compose = serviceBlocks(File(Repo.root, "docker-compose.yml").readText())
        val missing =
            services.flatMap { (module, service) ->
                val block = compose[service] ?: error("docker-compose.yml has no `$service` service")
                val reaching = MAPPED.findAll(block).map { it.groupValues[1] }.toSet() + REFERENCED.findAll(block).map { it.groupValues[1] }.toSet()
                (readBy(module) - reaching - unpassed.keys).filterNot(::isExempt).sorted().map { "$service: $it" }
            }
        assertTrue(
            missing.isEmpty(),
            "read by a process and never passed to its compose service, so a .env setting it is ignored: $missing — " +
                "add `NAME: \${NAME:-}` to that service's `environment:` block.",
        )
    }

    /** Each top-level service's text in a compose file, by name. */
    private fun serviceBlocks(compose: String): Map<String, String> {
        val blocks = mutableMapOf<String, StringBuilder>()
        var current: StringBuilder? = null
        for (line in compose.lines()) {
            val header = SERVICE.matchEntire(line)
            when {
                header != null -> current = StringBuilder().also { blocks[header.groupValues[1]] = it }
                line.isNotEmpty() && !line[0].isWhitespace() -> current = null
                else -> current?.appendLine(line)
            }
        }
        return blocks.mapValues { it.value.toString() }
    }

    private companion object {
        /** A whole string literal shaped like a setting: upper case, at least one underscore. */
        val SETTING = Regex(""""([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)"""")

        /** A setting named in `.env.example`, set or shown commented-out. */
        val DOCUMENTED = Regex("""^#?\s*([A-Z][A-Z0-9_]{2,})=""", RegexOption.MULTILINE)

        /** A service's header line, two spaces in under `services:`. */
        val SERVICE = Regex("""^ {2}([a-z][a-z0-9_-]*):\s*$""")

        /** `NAME: ${…}` inside a service's `environment:` block. */
        val MAPPED = Regex("""^\s{6}([A-Z][A-Z0-9_]{2,}):""", RegexOption.MULTILINE)

        /** `${NAME` anywhere in the block: a port, a limit, a mounted path. */
        val REFERENCED = Regex("""\$\{([A-Z][A-Z0-9_]{2,})""")
    }
}
