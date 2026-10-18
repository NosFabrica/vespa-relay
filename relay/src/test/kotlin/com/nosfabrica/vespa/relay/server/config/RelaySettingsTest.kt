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
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/** The relay process's own settings, read the way `RelayMain` reads them. */
class RelaySettingsTest {
    private val base = mapOf("RELAY_URL" to "wss://relay.example")

    @Test
    fun `the relay url is required and must be a relay url`() {
        assertContains(assertFailsWith<IllegalStateException> { relayEndpointFromEnv(emptyMap()) }.message.orEmpty(), "RELAY_URL is required")
        assertFailsWith<IllegalStateException> { relayEndpointFromEnv(mapOf("RELAY_URL" to "  ")) }
        assertFailsWith<IllegalStateException> { relayEndpointFromEnv(mapOf("RELAY_URL" to "not a url at all")) }
    }

    /** A NIP-98 `u` is compared as text, so the derived origin is the operator's spelling, not the normalizer's. */
    @Test
    fun `without RELAY_HTTP_URL the NIP-98 origin and the icon both come from RELAY_URL as written`() {
        val endpoint = relayEndpointFromEnv(base)
        assertEquals("https://relay.example", endpoint.http)
        assertEquals("https://relay.example/favicon.ico", endpoint.selfIconUrl)
        assertEquals("wss://relay.example/", endpoint.ws.url)
        assertEquals("http://localhost:7777", relayEndpointFromEnv(mapOf("RELAY_URL" to "ws://localhost:7777")).http)
        assertEquals("https://relay.example", relayEndpointFromEnv(base + ("RELAY_HTTP_URL" to "")).http, "compose's empty value is unset")
    }

    @Test
    fun `RELAY_HTTP_URL is the one origin for NIP-98 and the icon alike`() {
        val endpoint = relayEndpointFromEnv(base + ("RELAY_HTTP_URL" to " https://front.example/relay "))
        assertEquals("https://front.example/relay", endpoint.http)
        assertEquals("https://front.example/favicon.ico", endpoint.selfIconUrl)
        assertEquals("wss://relay.example/", endpoint.ws.url, "the NIP-42 identity is still RELAY_URL")
    }

    @Test
    fun `the NIP-11 fields come from the environment, and a blank one is unset`() {
        val doc = nip11FromEnv(mapOf("RELAY_NAME" to "", "RELAY_DESCRIPTION" to "search", "RELAY_ICON" to " "), "ab".repeat(32), "https://relay.example/favicon.ico")
        assertEquals("vespa-relay", doc.name)
        assertEquals("search", doc.description)
        assertEquals("https://relay.example/favicon.ico", doc.icon, "no icon set means the relay's own")
        assertEquals("ab".repeat(32), doc.selfPubkey)
        assertEquals("https://cdn.example/i.png", nip11FromEnv(mapOf("RELAY_ICON" to "https://cdn.example/i.png"), null, null).icon)
    }

    /** `0s` and `off` are the obvious ways to say "off", and running the default would ignore them. */
    @Test
    fun `a stats interval is whole seconds, zero is off, and anything else stops the boot`() {
        val d = statsIntervalsFromEnv(emptyMap())
        assertEquals(900L to 60L, d.chartsSeconds to d.countersSeconds)
        assertEquals(0L, statsIntervalsFromEnv(mapOf("STATS_INTERVAL_SECONDS" to "0")).chartsSeconds)
        for (bad in listOf("-5", "0s", "off", "15m")) {
            assertFailsWith<IllegalStateException>(bad) { statsIntervalsFromEnv(mapOf("STATS_INTERVAL_SECONDS" to bad)) }
            assertFailsWith<IllegalStateException>(bad) { statsIntervalsFromEnv(mapOf("STATS_COUNTERS_INTERVAL_SECONDS" to bad)) }
        }
    }

    @Test
    fun `state files default to the compose mounts, and an empty manifest path turns the card off`() {
        val d = relayFilesFromEnv(emptyMap())
        assertEquals(File("/var/lib/vespa-relay/stats.json"), d.stats)
        assertEquals(File("/var/lib/vespa-relay/sync-manifest.json"), d.syncManifest)
        assertNull(d.banState)
        assertNull(relayFilesFromEnv(mapOf("SYNC_MANIFEST_FILE" to "")).syncManifest)
        assertEquals(File("/var/lib/vespa-relay/stats.json"), relayFilesFromEnv(mapOf("STATS_FILE" to "")).stats)
        assertNull(relayFilesFromEnv(mapOf("RELAY_STATE_FILE" to "")).banState)
    }

    @Test
    fun `a sync config aimed at the relay stops the boot, and the rest of the family is named`() {
        for (key in listOf("SYNC_CONFIG", "SYNC_CONFIG_FILE", "MONITOR_CONFIG", "MONITOR_CONFIG_FILE")) {
            assertContains(assertFailsWith<IllegalStateException> { refuseSyncConfig(mapOf(key to "/etc/sync.conf")) }.message.orEmpty(), key)
        }
        refuseSyncConfig(mapOf("SYNC_CONFIG" to " "))
        val ignored = syncSettingsIgnored(mapOf("SYNC_STREAMS" to "a", "PARSE_AUDIT_FILE" to "/x", "SYNC_MANIFEST_FILE" to "/m", "SYNC_TOR_ALL" to "", "RELAY_URL" to "x"))
        assertEquals(listOf("PARSE_AUDIT_FILE", "SYNC_STREAMS"), ignored)
    }

    @Test
    fun `the maintenance switches default as documented`() {
        val d = maintenanceSwitchesFromEnv(emptyMap())
        assertEquals(false, d.reindexFts)
        assertEquals(true, d.trustReconcile)
        assertEquals(300, d.storeMetricsLogSeconds)
        assertFailsWith<IllegalStateException> { maintenanceSwitchesFromEnv(mapOf("TRUST_RECONCILE_ON_START" to "nope")) }
    }
}
