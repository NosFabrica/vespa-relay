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
package com.nosfabrica.vespa.relay.store

import com.nosfabrica.vespa.eventstore.EngineTuning
import com.nosfabrica.vespa.eventstore.SchemaDeploy
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.relay.util.BootClock
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/**
 * The store as the sync and the monitor open it: they wait for the schema the relay deploys, then
 * open read-only, because the relay is the index's only writer. [process] prefixes the boot lines.
 */
object ReadOnlyStore {
    /** Waits until an application serves at [vespaUrl] and returns the config server it was read from. */
    fun awaitSchema(
        env: Map<String, String>,
        vespaUrl: String,
        tuning: EngineTuning,
        process: String,
    ): String {
        val configUrl = env["VESPA_CONFIG_URL"] ?: VespaEventStore.deriveConfigUrl(vespaUrl)
        // Read only to say it is the relay's: a shared .env sets it for every service.
        if (env["AUTO_DEPLOY"] != null) System.err.println("schema: AUTO_DEPLOY is the relay's alone; this process never deploys")
        System.err.println("schema: waiting until an application serves at $vespaUrl — the relay deploys it (up to 10 min)")
        VespaEventStore.deploySchema(vespaUrl, configUrl, SchemaDeploy.AWAIT, tuning)
        BootClock.mark("$process: Vespa serves the schema")
        return configUrl
    }

    /** Opens the store read-only against the schema [awaitSchema] saw. */
    fun open(
        env: Map<String, String>,
        vespaUrl: String,
        relayUrl: NormalizedRelayUrl,
        configUrl: String,
        tuning: EngineTuning,
        process: String,
        /** Where a read is slow enough to log; null leaves the store's own default. */
        slowQueryThresholdMillis: Long? = null,
    ): VespaEventStore {
        val endpoints = vespaEndpoints(env)
        val store =
            VespaEventStore.open(
                vespaUrl,
                relay = relayUrl,
                schema = SchemaDeploy.AWAIT,
                configUrl = configUrl,
                feedEndpoints = endpoints.feed,
                queryEndpoints = endpoints.query,
                // Read-only, so this names the deployment's topology for the store, and writes nothing under it.
                writers = STORE_WRITERS,
                providerRefreshSeconds = providerRefreshSeconds(env),
                slowQueryThresholdMillis = slowQueryThresholdMillis,
                readOnly = true,
                tuning = tuning,
            )
        BootClock.mark("$process: store open (read-only)")
        return store
    }
}
