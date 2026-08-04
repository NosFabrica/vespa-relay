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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.relay.server.ServingPressure
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * The sync process's half of the clients-first rule. In one JVM the relay
 * recorded read latency straight into the [ServingPressure] ingest reads; as
 * separate processes the two still share one Vespa — a mirror batch's dedup
 * and projection queries queue in the same engine a client's REQ does — so the
 * signal crosses over HTTP instead: the relay serves its mean on `/pressure`,
 * and this poller adopts it into the instance ingest consults.
 *
 * A dead feed must not throttle forever on a number from the past: after
 * [MISSES_BEFORE_RESET] consecutive failures the pressure resets to none — a
 * relay that is down has no clients to protect — and says so once, not per
 * miss. Recovery is announced too, so the log shows which regime a slow
 * night's ingest was under.
 */
class PressurePoller(
    private val url: String,
    private val pressure: ServingPressure,
    private val intervalMs: Long = POLL_INTERVAL_MS,
) : AutoCloseable {
    private val http =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build()

    @Volatile private var poller: Thread? = null

    fun start(): PressurePoller {
        System.err.println("router: yielding to relay reads via $url")
        poller =
            Thread {
                var misses = 0
                var fed = false
                while (!Thread.currentThread().isInterrupted) {
                    when (val polled = poll()) {
                        null -> {
                            misses++
                            if (misses == MISSES_BEFORE_RESET) {
                                pressure.adopt(0, 0)
                                if (fed) {
                                    System.err.println(
                                        "router: pressure feed lost ($url unreachable x$misses) — ingest no longer yielding to relay reads",
                                    )
                                }
                                fed = false
                            }
                        }

                        else -> {
                            if (!fed && misses >= MISSES_BEFORE_RESET) {
                                System.err.println("router: pressure feed recovered — relay reads ${polled.first}ms")
                            }
                            misses = 0
                            fed = true
                            pressure.adopt(polled.first, polled.second)
                        }
                    }
                    try {
                        Thread.sleep(intervalMs)
                    } catch (_: InterruptedException) {
                        return@Thread
                    }
                }
            }.apply {
                isDaemon = true
                name = "sync-pressure-poll"
                start()
            }
        return this
    }

    /** One GET: (meanMs, samples), or null for any failure — the loop counts those. */
    private fun poll(): Pair<Long, Long>? =
        runCatching {
            val request =
                HttpRequest
                    .newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build()
            val response = http.send(request, HttpResponse.BodyHandlers.ofString())
            check(response.statusCode() == 200) { "HTTP ${response.statusCode()}" }
            val body = Json.parseToJsonElement(response.body()).jsonObject
            val mean = body.getValue("meanMs").jsonPrimitive.long
            val samples = body.getValue("samples").jsonPrimitive.long
            mean to samples
        }.getOrNull()

    override fun close() {
        poller?.interrupt()
    }

    companion object {
        /**
         * Often enough that a latency spike reaches ingest within a batch or
         * two, rare enough that the poll itself is no load on the relay.
         */
        const val POLL_INTERVAL_MS = 5_000L

        /**
         * Three misses is a relay actually gone (or the url wrong), not one
         * dropped packet. At the 5s interval this holds a stale mean for at
         * most ~15s, which one slow batch outlives anyway.
         */
        const val MISSES_BEFORE_RESET = 3
    }
}
