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
package com.nosfabrica.vespa.relay.sync

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
 * The sync process's half of the clients-first rule: the relay serves its
 * mean read latency on `/pressure`, and this poller adopts it into the
 * [ServingPressure] ingest consults. After [MISSES_BEFORE_RESET] consecutive
 * failures the pressure resets to none and the log says so once, whether or
 * not the feed ever connected; reconnection is announced too.
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
                // "Ever fed" decides the wording; "announced down" decides whether the next success is news.
                var everFed = false
                var down = false
                while (!Thread.currentThread().isInterrupted) {
                    val polled = poll()
                    // poll() restores the interrupt flag rather than counting a shutdown as a miss.
                    if (Thread.currentThread().isInterrupted) return@Thread
                    if (polled == null) {
                        misses++
                        if (misses == MISSES_BEFORE_RESET) {
                            pressure.adopt(0, 0)
                            down = true
                            System.err.println(
                                if (everFed) {
                                    "router: pressure feed lost ($url unreachable x$misses) — ingest no longer yielding to relay reads"
                                } else {
                                    "router: pressure feed has not connected ($url unreachable x$misses) — " +
                                        "ingest is NOT yielding to relay reads; check the url and the relay"
                                },
                            )
                        }
                    } else {
                        // Quiet on an uneventful first connect: the start() line already said the feed is on.
                        if (down) {
                            System.err.println(
                                "router: pressure feed ${if (everFed) "recovered" else "connected"} — relay reads ${polled.first}ms",
                            )
                        }
                        misses = 0
                        everFed = true
                        down = false
                        pressure.adopt(polled.first, polled.second)
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

    /** One GET: (meanMs, samples), or null for any failure. */
    private fun poll(): Pair<Long, Long>? =
        try {
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
        } catch (e: InterruptedException) {
            // The throw clears the flag; restore it so the loop sees the shutdown.
            Thread.currentThread().interrupt()
            null
        } catch (_: Exception) {
            null
        }

    override fun close() {
        poller?.interrupt()
    }

    companion object {
        /** Often enough that a latency spike reaches ingest within a batch or two, rare enough to be no load. */
        const val POLL_INTERVAL_MS = 5_000L

        /** Three misses is a relay gone or a wrong url, not one dropped packet. */
        const val MISSES_BEFORE_RESET = 3
    }
}
