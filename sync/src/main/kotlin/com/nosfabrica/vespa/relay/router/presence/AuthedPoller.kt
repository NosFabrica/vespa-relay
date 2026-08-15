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
package com.nosfabrica.vespa.relay.router.presence

import com.vitorpamplona.quartz.nip01Core.core.HexKey
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * Keeps [AuthedFeed] current from the relay's `GET /authed`.
 *
 * The same shape as `PressurePoller`, deliberately — a daemon thread, one plain
 * `HttpClient`, a miss counter, and a reset that says so once rather than per
 * miss. Two things differ, and both come from what this endpoint is:
 *
 *  - **it carries a credential.** `/pressure` is public because a mean latency
 *    names nobody; this is a list of the people currently reading. The token is
 *    required on both sides, and a 401 is reported in its own words because it
 *    is the one failure an operator fixes in a config file rather than by
 *    looking at the network.
 *  - **losing it EMPTIES the set.** See [AuthedFeed.clear]. A stale presence
 *    list does not decay into a mildly wrong throttle the way a stale latency
 *    does; it holds sockets open on other people's relays for readers who may
 *    have left hours ago.
 */
class AuthedPoller(
    private val url: String,
    private val token: String,
    private val feed: AuthedFeed,
    private val intervalMs: Long = POLL_INTERVAL_MS,
) : AutoCloseable {
    private val http =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build()

    @Volatile private var poller: Thread? = null

    fun start(): AuthedPoller {
        System.err.println("router: presence feed from $url")
        poller =
            Thread {
                var misses = 0
                var down = false
                while (!Thread.currentThread().isInterrupted) {
                    val polled = poll()
                    // poll() restores the interrupt flag rather than counting a
                    // shutdown as a miss — see PressurePoller, same trap.
                    if (Thread.currentThread().isInterrupted) return@Thread
                    if (polled == null) {
                        misses++
                        // ONE line per outage, not one per poll. At a 10-second
                        // interval a misconfigured token would otherwise print
                        // 8,640 identical lines a day into a container log that
                        // rotates inside the hour, which is how the line that
                        // matters gets lost. The reason is carried from the
                        // failed poll rather than restated, because "unreachable"
                        // and "the relay rejected our token" are fixed in
                        // different places.
                        if (misses == MISSES_BEFORE_RESET) {
                            feed.clear()
                            down = true
                            System.err.println(
                                if (feed.everFed()) {
                                    "router: presence feed lost ($url x$misses: $lastFailure) — every presence stream now holds nothing"
                                } else {
                                    "router: presence feed has not connected ($url x$misses: $lastFailure) — " +
                                        "presence streams will mirror NOTHING; check the url and RELAY_AUTHED_TOKEN on both services"
                                },
                            )
                        }
                    } else {
                        if (down) {
                            System.err.println("router: presence feed ${if (feed.everFed()) "recovered" else "connected"} — ${polled.pubkeys.size} reader(s)")
                        }
                        misses = 0
                        down = false
                        if (polled.omitted > 0) {
                            // Loud, because it is the one failure of this
                            // feature that produces no error anywhere: those
                            // readers are simply never mirrored for, and every
                            // stream looks healthy.
                            System.err.println(
                                "router: presence feed truncated — ${polled.omitted} signed-in reader(s) did not fit the relay's " +
                                    "response and are being mirrored for by nobody",
                            )
                        }
                        feed.adopt(polled.pubkeys, polled.omitted)
                    }
                    try {
                        Thread.sleep(intervalMs)
                    } catch (_: InterruptedException) {
                        return@Thread
                    }
                }
            }.apply {
                isDaemon = true
                name = "sync-authed-poll"
                start()
            }
        return this
    }

    /** One GET, or null for any failure — the loop counts those. */
    private fun poll(): Answer? =
        try {
            val request =
                HttpRequest
                    .newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(5))
                    .header("Authorization", "Bearer $token")
                    .GET()
                    .build()
            val response = http.send(request, HttpResponse.BodyHandlers.ofString())
            // Named apart from every other status: a 401 is a config mistake on
            // one of two services, and "unreachable x3" would send an operator
            // looking at the network instead.
            check(response.statusCode() != 401) { "HTTP 401 — the relay rejected our token; RELAY_AUTHED_TOKEN and SYNC_AUTHED_TOKEN must match" }
            check(response.statusCode() == 200) { "HTTP ${response.statusCode()}" }
            val body = Json.parseToJsonElement(response.body()).jsonObject
            Answer(
                pubkeys =
                    body
                        .getValue("pubkeys")
                        .jsonArray
                        .mapNotNull { it.jsonPrimitive.contentOrNull }
                        // A pubkey that is not 32 bytes of hex cannot be an
                        // `authors` entry, and a filter carrying one is a filter
                        // the relay may refuse whole — taking every OTHER reader
                        // on that leg down with it.
                        .filter { HEX32.matches(it) },
                omitted = body["omitted"]?.jsonPrimitive?.int ?: 0,
            )
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            null
        } catch (e: Exception) {
            // Recorded rather than logged: the loop above prints it once per
            // outage. Kept because the 401 is otherwise indistinguishable from
            // a relay that is simply not there, and the two are fixed in
            // different places.
            lastFailure = e.message ?: e.javaClass.simpleName
            null
        }

    /** Why the last poll failed, for the one line the loop prints per outage. */
    @Volatile
    private var lastFailure: String = "no answer"

    private class Answer(
        val pubkeys: List<HexKey>,
        val omitted: Int,
    )

    override fun close() {
        poller?.interrupt()
    }

    companion object {
        /**
         * How often the authed set is re-read.
         *
         * This is the floor on how long after signing in a reader waits for
         * their relays to be dialled, so it is seconds. It is not the stream's
         * own `pollSeconds`, which paces the RECONCILE against this set: one
         * fetch feeds every presence stream, and each stream decides for itself
         * how often to act on it.
         */
        const val POLL_INTERVAL_MS = 10_000L

        /** Three misses is a relay actually gone, not one dropped packet — PressurePoller's reasoning. */
        const val MISSES_BEFORE_RESET = 3

        private val HEX32 = Regex("^[0-9a-f]{64}$", RegexOption.IGNORE_CASE)
    }
}
