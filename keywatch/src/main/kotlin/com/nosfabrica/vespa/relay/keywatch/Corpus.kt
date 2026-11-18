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
package com.nosfabrica.vespa.relay.keywatch

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.count
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAll
import com.vitorpamplona.quartz.nip01Core.relay.client.listeners.RelayConnectionListener
import com.vitorpamplona.quartz.nip01Core.relay.client.single.IRelayClient
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.ClosedMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.NoticeMessage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.delay
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicInteger

/**
 * What keywatch reads: one relay, asked one REQ or one COUNT at a time. An interface so the
 * whole pipeline runs over an in-memory corpus in tests; the production one is [RelayCorpus].
 */
interface Corpus {
    /** One REQ per [MAX_FILTERS_PER_REQ] filters; each filter returns at most its own `limit`. */
    suspend fun fetch(filters: List<Filter>): List<Event>

    /** NIP-45 COUNT of one filter, or null when the relay gave no answer (it will not, sometimes, under load). */
    suspend fun count(filter: Filter): Int?

    /** Whether the relay is answering from its whole index right now. */
    suspend fun healthy(): Boolean = true

    companion object {
        /** search.brainstorm.world's NIP-11 `max_filters`. */
        const val MAX_FILTERS_PER_REQ = 20
    }
}

/** The same filter over another window, every other field kept. */
fun Filter.window(
    since: Long?,
    until: Long?,
    limit: Int?,
): Filter = Filter(ids, authors, kinds, tags, tagsAll, since, until, limit, search)

/** The same filter with [token] appended to its search string (or as the whole of it). */
fun Filter.withSearchToken(token: String): Filter = Filter(ids, authors, kinds, tags, tagsAll, since, until, limit, listOfNotNull(search, token).joinToString(" "))

/**
 * The relay answered from part of its index. Whatever the current phase concluded from what was
 * missing (no history, no citation, no mutual mention) may be false, so the phase is redone.
 */
class RelayDegraded(
    message: String,
) : RuntimeException(message)

/**
 * A read-only view of a production relay. Every filter carries [readToken]: search.brainstorm.world
 * answers an undeclared read with `CLOSED auth-required:` (its read lens gate), and `include:spam`
 * is the declaration that also keeps spam-flagged events in the answer — the bot farms that
 * publish their own keys are exactly what the spam filter hides.
 *
 * While part of its Vespa index is unreachable the relay refuses every COUNT (`CLOSED error: vespa
 * searched only N% of the corpus…`) but serves a REQ from what it can reach, with a plain EOSE and
 * a `partial:` NOTICE at most once per ten minutes per connection (measured 2026-11-18: the
 * observer's kind 10040 came back as an empty EOSE). A REQ alone cannot tell a missing event
 * from an absent one, so this view watches for both signs and throws [RelayDegraded] on either,
 * and asks the [canary] COUNT every [CANARY_EVERY] fetches to catch the reads the throttled
 * NOTICE does not announce. Any filter serves as the canary: the refusal does not depend on it.
 */
class RelayCorpus(
    private val client: INostrClient,
    private val relay: NormalizedRelayUrl,
    private val readToken: String,
    private val canary: Filter,
    private val idleTimeoutMs: Long = 30_000,
) : Corpus {
    private val partialSigns = AtomicInteger()
    private var fetches = 0

    private val listener =
        object : RelayConnectionListener {
            override suspend fun onIncomingMessage(
                relay: IRelayClient,
                msgStr: String,
                msg: Message,
            ) {
                when (msg) {
                    is NoticeMessage -> {
                        if (msg.message.startsWith(PARTIAL_NOTICE)) partialSigns.incrementAndGet()
                    }

                    is ClosedMessage -> {
                        if (PARTIAL_REFUSAL in msg.message) partialSigns.incrementAndGet()
                    }

                    else -> {}
                }
            }
        }

    init {
        client.addConnectionListener(listener)
    }

    override suspend fun fetch(filters: List<Filter>): List<Event> {
        val before = partialSigns.get()
        val events =
            filters.chunked(Corpus.MAX_FILTERS_PER_REQ).flatMap { chunk ->
                client.fetchAll(relay, chunk.map { it.withSearchToken(readToken) }, idleTimeoutMs)
            }
        if (partialSigns.get() != before) throw RelayDegraded("${relay.url} said it answered from part of its index")
        if (++fetches % CANARY_EVERY == 0 && !healthy()) throw RelayDegraded("${relay.url} refused the canary COUNT")
        return events
    }

    override suspend fun healthy(): Boolean = client.count(relay, canary.withSearchToken(readToken), idleTimeoutMs) != null

    /**
     * Asked again after 2, 4 and 8 seconds; a relay under load drops a COUNT now and then. A refusal
     * that says the index is partly unreachable is not load: it throws at once.
     */
    override suspend fun count(filter: Filter): Int? {
        repeat(COUNT_ATTEMPTS) { attempt ->
            val before = partialSigns.get()
            client.count(relay, filter.withSearchToken(readToken), idleTimeoutMs)?.let { return it.count }
            if (partialSigns.get() != before) throw RelayDegraded("${relay.url} refused a COUNT: part of its index is unreachable")
            if (attempt < COUNT_ATTEMPTS - 1) delay(2_000L shl attempt)
        }
        return null
    }

    fun close() = client.removeConnectionListener(listener)

    private companion object {
        const val COUNT_ATTEMPTS = 4
        const val CANARY_EVERY = 25
        const val PARTIAL_NOTICE = "partial:"
        const val PARTIAL_REFUSAL = "searched only"
    }
}

/**
 * Enumerates every event a filter matches, past the relay's per-REQ cap.
 *
 * A search answer is ranked, not newest-first, so the usual `until` cursor does not walk it
 * (measured: the second page of `nsec1` returned 44 of 26,820). Instead each time window is sized
 * with COUNT and halved until it fits under [pageCap], then fetched whole. Windows start one per
 * year so the bisection does not spend thirty COUNTs crossing empty decades; the first and last
 * catch the corpus's impossible timestamps (0, the year 2100).
 *
 * A window the relay will not count is halved anyway: a narrower COUNT is cheaper, and one
 * unanswered COUNT ended an hour-long production pass when it threw. Under [MIN_UNSIZED_SPAN] it is
 * fetched up to the cap unsized, and a full page there is recorded in [gaps], since the window may
 * hold more.
 */
class Sweeper(
    private val corpus: Corpus,
    private val pageCap: Int,
    private val nowSeconds: Long,
    private val log: (String) -> Unit,
) {
    /** Windows read without knowing they were whole, one line each, for the report. */
    val gaps = mutableListOf<String>()

    suspend fun sweep(
        filter: Filter,
        onPage: suspend (List<Event>) -> Unit,
    ) {
        for ((since, until) in initialWindows()) sweepWindow(filter, since, until, onPage)
    }

    private suspend fun sweepWindow(
        filter: Filter,
        since: Long,
        until: Long,
        onPage: suspend (List<Event>) -> Unit,
    ) {
        val n = corpus.count(filter.window(since, until, null))
        if (n == null) {
            if (until - since >= MIN_UNSIZED_SPAN) {
                val mid = since + (until - since) / 2
                sweepWindow(filter, mid + 1, until, onPage)
                sweepWindow(filter, since, mid, onPage)
                return
            }
            val page = corpus.fetch(listOf(filter.window(since, until, pageCap)))
            if (page.size >= pageCap) gaps += "${filter.toJson()} in [$since, $until]: no COUNT, and a full page of ${page.size}"
            log("${filter.search}: no COUNT for [$since, $until]; read ${page.size} unsized")
            onPage(page)
            return
        }
        if (n == 0) return
        if (n <= pageCap || until <= since) {
            if (n > pageCap) {
                gaps += "${filter.toJson()} at second $since: $n events, kept $pageCap"
                log("${filter.search}: $n events share second $since; kept the first $pageCap")
            }
            onPage(corpus.fetch(listOf(filter.window(since, until, pageCap))))
            return
        }
        val mid = since + (until - since) / 2
        sweepWindow(filter, mid + 1, until, onPage)
        sweepWindow(filter, since, mid, onPage)
    }

    private fun initialWindows(): List<Pair<Long, Long>> {
        val lastYear = LocalDate.ofEpochDay(nowSeconds / 86_400).year + 1
        val starts = (FIRST_YEAR..lastYear + 1).map { LocalDate.of(it, 1, 1).atStartOfDay().toEpochSecond(ZoneOffset.UTC) }
        return listOf(0L to starts.first() - 1) +
            starts.zipWithNext { a, b -> a to b - 1 } +
            listOf(starts.last() to LAST_SECOND)
    }

    private companion object {
        /** Nostr's first events are from late 2020. */
        const val FIRST_YEAR = 2021

        /** Past every far-future `created_at` the corpus holds, inside a 32-bit `until`. */
        const val LAST_SECOND = 4_294_967_295L

        /** An hour: below it, a window that cannot be counted is read rather than split further. */
        const val MIN_UNSIZED_SPAN = 3_600L
    }
}
