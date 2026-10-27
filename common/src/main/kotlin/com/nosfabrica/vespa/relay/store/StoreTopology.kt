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

import com.nosfabrica.vespa.eventstore.runtime.DEFAULT_PROVIDER_REFRESH_MILLIS
import com.nosfabrica.vespa.eventstore.runtime.WriterTopology
import com.nosfabrica.vespa.relay.util.strictInt
import java.net.URI

/**
 * What this deployment tells the store about its writers at every `VespaEventStore.open()`. The
 * relay is the only writer (the sync forwards to it), but `SINGLE_WRITER` buys nothing measurable
 * on `batchInsert` and costs a whole-corpus scan before the first insert (docs/decisions/ingest.md,
 * "One writer"), so the relay stays on strict.
 */
val STORE_WRITERS: WriterTopology = WriterTopology.SHARED_STRICT

/** A day: a longer window is a list that waits a day to apply, which `0` already says more honestly. */
const val MAX_PROVIDER_REFRESH_SECONDS: Long = 86_400L

/** The variable [providerRefreshSeconds] reads — one name for both processes, so compose hands it to each. */
const val PROVIDER_REFRESH_ENV = "TRUST_PROVIDER_REFRESH_SECONDS"

/**
 * How often each process rebuilds the store's kind-10040 provider pass, the cache every observer's
 * lens and Trusted List gate read. Both processes need it, for opposite reasons: the relay so that a
 * provider list the sync mirrored in becomes a reader's lens and delegation, the sync so that cards it
 * mirrors for a service first named on the relay's socket are projected at all. Without it each pass
 * learned only from its own process's writes, and on staging (2026-09-26) 24 of 47 recent observers
 * resolved no lens and got empty ranked pages (NosFabrica/vespa-eventstore#145, #243 here).
 *
 * Defaults to the store's 60 s, which is the window docs/configuration.md promises. Each tick is one
 * read of every stored 10040 (hundreds). `0` turns it off and brings the old behaviour back: a list
 * applies only when this process writes a 10040 itself or restarts. A value that does not parse, is
 * negative, or exceeds a day stops the boot rather than quietly falling back — the bound also keeps
 * the store's seconds-to-millis conversion from overflowing into a negative interval, which it reads
 * as "off".
 */
fun providerRefreshSeconds(env: Map<String, String>): Long =
    env[PROVIDER_REFRESH_ENV]?.trim()?.takeIf { it.isNotEmpty() }?.let {
        it.toLongOrNull()?.takeIf { n -> n in 0..MAX_PROVIDER_REFRESH_SECONDS }
            ?: error(
                "$PROVIDER_REFRESH_ENV='$it' is not a number of seconds up to $MAX_PROVIDER_REFRESH_SECONDS. " +
                    "Use 0 to turn the refresh off, or unset it for the default.",
            )
    } ?: (DEFAULT_PROVIDER_REFRESH_MILLIS / 1000)

/** The variable [feedBatch] checks. The store reads it (vespa-eventstore's `VespaEventIndex`), in both processes. */
const val FEED_BATCH_ENV = "VESPA_FEED_BATCH"

/** The store's feed chunk (`VespaEventIndex.FEED_CHUNK`): a larger batch would only be split again. */
const val MAX_FEED_BATCH = 4_096

/**
 * Documents per HTTP request through the batch handler the store ships in its application package
 * (vespa-eventstore #170, `BatchFeedHandler`); `0` or `1` writes every put through `/document/v1`
 * as before. Batching 100 per request took 28-31% off Vespa's CPU per live-relay event and raised
 * what three cores absorb by 40-60% (that store's benchmark/README.md, "More than one document per
 * request"). Each process deploys the application package, handler included, at boot.
 *
 * The STORE reads the variable, and leniently: a value that is not a number turns batching off
 * without a word. So each boot reads it here first, where a value that does not parse or is out of
 * range stops the boot instead. Null when unset: the store's own default applies — 100 since
 * vespa-eventstore 343926b68850 (#180), off before it — and docker-compose.yml sets 100.
 */
fun feedBatch(env: Map<String, String>): Int? = env.strictInt(FEED_BATCH_ENV, 0..MAX_FEED_BATCH)

/** The variable naming the containers that serve Vespa's document API. Read by both processes. */
const val FEED_URLS_ENV = "VESPA_FEED_URLS"

/** The variable naming the containers that serve `/search/`. Read by both processes. */
const val QUERY_URLS_ENV = "VESPA_QUERY_URLS"

/**
 * Which Vespa containers serve each role, for a cluster that runs feed and query containers as
 * separate clusters (vespa-eventstore's docs/scaling.md, "Feed and query containers"). Either
 * list empty means `VESPA_URL` serves that role, which is the bundled package's one cluster.
 *
 * The store sends every `/document/v1` request and the batch handler's to [feed] (writes, but
 * also the get by id and every visit), and every `/search/` request to [query]. A container
 * listed under the wrong role makes the store throw rather than answer wrong (vespa-eventstore
 * #171), so a mistake here is loud at the first request that meets it.
 */
class VespaEndpoints(
    val feed: List<String>,
    val query: List<String>,
) {
    /** Where this process sends a `/search/` of its own (the stats rollup): the first query container, else [fallback]. */
    fun queryUrl(fallback: String): String = query.firstOrNull() ?: fallback

    /** One boot-log line when either list is set, null when `VESPA_URL` serves both roles. */
    fun describe(): String? =
        if (feed.isEmpty() && query.isEmpty()) {
            null
        } else {
            "feed through ${feed.size.takeIf { it > 0 }?.let { "$it container(s)" } ?: "VESPA_URL"}, " +
                "queries through ${query.size.takeIf { it > 0 }?.let { "$it container(s)" } ?: "VESPA_URL"}"
        }
}

/**
 * [FEED_URLS_ENV] and [QUERY_URLS_ENV], comma-separated. Unset or blank is an empty list. The
 * store would take any string and fail on its first request, minutes into a boot, so a list
 * with an empty entry or an entry that is not an http(s) URL with a host stops the boot here.
 */
fun vespaEndpoints(env: Map<String, String>): VespaEndpoints = VespaEndpoints(urlList(env, FEED_URLS_ENV), urlList(env, QUERY_URLS_ENV))

private fun urlList(
    env: Map<String, String>,
    key: String,
): List<String> {
    val raw = env[key]?.trim()?.takeIf { it.isNotEmpty() } ?: return emptyList()
    return raw.split(',').map { it.trim() }.map { url ->
        val uri = runCatching { URI(url) }.getOrNull()
        if (uri == null || uri.scheme !in setOf("http", "https") || uri.host.isNullOrEmpty()) {
            error(
                "$key='$raw': '$url' is not an http(s) URL with a host. " +
                    "List the containers comma-separated (http://feed-0:8080,http://feed-1:8080), or unset it.",
            )
        }
        url.trimEnd('/')
    }
}
