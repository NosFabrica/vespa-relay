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

import com.nosfabrica.vespa.eventstore.search.SearchExpansionLimits
import com.nosfabrica.vespa.relay.identity.PubKeys
import com.nosfabrica.vespa.relay.server.Cidr
import com.nosfabrica.vespa.relay.server.ClientAddresses
import com.nosfabrica.vespa.relay.server.HttpRelay
import com.nosfabrica.vespa.relay.server.HttpRelayGate
import com.nosfabrica.vespa.relay.server.OpenRankingConfig
import com.nosfabrica.vespa.relay.server.SearchGate
import com.nosfabrica.vespa.relay.util.strictFlag
import com.nosfabrica.vespa.relay.util.strictInt
import com.nosfabrica.vespa.relay.util.strictLong
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.IngestQueue
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip77Negentropy.NegentropySettings

/** What one connection may ask for. The engine enforces these and NIP-11 `limitation` is rendered from them. */
fun defaultRelayLimits(): RelayLimits =
    RelayLimits(
        maxMessageLength = 262_144,
        maxSubscriptions = 50,
        maxFilters = 20,
        maxLimit = 5_000,
        defaultLimit = 5_000,
        maxSubidLength = 256,
        maxEventTags = 2_000,
        maxContentLength = 131_072,
        minPowDifficulty = 0,
        // False even with the read-lens gate on: `observer:` and `include:spam` need no signature.
        authRequired = false,
        paymentRequired = false,
        restrictedWrites = false,
        createdAtLowerLimit = null,
        createdAtUpperLimit = null,
    )

/**
 * [defaultRelayLimits] with any field the environment sets, each in its range: a zero cap boots and
 * then refuses every frame. An unset `DEFAULT_LIMIT` follows a lowered `MAX_LIMIT`; a set one may not exceed it.
 */
fun relayLimitsFromEnv(env: Map<String, String>): RelayLimits {
    val d = defaultRelayLimits()
    val positive = 1..Int.MAX_VALUE
    val maxLimit = env.intOr("MAX_LIMIT", d.maxLimit, positive)
    val defaultLimit = env.strictInt("DEFAULT_LIMIT", positive)
    if (defaultLimit != null && maxLimit != null && defaultLimit > maxLimit) {
        error("DEFAULT_LIMIT=$defaultLimit is above MAX_LIMIT=$maxLimit; a REQ with no limit would ask for more than any REQ may.")
    }
    val limits =
        RelayLimits(
            maxMessageLength = env.intOr("MAX_MESSAGE_LENGTH", d.maxMessageLength, MIN_MESSAGE_LENGTH..Int.MAX_VALUE),
            maxSubscriptions = env.intOr("MAX_SUBSCRIPTIONS", d.maxSubscriptions, positive),
            maxFilters = env.intOr("MAX_FILTERS", d.maxFilters, positive),
            maxLimit = maxLimit,
            defaultLimit = defaultLimit ?: listOfNotNull(d.defaultLimit, maxLimit).minOrNull(),
            maxSubidLength = env.intOr("MAX_SUBID_LENGTH", d.maxSubidLength, positive),
            maxEventTags = env.intOr("MAX_EVENT_TAGS", d.maxEventTags, positive),
            maxContentLength = env.intOr("MAX_CONTENT_LENGTH", d.maxContentLength, positive),
            minPowDifficulty = env.intOr("MIN_POW_DIFFICULTY", d.minPowDifficulty, 0..MAX_POW_DIFFICULTY),
            authRequired = d.authRequired,
            paymentRequired = d.paymentRequired,
            restrictedWrites = d.restrictedWrites,
            createdAtLowerLimit = env.longOr("CREATED_AT_LOWER_LIMIT", d.createdAtLowerLimit, 0..Long.MAX_VALUE),
            createdAtUpperLimit = env.longOr("CREATED_AT_UPPER_LIMIT", d.createdAtUpperLimit, 0..Long.MAX_VALUE),
        )
    val (lower, upper) = limits.createdAtLowerLimit to limits.createdAtUpperLimit
    if (lower != null && upper != null && lower > upper) error("CREATED_AT_LOWER_LIMIT=$lower is above CREATED_AT_UPPER_LIMIT=$upper; no event could be stored.")
    return limits
}

/** Room for the smallest EVENT frame: an event's id, pubkey and signature alone are 256 characters. */
private const val MIN_MESSAGE_LENGTH = 512

/** A sha256 has 256 bits, so no event could meet a harder target. */
private const val MAX_POW_DIFFICULTY = 256

/** NIP-77 bounds from the environment. `NEG_MAX_SYNC_EVENTS` caps the ids one reconciliation walks. */
fun negentropySettingsFromEnv(env: Map<String, String>): NegentropySettings {
    val d = NegentropySettings.Default
    return NegentropySettings(
        frameSizeLimit = env.longOr("NEG_FRAME_SIZE_LIMIT", d.frameSizeLimit, 0..Long.MAX_VALUE) ?: d.frameSizeLimit,
        maxSyncEvents = env.intOr("NEG_MAX_SYNC_EVENTS", d.maxSyncEvents, 0..Int.MAX_VALUE) ?: d.maxSyncEvents,
        maxSessionsPerConnection = env.intOr("NEG_MAX_SESSIONS_PER_CONNECTION", d.maxSessionsPerConnection, 0..Int.MAX_VALUE) ?: d.maxSessionsPerConnection,
    )
}

/** Deploy-time write authorization: `ALLOW_PUBKEYS` (empty is everyone) minus `DENY_PUBKEYS`. */
fun allowPubkeysFromEnv(env: Map<String, String>): Set<String> = PubKeys.decodeSet(env["ALLOW_PUBKEYS"], "ALLOW_PUBKEYS")

fun denyPubkeysFromEnv(env: Map<String, String>): Set<String> = PubKeys.decodeSet(env["DENY_PUBKEYS"], "DENY_PUBKEYS")

/** `ALLOW_KINDS` (empty is all) minus `DENY_KINDS`. */
fun allowKindsFromEnv(env: Map<String, String>): Set<Int> = parseIntSet(env["ALLOW_KINDS"], "ALLOW_KINDS")

fun denyKindsFromEnv(env: Map<String, String>): Set<Int> = parseIntSet(env["DENY_KINDS"], "DENY_KINDS")

/**
 * `REQUIRE_READ_LENS`: whether an unauthenticated read must declare its lens, see
 * [com.nosfabrica.vespa.relay.server.LensRequiredPolicy]. On unless switched off.
 */
fun requireReadLensFromEnv(env: Map<String, String>): Boolean = env.strictFlag("REQUIRE_READ_LENS") ?: true

/**
 * Whether a search also answers with the records its hits point at, and how much of the feed that
 * may be: `SEARCH_EXPAND_REFERENCES`, `SEARCH_EXPAND_MAX_PER_EVENT`, `SEARCH_EXPAND_MAX_TOTAL`.
 * A cap of 0 is honoured as 0.
 */
fun searchExpansionFromEnv(env: Map<String, String>): SearchExpansionLimits {
    val d = SearchExpansionLimits.Default
    return SearchExpansionLimits(
        enabled = env.strictFlag("SEARCH_EXPAND_REFERENCES") ?: true,
        maxPerEvent = env.capOr("SEARCH_EXPAND_MAX_PER_EVENT", d.maxPerEvent),
        maxPerRequest = env.capOr("SEARCH_EXPAND_MAX_TOTAL", d.maxPerRequest),
    )
}

/**
 * `SEARCH_CONCURRENCY_PER_CONNECTION`: ranked reads one connection may run at once, see
 * `SearchGate`. 0 turns the gate off.
 */
fun searchConcurrencyPerConnectionFromEnv(env: Map<String, String>): Int = env.strictInt("SEARCH_CONCURRENCY_PER_CONNECTION", 0..Int.MAX_VALUE) ?: SearchGate.DEFAULT_PERMITS

/**
 * `RELAY_INGEST_BATCH`: how many published events one store write may take. Unset keeps quartz's
 * 64, sized for SQLite. Measured on staging 2026-10-13 under a 5.3M-event kind-3 publish: the
 * write lock held 93-98% of each 5-minute window, batches at the 64 cap, 170-320 ms per batch,
 * so publishing topped out near 200-380 events/s. Set but unparseable stops the boot.
 */
fun ingestBatchFromEnv(env: Map<String, String>): Int = env.strictInt("RELAY_INGEST_BATCH", 1..MAX_INGEST_BATCH) ?: IngestQueue.DEFAULT_MAX_BATCH

/** The largest batch docs/decisions/ingest.md measured through the store (2 x 8192). */
private const val MAX_INGEST_BATCH = 8_192

/**
 * `HTTP_RELAY` and its settings, or null when the switch is off. [origins] are the addresses a NIP-98
 * token may name. Anything set that does not parse stops the boot: each of these is a limit.
 */
fun httpRelayFromEnv(
    env: Map<String, String>,
    origins: () -> List<String>,
): HttpRelay? {
    if (env.strictFlag("HTTP_RELAY") == false) return null
    val header = env["HTTP_RELAY_CLIENT_HEADER"]?.trim()?.takeIf { it.isNotEmpty() }
    val proxies =
        env["HTTP_RELAY_TRUSTED_PROXIES"]
            ?.split(',', ' ', '\n')
            ?.map { it.trim() }
            ?.filter { it.isNotEmpty() }
            ?.map { Cidr.parse(it) ?: error("HTTP_RELAY_TRUSTED_PROXIES: '$it' is not an address or block (10.0.0.0/8, ::1/128).") }
            .orEmpty()
    // Either alone is a header nobody may set, or trust in a header nobody names.
    if ((header == null) != proxies.isEmpty()) {
        error("HTTP_RELAY_CLIENT_HEADER and HTTP_RELAY_TRUSTED_PROXIES go together: the header is read only from those proxies.")
    }
    return HttpRelay(
        gate =
            HttpRelayGate(
                perClient = env.strictInt("HTTP_RELAY_PER_CLIENT", 0..Int.MAX_VALUE) ?: HttpRelayGate.DEFAULT_PER_CLIENT,
                total = env.strictInt("HTTP_RELAY_TOTAL", 0..Int.MAX_VALUE) ?: HttpRelayGate.DEFAULT_TOTAL,
            ),
        deadlineMs = (env.strictInt("HTTP_RELAY_DEADLINE_SECONDS", 1..MAX_DEADLINE_SECONDS) ?: HttpRelay.DEFAULT_DEADLINE_SECONDS.toInt()) * 1000L,
        origins = origins,
        clients = ClientAddresses(header, proxies),
    )
}

/**
 * The ORE-05 search backend's settings (`ORE_ENABLED`, off by default), or null when it is off; anything
 * set that does not parse stops the boot. The House Observer comes with each global search, from the proxy.
 */
fun openRankingFromEnv(env: Map<String, String>): OpenRankingConfig? {
    if (env.strictFlag("ORE_ENABLED") != true) return null
    return OpenRankingConfig(
        concurrency = env.strictInt("ORE_CONCURRENCY", 1..Int.MAX_VALUE) ?: OpenRankingConfig.DEFAULT_CONCURRENCY,
    )
}

/** An hour: an answer still running past that is a stuck one, not a slow one. */
private const val MAX_DEADLINE_SECONDS = 3_600

/** `REJECT_FUTURE_SECONDS`; 0 (the default) disables the check. */
fun rejectFutureSecondsFromEnv(env: Map<String, String>): Int = env.strictInt("REJECT_FUTURE_SECONDS", 0..Int.MAX_VALUE) ?: 0

/** `EXPIRATION_SWEEP_SECONDS`, the NIP-40 sweep period. 0 or negative disables it. */
fun expirationSweepSecondsFromEnv(env: Map<String, String>): Long = env.strictLong("EXPIRATION_SWEEP_SECONDS") ?: 3_600L

/** A comma/space/newline list of ints. A non-numeric entry throws, as [PubKeys] does for keys. */
private fun parseIntSet(
    raw: String?,
    varName: String,
): Set<Int> =
    raw
        ?.split(',', ' ', '\n')
        ?.map { it.trim() }
        ?.filter { it.isNotEmpty() }
        ?.map {
            it.toIntOrNull()
                ?: throw IllegalArgumentException("$varName must be a list of kind numbers, got \"$it\"")
        }?.toSet()
        .orEmpty()

/** A non-negative cap; [fallback] when unset. */
private fun Map<String, String>.capOr(
    key: String,
    fallback: Int,
): Int = strictInt(key, 0..Int.MAX_VALUE) ?: fallback

private fun Map<String, String>.intOr(
    key: String,
    fallback: Int?,
    range: IntRange,
): Int? = strictInt(key, range) ?: fallback

private fun Map<String, String>.longOr(
    key: String,
    fallback: Long?,
    range: LongRange,
): Long? = strictLong(key, range) ?: fallback
