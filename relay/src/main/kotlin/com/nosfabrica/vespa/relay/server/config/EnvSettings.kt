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
import com.nosfabrica.vespa.relay.server.SearchGate
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

/** [defaultRelayLimits] with any field the environment sets. Unparseable values keep the default. */
fun relayLimitsFromEnv(env: Map<String, String>): RelayLimits {
    val d = defaultRelayLimits()
    return RelayLimits(
        maxMessageLength = env.intOr("MAX_MESSAGE_LENGTH", d.maxMessageLength),
        maxSubscriptions = env.intOr("MAX_SUBSCRIPTIONS", d.maxSubscriptions),
        maxFilters = env.intOr("MAX_FILTERS", d.maxFilters),
        maxLimit = env.intOr("MAX_LIMIT", d.maxLimit),
        defaultLimit = env.intOr("DEFAULT_LIMIT", d.defaultLimit),
        maxSubidLength = env.intOr("MAX_SUBID_LENGTH", d.maxSubidLength),
        maxEventTags = env.intOr("MAX_EVENT_TAGS", d.maxEventTags),
        maxContentLength = env.intOr("MAX_CONTENT_LENGTH", d.maxContentLength),
        minPowDifficulty = env.intOr("MIN_POW_DIFFICULTY", d.minPowDifficulty),
        authRequired = d.authRequired,
        paymentRequired = d.paymentRequired,
        restrictedWrites = d.restrictedWrites,
        createdAtLowerLimit = env.longOr("CREATED_AT_LOWER_LIMIT", d.createdAtLowerLimit),
        createdAtUpperLimit = env.longOr("CREATED_AT_UPPER_LIMIT", d.createdAtUpperLimit),
    )
}

/** NIP-77 bounds from the environment. `NEG_MAX_SYNC_EVENTS` caps the ids one reconciliation walks. */
fun negentropySettingsFromEnv(env: Map<String, String>): NegentropySettings {
    val d = NegentropySettings.Default
    return NegentropySettings(
        frameSizeLimit = env.longOr("NEG_FRAME_SIZE_LIMIT", d.frameSizeLimit) ?: d.frameSizeLimit,
        maxSyncEvents = env.intOr("NEG_MAX_SYNC_EVENTS", d.maxSyncEvents) ?: d.maxSyncEvents,
        maxSessionsPerConnection = env.intOr("NEG_MAX_SESSIONS_PER_CONNECTION", d.maxSessionsPerConnection) ?: d.maxSessionsPerConnection,
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
 * [com.nosfabrica.vespa.relay.server.LensRequiredPolicy]. Unparseable is on: a typo that opened
 * the corpus could not be noticed.
 */
fun requireReadLensFromEnv(env: Map<String, String>): Boolean =
    when (env["REQUIRE_READ_LENS"]?.trim()?.lowercase()) {
        "false", "0", "no", "off" -> false
        else -> true
    }

/**
 * Whether a search also answers with the records its hits point at, and how much of the feed that
 * may be: `SEARCH_EXPAND_REFERENCES`, `SEARCH_EXPAND_MAX_PER_EVENT`, `SEARCH_EXPAND_MAX_TOTAL`.
 * A cap of 0 is honoured as 0; negative and unparseable keep the default.
 */
fun searchExpansionFromEnv(env: Map<String, String>): SearchExpansionLimits {
    val d = SearchExpansionLimits.Default
    return SearchExpansionLimits(
        enabled =
            when (env["SEARCH_EXPAND_REFERENCES"]?.trim()?.lowercase()) {
                "false", "0", "no", "off" -> false
                else -> true
            },
        maxPerEvent = env.capOr("SEARCH_EXPAND_MAX_PER_EVENT", d.maxPerEvent),
        maxPerRequest = env.capOr("SEARCH_EXPAND_MAX_TOTAL", d.maxPerRequest),
    )
}

/**
 * `SEARCH_CONCURRENCY_PER_CONNECTION`: ranked reads one connection may run at once, see
 * `SearchGate`. 0 turns the gate off; unparseable is the default, not off.
 */
fun searchConcurrencyPerConnectionFromEnv(env: Map<String, String>): Int = env.intOr("SEARCH_CONCURRENCY_PER_CONNECTION", SearchGate.DEFAULT_PERMITS)!!.coerceAtLeast(0)

/** `REJECT_FUTURE_SECONDS`; 0 (the default) disables the check. */
fun rejectFutureSecondsFromEnv(env: Map<String, String>): Int = env["REJECT_FUTURE_SECONDS"]?.trim()?.toIntOrNull()?.coerceAtLeast(0) ?: 0

/** `EXPIRATION_SWEEP_SECONDS`, the NIP-40 sweep period. 0 or negative disables it. */
fun expirationSweepSecondsFromEnv(env: Map<String, String>): Long = env["EXPIRATION_SWEEP_SECONDS"]?.trim()?.toLongOrNull() ?: 3_600L

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

/** A non-negative cap; [fallback] when absent, blank, negative or unparseable. */
private fun Map<String, String>.capOr(
    key: String,
    fallback: Int,
): Int =
    this[key]
        ?.trim()
        ?.takeIf { it.isNotEmpty() }
        ?.toIntOrNull()
        ?.takeIf { it >= 0 } ?: fallback

private fun Map<String, String>.intOr(
    key: String,
    fallback: Int?,
): Int? = this[key]?.trim()?.takeIf { it.isNotEmpty() }?.toIntOrNull() ?: fallback

private fun Map<String, String>.longOr(
    key: String,
    fallback: Long?,
): Long? = this[key]?.trim()?.takeIf { it.isNotEmpty() }?.toLongOrNull() ?: fallback
