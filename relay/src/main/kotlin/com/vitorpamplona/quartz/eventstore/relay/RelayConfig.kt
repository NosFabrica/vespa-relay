/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip77Negentropy.NegentropySettings

/**
 * The relay's protection limits. These bound what a single connection can ask
 * for — message size, subscription and filter counts, the `limit:` a REQ may
 * request — so one client can't exhaust the server or Vespa behind it. The
 * engine enforces them (via an auto-installed LimitsPolicy) and the NIP-11
 * `limitation` block is rendered from the very same object.
 *
 * These defaults are deliberately generous but finite. Every field is
 * overridable from the environment via [relayLimitsFromEnv].
 */
fun defaultRelayLimits(): RelayLimits =
    RelayLimits(
        maxMessageLength = 262_144, // 256 KiB per websocket frame
        maxSubscriptions = 50,
        maxFilters = 20,
        maxLimit = 5_000, // ceiling on a REQ's requested `limit`
        defaultLimit = 500, // applied when a filter names no `limit`
        maxSubidLength = 256,
        maxEventTags = 2_000,
        maxContentLength = 131_072, // 128 KiB of `.content`
        minPowDifficulty = 0,
        authRequired = false, // NIP-42 only switches the ranking observer
        paymentRequired = false,
        restrictedWrites = false,
        createdAtLowerLimit = null,
        createdAtUpperLimit = null,
    )

/**
 * Build [RelayLimits] from the environment, starting from [defaultRelayLimits]
 * and overriding only the fields an operator set. Blank/unparseable values are
 * ignored (the default stands) rather than crashing the relay at boot.
 */
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

/**
 * Build [NegentropySettings] from the environment, starting from
 * [NegentropySettings.Default] (strfry-parity values) and overriding only what
 * an operator set. `NEG_MAX_SYNC_EVENTS` is the important one on a large Vespa
 * corpus: it caps how many ids a single NIP-77 reconciliation will walk.
 */
fun negentropySettingsFromEnv(env: Map<String, String>): NegentropySettings {
    val d = NegentropySettings.Default
    return NegentropySettings(
        frameSizeLimit = env.longOr("NEG_FRAME_SIZE_LIMIT", d.frameSizeLimit) ?: d.frameSizeLimit,
        maxSyncEvents = env.intOr("NEG_MAX_SYNC_EVENTS", d.maxSyncEvents) ?: d.maxSyncEvents,
        maxSessionsPerConnection = env.intOr("NEG_MAX_SESSIONS_PER_CONNECTION", d.maxSessionsPerConnection) ?: d.maxSessionsPerConnection,
    )
}

/**
 * The set of admin pubkeys (64-hex) authorized for NIP-86 relay management,
 * from a comma/space-separated `RELAY_ADMIN_PUBKEYS`. Empty ⇒ NIP-86 disabled.
 * Entries that aren't 64-hex are dropped.
 */
fun adminPubkeysFromEnv(env: Map<String, String>): Set<String> = PubKeys.decodeSet(env["RELAY_ADMIN_PUBKEYS"], "RELAY_ADMIN_PUBKEYS")

/**
 * Static write authorization (geode's `[authorization]`): only these pubkeys may
 * publish (`ALLOW_PUBKEYS`, empty ⇒ everyone), minus any on the denylist
 * (`DENY_PUBKEYS`). Applied to EVENT commands before verification. Distinct from
 * NIP-86 bans, which are a runtime-mutable denylist.
 */
fun allowPubkeysFromEnv(env: Map<String, String>): Set<String> = PubKeys.decodeSet(env["ALLOW_PUBKEYS"], "ALLOW_PUBKEYS")

fun denyPubkeysFromEnv(env: Map<String, String>): Set<String> = PubKeys.decodeSet(env["DENY_PUBKEYS"], "DENY_PUBKEYS")

/** Static kind authorization: `ALLOW_KINDS` (empty ⇒ all) minus `DENY_KINDS`. */
fun allowKindsFromEnv(env: Map<String, String>): Set<Int> = parseIntSet(env["ALLOW_KINDS"])

fun denyKindsFromEnv(env: Map<String, String>): Set<Int> = parseIntSet(env["DENY_KINDS"])

/**
 * Reject events whose `created_at` is more than this many seconds in the future
 * (`REJECT_FUTURE_SECONDS`). 0 (the default) disables the check. Clock drift and
 * back-dated notes past this bound are refused before ingest.
 */
fun rejectFutureSecondsFromEnv(env: Map<String, String>): Int = env["REJECT_FUTURE_SECONDS"]?.trim()?.toIntOrNull()?.coerceAtLeast(0) ?: 0

/**
 * How often (seconds) to sweep NIP-40 expired events from the store
 * (`EXPIRATION_SWEEP_SECONDS`). 0 or negative disables the sweeper. Default 1h.
 */
fun expirationSweepSecondsFromEnv(env: Map<String, String>): Long = env["EXPIRATION_SWEEP_SECONDS"]?.trim()?.toLongOrNull() ?: 3_600L

/** Split a comma/space/newline list into a deduped set of ints, dropping non-numeric entries. */
private fun parseIntSet(raw: String?): Set<Int> =
    raw
        ?.split(',', ' ', '\n')
        ?.mapNotNull { it.trim().toIntOrNull() }
        ?.toSet()
        .orEmpty()

/** Parse an env var as Int, keeping [fallback] when absent, blank, or unparseable. */
private fun Map<String, String>.intOr(
    key: String,
    fallback: Int?,
): Int? = this[key]?.trim()?.takeIf { it.isNotEmpty() }?.toIntOrNull() ?: fallback

/** Parse an env var as Long, keeping [fallback] when absent, blank, or unparseable. */
private fun Map<String, String>.longOr(
    key: String,
    fallback: Long?,
): Long? = this[key]?.trim()?.takeIf { it.isNotEmpty() }?.toLongOrNull() ?: fallback
