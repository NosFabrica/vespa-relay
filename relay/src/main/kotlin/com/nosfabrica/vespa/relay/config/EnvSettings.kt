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
package com.nosfabrica.vespa.relay.config

import com.nosfabrica.vespa.eventstore.search.SearchExpansionLimits
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip77Negentropy.NegentropySettings

/**
 * The relay's protection limits: what a single connection may ask for. The
 * engine enforces them and the NIP-11 `limitation` block is rendered from the
 * same object, so the doc and the enforcement can never disagree.
 */
fun defaultRelayLimits(): RelayLimits =
    RelayLimits(
        maxMessageLength = 262_144, // 256 KiB per websocket frame
        maxSubscriptions = 50,
        maxFilters = 20,
        maxLimit = 5_000, // ceiling on a REQ's requested `limit`
        defaultLimit = 5_000, // applied when a filter names no `limit` — the ceiling itself
        maxSubidLength = 256,
        maxEventTags = 2_000,
        maxContentLength = 131_072, // 128 KiB of `.content`
        minPowDifficulty = 0,
        // FALSE, and still true after `REQUIRE_READ_LENS` made an undeclared
        // anonymous read a refusal: the two ways past that gate — NIP-50
        // `observer:` and `include:spam` — need no signature at all, because
        // trust scores here are public. Claiming auth_required would send
        // every client that reads NIP-11 looking for a key it does not need.
        authRequired = false,
        paymentRequired = false,
        restrictedWrites = false,
        createdAtLowerLimit = null,
        createdAtUpperLimit = null,
    )

/**
 * [defaultRelayLimits] with any field an operator set in the environment.
 * Blank/unparseable values keep the default rather than crashing the boot.
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
 * NIP-77 negentropy bounds from the environment, over strfry-parity defaults.
 * `NEG_MAX_SYNC_EVENTS` matters most on a large corpus: it caps how many ids a
 * single reconciliation will walk.
 */
fun negentropySettingsFromEnv(env: Map<String, String>): NegentropySettings {
    val d = NegentropySettings.Default
    return NegentropySettings(
        frameSizeLimit = env.longOr("NEG_FRAME_SIZE_LIMIT", d.frameSizeLimit) ?: d.frameSizeLimit,
        maxSyncEvents = env.intOr("NEG_MAX_SYNC_EVENTS", d.maxSyncEvents) ?: d.maxSyncEvents,
        maxSessionsPerConnection = env.intOr("NEG_MAX_SESSIONS_PER_CONNECTION", d.maxSessionsPerConnection) ?: d.maxSessionsPerConnection,
    )
}

/** NIP-86 admin pubkeys from `RELAY_ADMIN_PUBKEYS`. Empty ⇒ NIP-86 disabled. */
fun adminPubkeysFromEnv(env: Map<String, String>): Set<String> = PubKeys.decodeSet(env["RELAY_ADMIN_PUBKEYS"], "RELAY_ADMIN_PUBKEYS")

/**
 * Static write authorization: only `ALLOW_PUBKEYS` may publish (empty ⇒
 * everyone), minus `DENY_PUBKEYS`. Distinct from NIP-86 bans, which are a
 * runtime-mutable denylist.
 */
fun allowPubkeysFromEnv(env: Map<String, String>): Set<String> = PubKeys.decodeSet(env["ALLOW_PUBKEYS"], "ALLOW_PUBKEYS")

fun denyPubkeysFromEnv(env: Map<String, String>): Set<String> = PubKeys.decodeSet(env["DENY_PUBKEYS"], "DENY_PUBKEYS")

/** Static kind authorization: `ALLOW_KINDS` (empty ⇒ all) minus `DENY_KINDS`. */
fun allowKindsFromEnv(env: Map<String, String>): Set<Int> = parseIntSet(env["ALLOW_KINDS"], "ALLOW_KINDS")

fun denyKindsFromEnv(env: Map<String, String>): Set<Int> = parseIntSet(env["DENY_KINDS"], "DENY_KINDS")

/**
 * Whether an unauthenticated read must declare its lens to be answered:
 * `REQUIRE_READ_LENS`, on unless an operator writes `false`/`0`/`no`.
 *
 * On (the default) a REQ or COUNT from a connection that has not signed a
 * NIP-42 AUTH is refused with `auth-required:` unless every filter names a
 * NIP-50 `observer:` or waives one with `include:spam` — see
 * [com.nosfabrica.vespa.relay.server.LensRequiredPolicy] for why a
 * trust-ranking relay makes that the default rather than answering out of the
 * whole corpus without either side saying so.
 *
 * Off is the older behaviour, and it is a real deployment rather than a debug
 * switch: a relay whose readers are all mirrors, or one serving a corpus with
 * no trust data behind it at all, has nothing to gate on and would only be
 * refusing every client for a lens it cannot apply.
 *
 * Anything unparseable is ON. The failure modes are not symmetric — a typo
 * that silently opens the corpus is the one that cannot be noticed from
 * outside, and `REQUIRE_READ_LENS=treu` looks exactly like a relay working.
 */
fun requireReadLensFromEnv(env: Map<String, String>): Boolean =
    when (env["REQUIRE_READ_LENS"]?.trim()?.lowercase()) {
        "false", "0", "no", "off" -> false
        else -> true
    }

/**
 * Whether a NIP-50 search also answers with the records its hits point at, and
 * how much of the feed that splice may be:
 * `SEARCH_EXPAND_REFERENCES` (off with `false`/`0`/`no`/`off`),
 * `SEARCH_EXPAND_MAX_PER_EVENT` and `SEARCH_EXPAND_MAX_TOTAL`.
 *
 * On is the default, and the reason is the shape of the data rather than a
 * preference: a Trusted List, a NIP-85 assertion and a NIP-32 label carry text
 * that is ABOUT something else, so the record a reader actually wants holds
 * none of the matched words and no ranking will ever recall it from the same
 * search. The splice itself lives in the store now; what this relay owns is the
 * budget for it, which is a property of a deployment rather than of a store.
 *
 * PLACEMENT IS NOT A KNOB. A spliced member lands where the confidence its
 * publisher expressed puts it, always — that is what a 0..100 score on a
 * Trusted List member MEANS, and an operator should not have to know a variable
 * name to get the behaviour the data already describes.
 *
 * A cap of 0 is honoured as 0 — an expansion that adds nothing — rather than
 * quietly meaning "unbounded"; turning the feature off is what
 * `SEARCH_EXPAND_REFERENCES=false` is for, and the two should not be spelled
 * the same way. Negative and unparseable values keep the default.
 */
fun searchExpansionFromEnv(env: Map<String, String>): SearchExpansionLimits {
    val d = SearchExpansionLimits.Default
    return SearchExpansionLimits(
        enabled =
            when (env["SEARCH_EXPAND_REFERENCES"]?.trim()?.lowercase()) {
                "false", "0", "no", "off" -> false
                else -> true
            },
        // `coerceAtLeast(0)` here turned `-1` into a cap of ZERO — the feature
        // on and adding nothing, which is the silent inertness this codebase
        // forbids and the opposite of what the KDoc above promises. A negative
        // is unparseable in spirit and keeps the default, like every other
        // limit in this file; zero is honoured as zero, and the boot log says
        // so.
        maxPerEvent = env.capOr("SEARCH_EXPAND_MAX_PER_EVENT", d.maxPerEvent),
        maxPerRequest = env.capOr("SEARCH_EXPAND_MAX_TOTAL", d.maxPerRequest),
    )
}

/**
 * Reject events dated more than `REJECT_FUTURE_SECONDS` in the future.
 * 0 (the default) disables the check.
 */
fun rejectFutureSecondsFromEnv(env: Map<String, String>): Int = env["REJECT_FUTURE_SECONDS"]?.trim()?.toIntOrNull()?.coerceAtLeast(0) ?: 0

/**
 * How often (seconds) to sweep NIP-40 expired events
 * (`EXPIRATION_SWEEP_SECONDS`). 0 or negative disables the sweeper. Default 1h.
 */
fun expirationSweepSecondsFromEnv(env: Map<String, String>): Long = env["EXPIRATION_SWEEP_SECONDS"]?.trim()?.toLongOrNull() ?: 3_600L

/**
 * Split a comma/space/newline list into a deduped set of ints. A
 * non-numeric entry throws — the same policy [PubKeys] applies to key lists,
 * for the same reason: `DENY_KINDS=4;5` silently denying nothing looks
 * exactly like the feature not working, and nothing downstream can notice.
 */
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

/** Parse an env var as a non-negative cap, keeping [fallback] when absent, blank, negative or unparseable. */
private fun Map<String, String>.capOr(
    key: String,
    fallback: Int,
): Int =
    this[key]
        ?.trim()
        ?.takeIf { it.isNotEmpty() }
        ?.toIntOrNull()
        ?.takeIf { it >= 0 } ?: fallback

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
