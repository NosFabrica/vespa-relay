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

import com.nosfabrica.vespa.eventstore.EngineTuning
import com.nosfabrica.vespa.eventstore.search.SearchExpansionLimits
import com.nosfabrica.vespa.relay.identity.PubKeys
import com.nosfabrica.vespa.relay.ingest.IngestTuning
import com.nosfabrica.vespa.relay.server.HttpRelay
import com.nosfabrica.vespa.relay.server.HttpRelayGate
import com.nosfabrica.vespa.relay.server.OpenRankingConfig
import com.nosfabrica.vespa.relay.server.PublishSlots
import com.nosfabrica.vespa.relay.server.ReplayGate
import com.nosfabrica.vespa.relay.server.SearchGate
import com.nosfabrica.vespa.relay.util.strictChoice
import com.nosfabrica.vespa.relay.util.strictFlag
import com.nosfabrica.vespa.relay.util.strictInt
import com.nosfabrica.vespa.relay.util.strictLong
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.IngestQueue
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip77Negentropy.NegentropySettings

/**
 * What one connection may ask for. The engine enforces these and NIP-11 `limitation` is rendered from them.
 * A REQ naming no `limit` is served 500 events, strfry's default, not the 5,000 `max_limit` allows: a client
 * that reconnects and catches up with open filters otherwise pulls ten times the history it shows.
 */
fun defaultRelayLimits(): RelayLimits =
    RelayLimits(
        maxMessageLength = 262_144,
        maxSubscriptions = 50,
        maxFilters = 20,
        maxLimit = 5_000,
        defaultLimit = 500,
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
            maxMessageLength = maxMessageLengthFromEnv(env) ?: d.maxMessageLength,
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

/** The most UTF-8 bytes a message of the engine's length can take: three per character, as the HTTP body cap counts it. */
fun messageBytes(limits: RelayLimits): Long = (limits.maxMessageLength ?: defaultRelayLimits().maxMessageLength ?: 262_144) * 3L

/**
 * The websocket's frame cap with `MAX_FRAME_BYTES` unset: [DEFAULT_MAX_FRAME_BYTES], or more when the
 * engine admits longer messages, so the engine's length always decides first.
 */
fun defaultMaxFrameBytes(limits: RelayLimits): Int = maxOf(DEFAULT_MAX_FRAME_BYTES.toLong(), messageBytes(limits)).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()

/**
 * `MAX_FRAME_BYTES`: the largest websocket message read, judged from the frame header before its payload is
 * buffered. One under it but over the engine's length is refused by id; one over it closes the connection.
 */
fun maxFrameBytesFromEnv(
    env: Map<String, String>,
    limits: RelayLimits,
): Int {
    val set = env.strictInt("MAX_FRAME_BYTES", 1..Int.MAX_VALUE) ?: return defaultMaxFrameBytes(limits)
    val floor = messageBytes(limits)
    if (set < floor) {
        error(
            "MAX_FRAME_BYTES=$set is below $floor, three bytes for each of MAX_MESSAGE_LENGTH's ${floor / 3} characters: " +
                "a message the engine may accept would close the connection. Raise it to $floor or more, or lower MAX_MESSAGE_LENGTH.",
        )
    }
    return set
}

/**
 * Past every real event, and a bound on what one message may make the relay read: past the engine's length
 * the native transport streams a message without holding it, Ktor holds it whole (docs/decisions/relay-server.md).
 */
const val DEFAULT_MAX_FRAME_BYTES = 16 * 1_048_576

/** `MAX_MESSAGE_LENGTH`, at most a third of the largest frame cap, since a message of it may take three bytes a character. */
private fun maxMessageLengthFromEnv(env: Map<String, String>): Int? {
    val set = env.strictInt("MAX_MESSAGE_LENGTH", MIN_MESSAGE_LENGTH..Int.MAX_VALUE) ?: return null
    if (set > MAX_MESSAGE_LENGTH) {
        error("MAX_MESSAGE_LENGTH=$set is above $MAX_MESSAGE_LENGTH: at three bytes a character its messages would pass the largest MAX_FRAME_BYTES, ${Int.MAX_VALUE}.")
    }
    return set
}

private const val MAX_MESSAGE_LENGTH = Int.MAX_VALUE / 3

/** Room for the smallest EVENT frame: an event's id, pubkey and signature alone are 256 characters. */
private const val MIN_MESSAGE_LENGTH = 512

/** A sha256 has 256 bits, so no event could meet a harder target. */
private const val MAX_POW_DIFFICULTY = 256

/** The negentropy library's smallest frame; it refuses a session below it, so a relay would answer no NEG-OPEN. */
private const val MIN_NEG_FRAME_SIZE = 4_096L

/** `NEG_FRAME_SIZE_LIMIT`: 0 is no limit; any other value is a frame the library can build. */
private fun frameSizeLimitFromEnv(env: Map<String, String>): Long? =
    env.strictLong("NEG_FRAME_SIZE_LIMIT", 0..Long.MAX_VALUE)?.also {
        if (it in 1 until MIN_NEG_FRAME_SIZE) error("NEG_FRAME_SIZE_LIMIT='$it' is below $MIN_NEG_FRAME_SIZE, the smallest frame negentropy builds. Use 0 for no limit or at least $MIN_NEG_FRAME_SIZE.")
    }

/** NIP-77 bounds from the environment. `NEG_MAX_SYNC_EVENTS` caps the ids one reconciliation walks. */
fun negentropySettingsFromEnv(env: Map<String, String>): NegentropySettings {
    val d = NegentropySettings.Default
    return NegentropySettings(
        frameSizeLimit = frameSizeLimitFromEnv(env) ?: d.frameSizeLimit,
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
 * `REPLAY_CONCURRENCY` and `REPLAY_QUEUE`: plain REQ replays the whole relay runs against the store at once, and
 * how many may wait for a turn before a new REQ is refused `rate-limited:`; see `ReplayGate`. 0 replays turns it off.
 */
fun replayGateFromEnv(env: Map<String, String>): ReplayGate =
    ReplayGate(
        permits = env.strictInt("REPLAY_CONCURRENCY", 0..MAX_REPLAY_CONCURRENCY) ?: ReplayGate.DEFAULT_PERMITS,
        maxWaiting = env.strictInt("REPLAY_QUEUE", 0..MAX_REPLAY_QUEUE) ?: ReplayGate.DEFAULT_WAITING,
    )

/**
 * `PUBLISH_IN_FLIGHT_PER_CONNECTION`: publishes one connection may have unanswered before its reader waits for an
 * answer, see `PublishSlots`. 0 turns the bound off.
 */
fun publishSlotsFromEnv(env: Map<String, String>): PublishSlots = PublishSlots(env.strictInt("PUBLISH_IN_FLIGHT_PER_CONNECTION", 0..MAX_PUBLISH_IN_FLIGHT) ?: PublishSlots.DEFAULT_PER_CONNECTION)

/** Past the deepest ingest queue a pipeline builds, one connection's bound holds nothing back. */
private const val MAX_PUBLISH_IN_FLIGHT = 16_384

/** Past the store client's own bound on streams in flight, a replay permit admits nothing more. */
private const val MAX_REPLAY_CONCURRENCY = 65_536
private const val MAX_REPLAY_QUEUE = 1_000_000

/**
 * `RELAY_INGEST_BATCH`: how many published events one store write may take. Unset is [default]:
 * quartz's 64 for its queue, sized for SQLite; the mirror's batch for the pipeline. Set but unparseable
 * stops the boot; see docs/decisions/relay-server.md for why a Vespa store wants more.
 */
fun ingestBatchFromEnv(
    env: Map<String, String>,
    default: Int = IngestQueue.DEFAULT_MAX_BATCH,
): Int = env.strictInt("RELAY_INGEST_BATCH", 1..MAX_INGEST_BATCH) ?: default

/**
 * How publishes reach the store. `RELAY_INGEST=pipeline` (the default) writes them through the
 * mirror's [com.nosfabrica.vespa.relay.ingest.IngestPipeline], `RELAY_INGEST_CONCURRENCY` workers
 * wide, holding at most `RELAY_INGEST_QUEUE_MB` of parsed publishes, a narrow batch waiting up to
 * `RELAY_INGEST_LINGER_MS` for more while publishes keep arriving; `queue` keeps quartz's
 * single-writer [IngestQueue]. Null [tuning] means the queue.
 */
data class RelayIngest(
    val tuning: IngestTuning?,
    val batch: Int,
)

fun relayIngestFromEnv(
    env: Map<String, String>,
    engine: EngineTuning = EngineTuning.fromEnv(env),
): RelayIngest =
    when (env.strictChoice("RELAY_INGEST", setOf("pipeline", "queue")) ?: "pipeline") {
        "queue" -> {
            // Read anyway: a malformed value stops the boot in either mode, like every strict setting.
            env.strictInt("RELAY_INGEST_CONCURRENCY", 1..64)
            env.strictInt("RELAY_INGEST_QUEUE_MB", 16..65_536)
            env.strictInt("RELAY_INGEST_LINGER_MS", 0..MAX_PUBLISH_LINGER_MS)
            verifyThreadsFromEnv(env, "RELAY_INGEST_VERIFY_THREADS")
            RelayIngest(null, ingestBatchFromEnv(env))
        }

        else -> {
            val batch = ingestBatchFromEnv(env, DEFAULT_PIPELINE_BATCH)
            val concurrency = env.strictInt("RELAY_INGEST_CONCURRENCY", 1..64) ?: DEFAULT_PUBLISH_CONCURRENCY
            val queueMb = env.strictInt("RELAY_INGEST_QUEUE_MB", 16..65_536) ?: DEFAULT_PIPELINE_QUEUE_MB
            val lingerMs = env.strictInt("RELAY_INGEST_LINGER_MS", 0..MAX_PUBLISH_LINGER_MS) ?: DEFAULT_PUBLISH_LINGER_MS
            // One batch per worker waiting: enough to keep every worker fed, and no deeper, since
            // each waiting publish is a parsed event in the heap (the mirror's floor is 4,096).
            // And a byte bound over waiting plus in-pass events, because a count is no bound on
            // bytes when every publish may be a full frame.
            RelayIngest(
                IngestTuning(
                    concurrency,
                    batch,
                    dedupChunk = engine.dedupWidth(),
                    queue = batch * concurrency,
                    queueBytes = queueMb * 1_048_576L,
                    lingerMs = lingerMs.toLong(),
                    verifyThreads = verifyThreadsFromEnv(env, "RELAY_INGEST_VERIFY_THREADS"),
                ),
                batch,
            )
        }
    }

/**
 * The mirror lane's pipeline ([com.nosfabrica.vespa.relay.server.MirrorLane]), which writes what
 * the sync process forwards: `RELAY_MIRROR_INGEST_CONCURRENCY` workers (`1`-`64`) of
 * `RELAY_MIRROR_INGEST_BATCH` events (`1`-`20000`), holding at most `RELAY_MIRROR_INGEST_QUEUE_MB`
 * (`16`-`65536`) of parsed events. The defaults are the sync's own write shape (`SYNC_INGEST_*`), so
 * moving the write here changes where it runs, not its shape. Queue depth is one batch per worker,
 * as for publishes: the sync's forwarding window is its own backpressure.
 */
fun mirrorIngestFromEnv(
    env: Map<String, String>,
    engine: EngineTuning = EngineTuning.fromEnv(env),
): IngestTuning {
    val batch = env.strictInt("RELAY_MIRROR_INGEST_BATCH", 1..20_000) ?: DEFAULT_PIPELINE_BATCH
    val concurrency = env.strictInt("RELAY_MIRROR_INGEST_CONCURRENCY", 1..64) ?: DEFAULT_PIPELINE_CONCURRENCY
    val queueMb = env.strictInt("RELAY_MIRROR_INGEST_QUEUE_MB", 16..65_536) ?: DEFAULT_PIPELINE_QUEUE_MB
    return IngestTuning(
        concurrency,
        batch,
        dedupChunk = engine.dedupWidth(),
        queue = batch * concurrency,
        queueBytes = queueMb * 1_048_576L,
        verifyThreads = verifyThreadsFromEnv(env, "RELAY_MIRROR_INGEST_VERIFY_THREADS"),
    )
}

/** How many threads one pipeline's batch may check signatures on (`1`-`64`); 1, the default, checks them on its worker. */
private fun verifyThreadsFromEnv(
    env: Map<String, String>,
    key: String,
): Int = env.strictInt(key, 1..64) ?: 1

/** The store's dedup probe width, which both pipelines chunk their id probes by, since both probe one store. */
private fun EngineTuning.dedupWidth(): Int = dedupChunk ?: EngineTuning.DEFAULT_DEDUP_CHUNK

/** The mirror's own defaults (`RouterConfig`), so one store sees one write shape. */
private const val DEFAULT_PIPELINE_BATCH = 1_000
private const val DEFAULT_PIPELINE_CONCURRENCY = 2

/**
 * One worker for client publishes, not the mirror's two. Commits serialize on the store's writer
 * lock, so a second worker only pays when it can plan a wide batch while the other commits, and
 * publishes are mostly many clients each awaiting their OK: a shallow queue, which two workers
 * split into narrow writes that queue on the lock. Measured 2026-11-22 (docs/ingest-benchmark.md):
 * 20 publishers at 202-322 ev/s with two workers against 316-462 with one, and a 100k bulk
 * publish level within the box's spread (1,580-1,808 against 1,507-1,721). The mirror lane is
 * always a deep queue and keeps [DEFAULT_PIPELINE_CONCURRENCY].
 */
private const val DEFAULT_PUBLISH_CONCURRENCY = 1

/**
 * Parsed publishes the pipeline may hold, in the estimate's (high) megabytes. Measured
 * 2026-10-17: a 256 KB frame, the relay's limit, parses to ~476 KB, so the count cap alone (2,000
 * waiting plus two 1,000-event passes) let a flood of full frames hold ~1.9 GB of a ~4.2 GB
 * default heap, where quartz's queue held ~0.55 GB. 512 estimated MB is ~840 such events,
 * ~400 MB real; typical kind 3s (~53 KB) stay under the count cap long before this.
 */
private const val DEFAULT_PIPELINE_QUEUE_MB = 512

/**
 * How long a narrow batch of publishes may wait for more while they keep arriving: the most the
 * pipeline adds to any publish's OK. Why 20 and not 10 is measured in docs/ingest-benchmark.md.
 */
private const val DEFAULT_PUBLISH_LINGER_MS = 20

/** A linger past this is a latency no publisher would accept for a wider write. */
private const val MAX_PUBLISH_LINGER_MS = 1_000

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
    return HttpRelay(
        gate =
            HttpRelayGate(
                total = env.strictInt("HTTP_RELAY_TOTAL", 0..Int.MAX_VALUE) ?: HttpRelayGate.DEFAULT_TOTAL,
            ),
        deadlineMs = (env.strictInt("HTTP_RELAY_DEADLINE_SECONDS", 1..MAX_DEADLINE_SECONDS) ?: HttpRelay.DEFAULT_DEADLINE_SECONDS.toInt()) * 1000L,
        origins = origins,
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
