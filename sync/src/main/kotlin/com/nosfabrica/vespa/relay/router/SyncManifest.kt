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

import com.nosfabrica.vespa.relay.router.config.SyncStream
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import java.io.File
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption

/**
 * What this router is CONFIGURED to mirror, written where the relay can read it.
 *
 * ## The number this exists to make computable
 *
 * A client comparing "events we hold for an author" against "events the author's
 * relay holds for them" is comparing a FILTERED mirror against an UNFILTERED
 * total, and the quotient can never reach 100%. Measured on
 * `460c25e6…065c` against a complete mirror: 31,118 here of 89,485 there, drawn
 * as *35% mirroring* on a relay that was not behind by one event. Every kind we
 * hold was in the sync's filter and every kind absent from our store was outside
 * it — 3, 4, 5, 6, 7 and 1059, of which reactions alone accounted for thousands
 * for that one author, and two of which (4, 1059) are encrypted DMs and gift
 * wraps this relay has no reason to ever mirror.
 *
 * The fix is to scope both sides of that comparison to the kinds the mirror
 * actually asks for, and the only place that list exists is here: `router.conf`
 * is read by this process and by nothing else. A client cannot hardcode a copy —
 * the list is edited (kinds 5 and 62 were queued to be added while this was
 * being written), and a stale copy drifts silently in the direction of the same
 * wrong percentage.
 *
 * ## Why a file, and why this file
 *
 * The router has no HTTP server; the relay is the thing that serves pages, and
 * it already reads two of this process's files off the `/var/lib/vespa-relay`
 * mount both containers share — see `SyncCoverageReport` on the relay side for
 * the full argument. This is the third, on the same terms: the router is the
 * only writer, the relay's read is best-effort, and nothing here may ever cost
 * either process anything.
 *
 * It is deliberately NOT folded into `SYNC_STATE_FILE`. Bands are state the
 * router accumulates and rewrites every 30 seconds; this is CONFIG, written once
 * at boot because that is the only moment it can change — a `router.conf` edit
 * is a `restart sync`, never a reload. Putting a config statement inside a state
 * file would make every band flush re-assert it.
 *
 * ## What it says, and what it deliberately does not
 *
 * ```json
 * {
 *   "writtenAt": 1770000000,
 *   "streams": [
 *     {"name": "content",    "dir": "down", "kinds": [0, 1, 3, …]},
 *     {"name": "assertions", "dir": "down", "kinds": [30382]},
 *     {"name": "outbox",     "dir": "up"}
 *   ]
 * }
 * ```
 *
 * The streams are the ones this process is RUNNING, not every stream in the
 * config: `SYNC_STREAMS` narrows the run, and a stream that was narrowed out
 * mirrors nothing while it is narrowed out. Saying otherwise would restate the
 * config as coverage, which is the mistake this whole file exists to correct.
 *
 * A stream whose filter names no `kinds` gets no `kinds` member — it mirrors
 * every kind its relays serve, and an omitted member is how the reader learns
 * there is no bound to apply. `since` rides along when a stream has one, because
 * a mirror bounded in TIME breaks a count comparison exactly the way a mirror
 * bounded in kinds does.
 *
 * No union is computed here. The relay publishes one (`MirrorReport`), and two
 * places computing the same set from the same data is two places that can
 * disagree about it.
 *
 * `writtenAt` is the disclosure that makes the rest safe to trust: this file
 * outlives the process that wrote it, so a mirror that has been switched off, or
 * a `router.conf` that changed under a router nobody restarted, is a stale
 * document that still parses. The timestamp is the only thing that can say so.
 */
class SyncManifest(
    /** Where the manifest is written; null publishes nothing — see [write]. */
    private val file: File?,
) {
    /**
     * Whether this router publishes a manifest at all — i.e. whether
     * `SYNC_MANIFEST_FILE` named a path.
     *
     * Separate from [write]'s result on purpose. Both a router that was never
     * given a path and one whose disk refused the write end up publishing
     * nothing, but they are different problems with different fixes, and the two
     * were briefly one boolean — which had the caller announce "unset" at an
     * operator who had set it perfectly well and whose volume was read-only.
     */
    val publishes: Boolean get() = file != null

    /**
     * Publish [streams]. Returns whether anything was written, so the caller can
     * say so on the one line a boot gets.
     *
     * Never throws: a sync process that cannot write this file still mirrors
     * perfectly, and the whole cost of the failure is that the relay serves no
     * mirrored kind set. The failure is LOUD for that reason — the symptom on
     * the other side is a client that silently falls back to comparing against
     * an unfiltered total, which is the bug this replaces.
     */
    fun write(
        streams: List<SyncStream>,
        nowSeconds: Long = System.currentTimeMillis() / 1000,
    ): Boolean {
        val f = file ?: return false
        return runCatching {
            f.parentFile?.mkdirs()
            val tmp = File(f.parentFile ?: File("."), "${f.name}.tmp")
            tmp.writeText(json.encodeToString(JsonObject.serializer(), document(streams, nowSeconds)))
            // Temp file plus an atomic move, for the same reason the bands are
            // written that way: the relay reads this on its own schedule, and a
            // reader that catches a half-written document parses nothing.
            try {
                Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
            } catch (_: AtomicMoveNotSupportedException) {
                Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING)
            }
        }.onFailure {
            System.err.println("router: could not write the sync manifest to ${f.path} (${it.message}) — the relay cannot publish which kinds this mirror holds")
        }.isSuccess
    }

    companion object {
        // Pretty-printed. It is a handful of lines an operator reads to answer
        // "what does this router actually mirror", and the poll cost the stats
        // document worries about is not paid here — the relay reads it from
        // disk on its own rollup, never per request.
        private val json = Json { prettyPrint = true }

        /** The document, pure, so it can be asserted without a filesystem. */
        fun document(
            streams: List<SyncStream>,
            nowSeconds: Long,
        ): JsonObject =
            buildJsonObject {
                put("writtenAt", nowSeconds)
                putJsonArray("streams") {
                    for (stream in streams) {
                        add(
                            buildJsonObject {
                                put("name", stream.name)
                                put("dir", stream.dir.wire)
                                // Sorted and de-duplicated: this is a SET being
                                // published, and a reader diffing two documents
                                // should not see a change because the config
                                // file listed the same kinds in another order.
                                stream.filter.kinds?.takeIf { it.isNotEmpty() }?.let { kinds ->
                                    putJsonArray("kinds") { for (kind in kinds.distinct().sorted()) add(kind) }
                                }
                                // Only when the stream HAS a floor. A mirror
                                // that reaches all the way back carries no
                                // `since`, and writing one anyway would be a
                                // bound the reader then applies to a comparison
                                // that does not need it.
                                stream.filter.since?.let { put("since", it) }
                            },
                        )
                    }
                }
            }

        /**
         * `SYNC_MANIFEST_FILE` — where the manifest is written. Unset publishes
         * nothing, which is the right default for a router with no relay beside
         * it, and `SyncMain` says so on boot rather than leaving the absence to
         * be discovered from the other side.
         *
         * No `ROUTER_*` spelling: those exist for settings that predate the
         * rename, and inventing a legacy name for a setting that never had one
         * is a second thing to grep for that can never appear in a real config.
         */
        fun fromEnv(env: Map<String, String>): SyncManifest =
            SyncManifest(
                env["SYNC_MANIFEST_FILE"]
                    ?.trim()
                    ?.takeIf { it.isNotEmpty() }
                    ?.let(::File),
            )
    }
}
