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
package com.nosfabrica.vespa.relay.server

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicReference

/**
 * What `GET /stats.json` serves: the last computed statistics document, held in
 * memory with a validator, and written through to a file so a restart answers
 * with the previous one instead of nothing.
 *
 * ## Why the file and the route are not the same thing
 *
 * The obvious shape is to write the JSON somewhere public and let a static
 * handler serve it. Two reasons it is not that here.
 *
 * The relay has no public static directory. `/web/…` is served from
 * [WebAssets] off the classpath rather than Ktor's `staticResources`,
 * specifically so it can mint content-derived validators — a jar entry's mtime
 * is the build's, not the file's. Adding a disk-backed static root for one file
 * would be a second serving mechanism, with a path-traversal surface, for a
 * document already in memory.
 *
 * And the two have different jobs. The file exists for DURABILITY: a full
 * rollup is minutes of grouping over the corpus, so a deploy must not blank the
 * page, and the operator gets a copy under `/var/lib/vespa-relay` that is
 * readable from the host (the same bind mount the FTS cursor and the parse
 * audit land in). Serving is a different question and is answered from memory,
 * where the bytes and their ETag are already computed.
 *
 * The write is atomic (temp file + move), same as [BanListFile] and for the
 * same reason: a reader that catches a half-written document gets a parse error
 * on the next boot, and the recovery for that is silently starting empty.
 */
class StatsSnapshot(
    /** Where the document is persisted; null/blank keeps it purely in memory. */
    private val path: String? = null,
) {
    /**
     * The served bytes plus the validator over them, swapped as one reference.
     *
     * Atomic because the rollup publishes from a background coroutine while
     * Netty threads serve: two fields would let a request pair new bytes with
     * the old tag, which is the one combination a cache never recovers from.
     */
    private val current = AtomicReference<Served?>(null)

    /** The current document's bytes and ETag, or null before the first rollup. */
    fun served(): Served? = current.get()

    /**
     * Publish [members] into the served document and persist the result.
     *
     * ## Two writers, one document
     *
     * The rollup computes `/stats.json` in two passes on two cadences — cheap
     * counters about once a minute, corpus-wide groupings about once every
     * fifteen; see `StatsTier` for the costs that forced the split — so a
     * publish is a MERGE rather than a replacement. [owns] is what makes that
     * safe: it names the top-level members this writer is responsible for, so
     * the merge can tell "the other tier computed this" from "this tier stopped
     * computing it".
     *
     *  - members in [members] replace whatever was there
     *  - members in [owns] and NOT in [members] are REMOVED — a section that
     *    stops existing (a router whose state files are gone) must disappear
     *    rather than sit there forever at its last value with a timestamp
     *    claiming this pass computed it
     *  - everything else is left exactly as the other writer left it, timestamps
     *    included, which is why each section carries its own `generatedAt`
     *
     * Called with the defaults — no [owns], no [tier] — it replaces the whole
     * document, which is what a single-writer caller and every test that hands
     * over a complete document mean.
     *
     * `@Synchronized` because this is now a read-modify-write over the served
     * document with two coroutines doing it. The [AtomicReference] alone was
     * enough when a publish only ever overwrote; merging without the lock would
     * let the slower tier's read-then-write straddle the faster tier's publish
     * and drop it.
     */
    @Synchronized
    fun publish(
        members: JsonObject,
        owns: Set<String> = emptySet(),
        tier: String? = null,
    ) {
        val doc = merged(members, owns, tier)
        val bytes = JSON.encodeToString(JsonObject.serializer(), doc).toByteArray(Charsets.UTF_8)
        current.set(Served(doc, bytes, etagOf(bytes)))
        save(bytes)
    }

    /**
     * [members] folded onto the served document, per the contract in [publish].
     *
     * Two members are special, and both for the same reason: they are the only
     * ones BOTH writers touch, so neither can own them and a plain replacement
     * would let each tier erase the other's half.
     *
     * `tiers` is a union — every writer contributes the entry under its own name
     * and leaves the rest alone, which is what lets the page print "counters 40s
     * ago, charts 12m ago" from one object.
     *
     * `stale` is the notice a failed pass leaves behind, and it survives ANOTHER
     * tier's success: with two cadences, a charts tier that has been failing all
     * night sits beside counters that are ten seconds old, and clearing the
     * notice on every counters pass would hide exactly the failure the notice
     * exists for. A writer clears its own notice and an unattributed one (the
     * seed marker `loadFromFile` leaves, which any successful pass supersedes).
     */
    private fun merged(
        members: JsonObject,
        owns: Set<String>,
        tier: String?,
    ): JsonObject {
        val base = current.get()?.doc
        if (base == null || owns.isEmpty()) return members
        // A document written against another schema is not something to merge
        // onto: its members can mean different things, and the ones this schema
        // dropped are owned by nobody now, so they would linger forever beside
        // numbers that superseded them. Replacing it costs the other tier's
        // sections until its next pass — visible for minutes after an upgrade,
        // stated by their absence, and preferable to a document that is half
        // schema 1 and half schema 2 while claiming to be one of them.
        if (base["schema"] != members["schema"]) return members
        val out = LinkedHashMap<String, JsonElement>(base)
        owns.forEach { out.remove(it) }
        out.remove("stale")
        members.forEach { (member, value) -> out[member] = value }
        val wasTiers = base["tiers"] as? JsonObject
        val nowTiers = members["tiers"] as? JsonObject
        if (wasTiers != null && nowTiers != null) out["tiers"] = JsonObject(wasTiers + nowTiers)
        val stale = base["stale"] as? JsonObject
        val staleTier = (stale?.get("tier") as? JsonPrimitive)?.contentOrNull
        if (stale != null && staleTier != null && staleTier != tier) out["stale"] = stale
        return JsonObject(out)
    }

    /**
     * Say, in the served document, that it is no longer current — and why.
     *
     * ## The failure this ends
     *
     * A rollup that throws keeps the previous document serving, which is right:
     * blanking a page because one refresh failed is worse than showing a slightly
     * old one. But the page then had NO WAY to tell the two apart. A document
     * from four hours ago and one from four minutes ago carried the same
     * `generatedAt` semantics, the same everything — so a stats service that had
     * been failing all night looked exactly like a healthy one, and the operator
     * only ever found out by noticing a number that had not moved. "Stale" and
     * "crashed" were the same picture.
     *
     * So the previous document is re-published with a `stale` member naming the
     * reason and the moment the attempt was made. It costs one parse-and-encode
     * per FAILURE, which is a cost paid only when something is already wrong.
     *
     * ## Why it re-publishes instead of a side channel
     *
     * The document is the artifact. A header, or a second endpoint, would be a
     * fact about the response rather than about the data, and every reader that
     * caches or forwards `/stats.json` — which is the point of the ETag — would
     * drop it. Anything that reads the document reads this with it.
     *
     * Silent no-op before the first rollup: there is nothing to mark, and
     * minting a document whose only member is `stale` would serve a shape the
     * page has to special-case for a state it already draws ("not computed yet").
     */
    @Synchronized
    fun markStale(
        reason: String,
        atSeconds: Long = System.currentTimeMillis() / 1000,
        /**
         * Which cadence failed, or null for a notice about the whole document.
         *
         * Carried INTO the document because a reader has to know which numbers
         * the notice is about — a document whose charts are four hours old and
         * whose counters are forty seconds old is not "stale", it is half stale
         * — and read back out by [publish], which clears a notice only for the
         * tier that left it.
         */
        tier: String? = null,
        /**
         * Whether the marked document is written back to [path].
         *
         * False on the SEED path. `loadFromFile` marks what it just read, and
         * with a plain `publish` that turned a documented read into a boot-time
         * rewrite of the same bytes — noisy on a read-only or full volume, for a
         * notice the next successful rollup clears anyway. A rollup FAILURE does
         * persist: that document is what the next restart will seed from, and it
         * should carry the reason with it.
         */
        persist: Boolean = true,
    ) {
        val existing = current.get() ?: return
        runCatching {
            val doc = existing.doc
            // Rebuilt rather than mutated in place: `stale` must go on the
            // OUTSIDE of whatever the rollup produced, and a document that
            // already carries one from a previous failure gets the newer reason
            // rather than two.
            val marked =
                buildJsonObject {
                    for ((member, value) in doc) if (member != "stale") put(member, value)
                    put(
                        "stale",
                        buildJsonObject {
                            put("reason", reason)
                            put("since", atSeconds)
                            tier?.let { put("tier", it) }
                            // The reader's arithmetic, done here, because the
                            // interesting quantity is the AGE and the page would
                            // otherwise have to know the rollup's interval to
                            // decide whether one skipped refresh is a problem.
                            //
                            // The failing TIER's own timestamp when there is one,
                            // not the document's: with two cadences the document
                            // was touched by the healthy half seconds ago, and
                            // stamping the notice with that would say the numbers
                            // it is warning about are current.
                            (tierGeneratedAt(doc, tier) ?: (doc["generatedAt"] as? JsonPrimitive)?.contentOrNull)
                                ?.let { put("generatedAt", it) }
                        },
                    )
                }
            val bytes = JSON.encodeToString(JsonObject.serializer(), marked).toByteArray(Charsets.UTF_8)
            current.set(Served(marked, bytes, etagOf(bytes)))
            if (persist) save(bytes)
        }.onFailure {
            // The served document stays exactly as it was. Losing the staleness
            // notice is a small harm; losing the statistics because the notice
            // could not be attached is not one worth risking.
            System.err.println("stats: could not mark the served document stale (${it.message})")
        }
    }

    /**
     * Seed from [path] if it holds a parseable document. A missing or corrupt
     * file is not an error: the next rollup overwrites it, and the page's own
     * "not computed yet" state is the correct thing to show meanwhile. Corrupt
     * still gets a line — silently serving nothing looks exactly like a relay
     * whose rollup never runs.
     */
    fun loadFromFile() {
        val file = File(path ?: return)
        if (path.isBlank() || !file.exists()) return
        runCatching {
            val bytes = file.readBytes()
            // Parsed, not trusted as-is: serving whatever the file holds would
            // let a hand-edited or truncated document out under this relay's
            // name, and the parse is the only check available. Kept, not
            // discarded: the next publish MERGES onto it — one tier's pass must
            // not blank the other's sections just because the process restarted
            // between them.
            val doc = Json.parseToJsonElement(bytes.decodeToString()).jsonObject
            current.set(Served(doc, bytes, etagOf(bytes)))
            // Seeded, not computed — and the page must be able to tell. This
            // document was written by a previous process and can be arbitrarily
            // old; a deploy that ships a broken rollup would otherwise serve
            // last week's numbers under this relay's name with nothing saying so.
            // Cleared by the first successful rollup, which publishes without it.
            markStale(
                "served from $path after a restart; no rollup has completed in this process yet",
                // Seeding must not write. See [markStale]'s `persist`.
                persist = false,
            )
        }.onFailure { e ->
            System.err.println("stats: could not read $path (${e.message}) — serving nothing until the first rollup")
        }
    }

    @Synchronized
    private fun save(bytes: ByteArray) {
        val target = File(path?.takeIf { it.isNotBlank() } ?: return)
        target.absoluteFile.parentFile?.mkdirs()
        val tmp = File(target.path + ".tmp")
        runCatching {
            tmp.writeBytes(bytes)
            Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        }.recoverCatching {
            // Some filesystems don't support ATOMIC_MOVE; fall back to a plain replace.
            Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }.onFailure { e ->
            // Not fatal — the in-memory document is already serving. But an
            // unwritable state file means every restart recomputes from
            // scratch, which on this corpus is minutes of blank page.
            System.err.println("stats: could not persist to $path: ${e.message}")
        }
    }

    /**
     * One published document: the exact bytes served, the validator over them,
     * and the parsed form the next publish merges onto.
     *
     * The tree is kept rather than re-parsed on demand because both remaining
     * readers of it are on a hot-ish path — every merge and every staleness
     * notice — and because re-parsing 100KB of JSON to answer "what did the
     * other tier publish" is work this already has the answer to. [bytes] stays
     * the served form: the ETag is over those exact bytes, and re-encoding the
     * tree to serve it could mint different bytes for the same validator.
     */
    class Served(
        val doc: JsonObject,
        val bytes: ByteArray,
        val etag: String,
    )

    private companion object {
        /** When the tier named [tier] last computed anything, per the document's own `tiers` member. */
        fun tierGeneratedAt(
            doc: JsonObject,
            tier: String?,
        ): String? =
            ((doc["tiers"] as? JsonObject)?.get(tier ?: return null) as? JsonObject)
                ?.get("generatedAt")
                ?.let { (it as? JsonPrimitive)?.contentOrNull }

        /**
         * Not pretty-printed. This document is machine-read first — the page
         * fetches it, and anyone charting our coverage against a network-wide
         * dashboard fetches it too — and indentation on a 50-kind table is
         * bytes across the wire on every poll.
         */
        val JSON = Json

        /** The same content-derived strong ETag the pages and modules use — see `etagOf` in HttpServer. */
        fun etagOf(bytes: ByteArray): String =
            MessageDigest
                .getInstance("SHA-256")
                .digest(bytes)
                .take(8)
                .joinToString("") { "%02x".format(it) }
                .let { "\"$it\"" }
    }
}
