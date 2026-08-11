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

import com.nosfabrica.vespa.relay.router.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.router.config.SyncDirection
import com.nosfabrica.vespa.relay.router.config.SyncStream
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.longOrNull
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The manifest is the only statement anywhere of WHAT this mirror holds — the
 * kind list lives in `router.conf`, which no other process reads. What these
 * pin is the shape a client scopes a NIP-45 COUNT to, so a member that changes
 * meaning here silently changes somebody else's denominator.
 */
class SyncManifestTest {
    private val now = 1_800_000_000L

    private fun stream(
        name: String,
        dir: SyncDirection = SyncDirection.DOWN,
        filter: Filter = Filter(kinds = listOf(1)),
    ) = SyncStream(name = name, dir = dir, filter = filter, urls = emptyList(), trusted = false)

    private fun doc(vararg streams: SyncStream): JsonObject = SyncManifest.document(streams.toList(), now)

    private fun streamsOf(doc: JsonObject) = doc.getValue("streams").jsonArray.map { it.jsonObject }

    private fun kindsOf(
        doc: JsonObject,
        index: Int = 0,
    ) = streamsOf(doc)[index]["kinds"]?.jsonArray?.map { it.jsonPrimitive.int }

    @Test
    fun `a stream publishes its name, direction and kinds`() {
        val d = doc(stream("content", filter = Filter(kinds = listOf(1, 0, 30023))))
        val s = streamsOf(d).single()
        assertEquals("content", s.getValue("name").jsonPrimitive.content)
        assertEquals("down", s.getValue("dir").jsonPrimitive.content)
        // Sorted, not as configured: this is a set, and a reader diffing two
        // documents should not see a change because the config was reordered.
        assertEquals(listOf(0, 1, 30023), kindsOf(d))
        assertEquals(now, d.getValue("writtenAt").jsonPrimitive.long)
    }

    @Test
    fun `a duplicated kind is published once`() {
        assertEquals(listOf(0, 1), kindsOf(doc(stream("content", filter = Filter(kinds = listOf(1, 0, 1))))))
    }

    /**
     * The distinction the whole document rests on: no `kinds` member means the
     * stream mirrors whatever its relays serve, which the reader must not
     * mistake for "mirrors nothing" — see `MirrorReport.allKinds`.
     */
    @Test
    fun `a stream with no kinds publishes no kinds member`() {
        assertNull(kindsOf(doc(stream("everything", filter = Filter()))))
    }

    @Test
    fun `an empty kinds list is the same statement as none at all`() {
        assertNull(kindsOf(doc(stream("everything", filter = Filter(kinds = emptyList())))))
    }

    /**
     * A push stream holds nothing HERE, and the direction is what says so. It is
     * published rather than dropped: "we push kind 62 up there" is the fact that
     * explains why 62 is not in the mirrored set.
     */
    @Test
    fun `an up stream is published with its direction`() {
        val s = streamsOf(doc(stream("push", dir = SyncDirection.UP, filter = Filter(kinds = listOf(62))))).single()
        assertEquals("up", s.getValue("dir").jsonPrimitive.content)
        assertEquals(listOf(62), s.getValue("kinds").jsonArray.map { it.jsonPrimitive.int })
    }

    /** A mirror bounded in TIME breaks a count comparison the way a kind bound does. */
    @Test
    fun `a floor is published, and its absence is not invented`() {
        assertEquals(
            1_700_000_000L,
            streamsOf(doc(stream("recent", filter = Filter(kinds = listOf(1), since = 1_700_000_000L)))).single()["since"]?.jsonPrimitive?.longOrNull,
        )
        assertNull(streamsOf(doc(stream("all"))).single()["since"])
    }

    @Test
    fun `every running stream is published, in configuration order`() {
        val d = doc(stream("content"), stream("assertions", filter = Filter(kinds = listOf(30382))), stream("push", dir = SyncDirection.UP))
        assertEquals(listOf("content", "assertions", "push"), streamsOf(d).map { it.getValue("name").jsonPrimitive.content })
    }

    /**
     * The shipped example, through the real loader — the one case where the
     * kinds are not this test's own invention.
     *
     * It is also the shape of the deployment the issue was measured on: one
     * content stream of ~130 kinds beside three small ones. What it pins is that
     * a stream's configured kinds arrive whole, because the number a client
     * computes from them is a denominator.
     */
    @Test
    fun `the shipped example config publishes every stream's own kinds`() {
        val example =
            RouterConfigLoader.parse(
                requireNotNull(listOf(File("../router.conf.example"), File("router.conf.example")).firstOrNull { it.isFile }) {
                    "missing router.conf.example"
                }.readText(),
            )
        val published = streamsOf(SyncManifest.document(example.streams, now))
        assertEquals(example.streams.map { it.name }, published.map { it.getValue("name").jsonPrimitive.content })
        published.forEachIndexed { i, s ->
            assertEquals(
                example.streams[i]
                    .filter.kinds
                    .orEmpty()
                    .sorted(),
                s["kinds"]?.jsonArray?.map { it.jsonPrimitive.int }.orEmpty(),
                "stream '${example.streams[i].name}' publishes kinds its config does not",
            )
        }
    }

    // ---- the file -----------------------------------------------------------

    @Test
    fun `writing lands a parseable document and reports that it did`() {
        val f = File.createTempFile("sync-manifest", ".json").also { it.delete() }
        try {
            assertTrue(SyncManifest(f).write(listOf(stream("content")), now))
            val written = Json.parseToJsonElement(f.readText()).jsonObject
            assertEquals(doc(stream("content")), written)
            // The temp file the atomic move goes through must not survive it —
            // it sits in the directory the relay reads.
            assertFalse(File(f.parentFile, "${f.name}.tmp").exists())
        } finally {
            f.delete()
        }
    }

    /**
     * Unset is not an error and not a crash: the router mirrors exactly as well
     * without a manifest. It returns false so SyncMain can say the one thing
     * that is actually lost — the relay cannot publish the kind set.
     */
    @Test
    fun `with no file nothing is written and the caller is told`() {
        assertFalse(SyncManifest(null).write(listOf(stream("content")), now))
    }

    /**
     * "Never asked to publish" and "asked, and the disk refused" both write
     * nothing, and only the first is a config mistake. They were one boolean
     * once, and the boot line then told an operator with a read-only volume
     * that the setting they had just set was unset.
     */
    @Test
    fun `an unset manifest is distinguishable from a failed write`() {
        assertFalse(SyncManifest(null).publishes)
        val blocked = File.createTempFile("sync-manifest-not-a-dir", ".txt")
        try {
            val configured = SyncManifest(File(blocked, "nested/manifest.json"))
            assertTrue(configured.publishes, "a path was given — the write failing is a different fact")
            assertFalse(configured.write(listOf(stream("content")), now))
        } finally {
            blocked.delete()
        }
    }

    /**
     * An unwritable path costs the manifest, never the mirror. The directory is
     * created where it can be, so a first boot on a fresh volume does not need
     * anyone to have made it.
     */
    @Test
    fun `an unwritable path is survivable`() {
        val blocked = File.createTempFile("sync-manifest-not-a-dir", ".txt")
        try {
            assertFalse(SyncManifest(File(blocked, "nested/manifest.json")).write(listOf(stream("content")), now))
        } finally {
            blocked.delete()
        }
    }

    @Test
    fun `a nested path is created rather than demanded`() {
        val root = File.createTempFile("sync-manifest-dir", "").also { it.delete() }
        val target = File(File(root, "state"), "sync-manifest.json")
        try {
            assertTrue(SyncManifest(target).write(listOf(stream("content")), now))
            assertTrue(target.isFile)
        } finally {
            target.delete()
            target.parentFile?.delete()
            root.delete()
        }
    }

    @Test
    fun `the env names the file, and a blank value publishes nothing`() {
        assertFalse(SyncManifest.fromEnv(mapOf("SYNC_MANIFEST_FILE" to "   ")).write(listOf(stream("content")), now))
        assertFalse(SyncManifest.fromEnv(emptyMap()).write(listOf(stream("content")), now))
        val named = File.createTempFile("sync-manifest-env", ".json").also { it.delete() }
        try {
            assertTrue(SyncManifest.fromEnv(mapOf("SYNC_MANIFEST_FILE" to named.path)).write(listOf(stream("content")), now))
            assertTrue(named.isFile)
        } finally {
            named.delete()
        }
    }
}
