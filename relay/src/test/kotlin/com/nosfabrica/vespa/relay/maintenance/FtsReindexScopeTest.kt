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
package com.nosfabrica.vespa.relay.maintenance

import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/** REINDEX_FTS_KINDS: what it accepts, and the cursor file that must not resume one scope's walk under another. */
class FtsReindexScopeTest {
    @Test
    fun `unset or blank is the whole corpus`() {
        assertNull(parseReindexKinds(null))
        assertNull(parseReindexKinds(""))
        assertNull(parseReindexKinds("   "))
    }

    /** The list docs/migrations.md prints is comma-and-newline wrapped; it must paste as is. */
    @Test
    fun `a pasted list parses, deduplicated and sorted`() {
        assertEquals(listOf(5, 14, 38000), parseReindexKinds("38000, 5,\n14, 5"))
    }

    /** A typo must not quietly become the whole-corpus walk the scope exists to avoid. */
    @Test
    fun `a value that is not a kind stops the boot`() {
        val e = assertFailsWith<IllegalStateException> { parseReindexKinds("5, 14, 3800O") }
        assertContains(e.message!!, "'3800O'")
        assertFailsWith<IllegalStateException> { parseReindexKinds("70000") }
        assertFailsWith<IllegalStateException> { parseReindexKinds("-1") }
    }

    /** Separators alone parse to no kinds; the store refuses that scope on every page, so the boot must. */
    @Test
    fun `a value that names no kinds stops the boot`() {
        assertContains(assertFailsWith<IllegalStateException> { parseReindexKinds(",") }.message!!, "names no kinds")
        assertFailsWith<IllegalStateException> { parseReindexKinds(" , \n ,") }
    }

    @Test
    fun `a cursor round-trips with its scope`() {
        val kinds = listOf(5, 14, 38000)
        assertEquals(FtsCursor(kinds, "abc=="), decodeFtsCursor(encodeFtsCursor(kinds, "abc==")))
        assertEquals(FtsCursor(null, "abc=="), decodeFtsCursor(encodeFtsCursor(null, "abc==")))
        assertEquals(FtsCursor(kinds, null), decodeFtsCursor(encodeFtsCursor(kinds, null)))
    }

    /** Files written before scoping hold the bare cursor of a whole-corpus walk, and resume only that. */
    @Test
    fun `a bare legacy cursor is the whole-corpus walk's`() {
        assertEquals(FtsCursor(null, "legacy-cursor"), decodeFtsCursor("legacy-cursor\n"))
    }

    /** A cursor from another scope addresses a different walk: resuming it would skip or misread documents. */
    @Test
    fun `a cursor resumes only the scope that wrote it`() {
        val scoped = FtsCursor(listOf(5, 14), "c1")
        assertEquals("c1", resumableCursor(scoped, listOf(5, 14)))
        assertNull(resumableCursor(scoped, listOf(5, 14, 62)), "a widened list starts over")
        assertNull(resumableCursor(scoped, null), "the whole-corpus walk does not resume a scoped one")
        assertNull(resumableCursor(FtsCursor(null, "c2"), listOf(5)), "nor the reverse")
        assertEquals("c2", resumableCursor(FtsCursor(null, "c2"), null))
    }
}
