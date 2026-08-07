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

import java.io.File
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DeletionGuardsTest {
    /** Tests run from the module dir; the deployment files sit at the repo root. */
    private fun repoFile(name: String): File = requireNotNull(listOf(File("../$name"), File(name)).firstOrNull { it.isFile }) { "missing $name" }

    @Test
    fun `only what the store itself reads as on counts as on`() {
        // The store accepts "1" or a strict "true" and treats everything else
        // as its cached fast path. A value it would ignore must fail HERE,
        // while it is still a boot message, rather than pass as protection
        // nobody has.
        assertTrue(deletionGuardsEnforced("1"))
        assertTrue(deletionGuardsEnforced("true"))
        assertFalse(deletionGuardsEnforced(null))
        assertFalse(deletionGuardsEnforced(""))
        assertFalse(deletionGuardsEnforced("0"))
        assertFalse(deletionGuardsEnforced("false"))
        assertFalse(deletionGuardsEnforced("yes"), "the store does not read this as true, so neither may we")
        assertFalse(deletionGuardsEnforced("TRUE"), "toBooleanStrictOrNull is case-sensitive")
    }

    @Test
    fun `a process that would skip the checks does not start`() {
        requireDeletionGuards(mapOf(DELETION_GUARD_ENV to "1"))
        // Unset is the dangerous spelling — it is what a hand-rolled launcher
        // produces, and the store reads it as "use the cache".
        val failure = assertFailsWith<IllegalStateException> { requireDeletionGuards(emptyMap()) }
        assertTrue(DELETION_GUARD_ENV in failure.message.orEmpty(), "the message must name the variable to set")
        assertFailsWith<IllegalStateException> { requireDeletionGuards(mapOf(DELETION_GUARD_ENV to "0")) }
    }

    @Test
    fun `every shipped way to launch either process sets it`() {
        // The tripwire above turns a missing value into a container that will
        // not start, so the launch paths are part of the mechanism rather than
        // documentation of it: drop the line and every deployment is down.
        assertTrue(
            repoFile("Dockerfile").readText().contains(Regex("""^ENV $DELETION_GUARD_ENV=1$""", RegexOption.MULTILINE)),
            "the image carries both entrypoints and must set $DELETION_GUARD_ENV",
        )
        for (module in listOf("relay", "sync")) {
            assertTrue(
                repoFile("$module/build.gradle.kts").readText().contains("""environment("$DELETION_GUARD_ENV", "1")"""),
                "./gradlew :$module:run would refuse to boot",
            )
        }
    }
}
