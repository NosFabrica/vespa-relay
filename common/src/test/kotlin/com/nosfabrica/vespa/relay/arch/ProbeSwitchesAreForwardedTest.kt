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
package com.nosfabrica.vespa.relay.arch

import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Every `-D` switch a module's probes read is forwarded by that module's `tasks.test`: an
 * unforwarded switch reaches the Gradle daemon, not the test JVM, and the probe skips itself.
 */
class ProbeSwitchesAreForwardedTest {
    @Test
    fun `every property a module's probes read is forwarded by that module's test task`() {
        val missing = mutableListOf<String>()

        for (module in Repo.modules) {
            val read =
                Repo
                    .sources(module, "test")
                    // This file spells the pattern it scans for and is not a probe.
                    .filter { it.name != "ProbeSwitchesAreForwardedTest.kt" }
                    .flatMap { PROPERTY.findAll(it.readText()).map { m -> m.groupValues[1] } }
                    .toSortedSet()
            if (read.isEmpty()) continue
            val build = Repo.buildFile(module)
            // Matched on the `systemProperty("name"` half only, so the read half may be spelled any way.
            read.filterNot { build.contains("""systemProperty("$it"""") }.forEach {
                missing += ":$module reads -D$it in a test and never forwards it"
            }
        }

        assertTrue(
            missing.isEmpty(),
            "a probe switch that is not forwarded reaches the Gradle daemon and not the test JVM, " +
                "so the probe skips itself and the build goes green:\n" + missing.joinToString("\n"),
        )
    }

    private companion object {
        /** `System.getProperty("name")`, how every probe reads its switch. */
        val PROPERTY = Regex("""System\.getProperty\("([A-Za-z0-9_]+)"\)""")
    }
}
