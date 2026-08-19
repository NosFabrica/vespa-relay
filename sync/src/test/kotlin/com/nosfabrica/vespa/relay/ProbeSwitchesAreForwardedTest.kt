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
package com.nosfabrica.vespa.relay

import java.io.File
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * EVERY PROBE SWITCH IS FORWARDED TO THE MODULE THAT READS IT.
 *
 * ## The failure this closes
 *
 * A probe is a test that skips itself unless a system property asks for it, and
 * a property on the Gradle command line reaches the DAEMON — the tests run in a
 * FORKED JVM that never sees it. Each module's `tasks.test` has to forward the
 * ones its own probes read.
 *
 * A missing forward does not fail. The probe prints its own `[skip]` line and
 * passes, which is indistinguishable from a probe nobody asked for: an operator
 * runs the documented command, sees a green build, and concludes the thing they
 * were measuring is fine. That is the same class of silence as a knob that is
 * accepted and does nothing, which this repo refuses everywhere else.
 *
 * It became reachable the moment the module split moved the fold, consistency
 * and auth probes out of `:sync` and into `:monitor`, leaving nine forwards
 * behind in a build file whose tests no longer read them.
 *
 * ## Why it reads the build files as text
 *
 * Because that is where the bug is. A test that asked Gradle's own model would
 * be asserting that the configuration it was handed matches itself.
 */
class ProbeSwitchesAreForwardedTest {
    @Test
    fun `every property a module's probes read is forwarded by that module's test task`() {
        val root = File(".").absoluteFile.parentFile.let { if (File(it, "settings.gradle.kts").isFile) it else it.parentFile }
        val modules = listOf("common", "peers", "monitor", "sync", "relay", "web")
        val missing = mutableListOf<String>()

        for (module in modules) {
            val dir = File(root, module)
            val tests = File(dir, "src/test")
            if (!tests.isDirectory) continue
            val read =
                tests
                    .walkTopDown()
                    // This file is the scanner, not a probe: its own KDoc
                    // spells the pattern it looks for, and matching that was
                    // the first thing it reported.
                    .filter { it.extension == "kt" && it.name != "ProbeSwitchesAreForwardedTest.kt" }
                    .flatMap { PROPERTY.findAll(it.readText()).map { m -> m.groupValues[1] } }
                    .toSortedSet()
            if (read.isEmpty()) continue
            val build = File(dir, "build.gradle.kts").takeIf { it.isFile }?.readText().orEmpty()
            // The forward is `systemProperty("name", it)` — matched on that
            // rather than on the whole idiom, so a build file that spells the
            // read half differently still counts as forwarding.
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
        /** `System.getProperty("name")` — how every probe in this repo reads its switch. */
        val PROPERTY = Regex("""System\.getProperty\("([A-Za-z0-9_]+)"\)""")
    }
}
