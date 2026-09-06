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

/** Every browser file belongs in `:web`; engines publish `/stats.json` and render nothing. */
class NoBrowserFilesInEngineModulesTest {
    @Test
    fun `no module but web ships a browser file`() {
        val root = File(".").absoluteFile.parentFile.let { if (File(it, "settings.gradle.kts").isFile) it else it.parentFile }
        val offenders =
            listOf("common", "peers", "monitor", "sync", "relay")
                .map { File(root, it) }
                .filter { it.isDirectory }
                .flatMap { module ->
                    File(module, "src")
                        .walkTopDown()
                        .filter { it.isFile && it.extension.lowercase() in BROWSER }
                        .map { "${module.name}: ${it.relativeTo(module)}" }
                }

        assertTrue(
            offenders.isEmpty(),
            "browser files belong in :web — engines publish /stats.json and render nothing:\n" + offenders.joinToString("\n"),
        )
    }

    private companion object {
        /** Extensions, not paths: the failure to catch is a new file somewhere plausible. */
        val BROWSER = setOf("html", "js", "mjs", "css")
    }
}
