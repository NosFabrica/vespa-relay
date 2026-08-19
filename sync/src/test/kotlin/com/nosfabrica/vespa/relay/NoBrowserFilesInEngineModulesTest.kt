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
 * ENGINES PRODUCE DOCUMENTS; :web RENDERS THEM. The seam is `/stats.json`, and
 * this is the half of it a compiler cannot hold.
 *
 * ## Why it needs a test
 *
 * The layout drifted into the state this replaced by an argument that sounds
 * right every time: a page belongs next to the thing that serves it. It does
 * not. `:sync` and `:monitor` each ended up with one `.html` and one
 * `cards.js`, while three quarters of the same design — the stylesheet, the DOM
 * vocabulary, the render engine — sat in `:web`, so the line between the two
 * was "how many modules happen to import this file" rather than a principle.
 *
 * It cost more than tidiness. Assets spread across module roots do not resolve
 * for a filesystem-based test the way they do for a server reading the
 * classpath, so the JS suite needed its own resolver hook — machinery whose
 * only job was to undo the split. Putting every browser file in one module
 * deleted it.
 *
 * ## What "engine module" means
 *
 * Everything but `:web`. An engine may serve a page — `SyncMain` binds two —
 * but it does so with markup and modules it does not own, which is exactly the
 * relationship a back end has with a front end.
 */
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
        /**
         * What counts as browser code.
         *
         * Extensions rather than paths: the failure this catches is a NEW file
         * put somewhere plausible, and a path list would only ever describe
         * where the last one went.
         */
        val BROWSER = setOf("html", "js", "mjs", "css")
    }
}
