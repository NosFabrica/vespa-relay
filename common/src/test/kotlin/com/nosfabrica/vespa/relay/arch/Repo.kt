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

import java.io.File

/**
 * The checkout, for the guards that read the source tree instead of running it. The modules come
 * from `settings.gradle.kts`, so a new one is covered by these tests the day it is added.
 */
object Repo {
    private val INCLUDE = Regex("""^include\(":([A-Za-z0-9_-]+)"\)""", RegexOption.MULTILINE)
    private val PACKAGE = Regex("""^package (\S+)""", RegexOption.MULTILINE)
    private val IMPORT = Regex("""^import (\S+)""", RegexOption.MULTILINE)

    /** Found from whichever module directory the test JVM was forked in. */
    val root: File =
        File(".").absoluteFile.parentFile.let { if (File(it, "settings.gradle.kts").isFile) it else it.parentFile }

    /** The modules in the order `settings.gradle.kts` includes them, which is the layering. */
    val modules: List<String> =
        INCLUDE
            .findAll(File(root, "settings.gradle.kts").readText())
            .map { it.groupValues[1] }
            .toList()

    fun dir(module: String): File = File(root, module)

    fun buildFile(module: String): String = File(dir(module), "build.gradle.kts").takeIf { it.isFile }?.readText().orEmpty()

    /** Every `.kt` file in one source set of one module; empty when the module has no such sources. */
    fun sources(
        module: String,
        sourceSet: String,
    ): Sequence<File> =
        File(dir(module), "src/$sourceSet")
            .takeIf { it.isDirectory }
            ?.walkTopDown()
            ?.filter { it.isFile && it.extension == "kt" }
            ?: emptySequence()

    /** Every file under a module's `src`, whatever its extension; empty when it has none. */
    fun files(module: String): Sequence<File> =
        File(dir(module), "src")
            .takeIf { it.isDirectory }
            ?.walkTopDown()
            ?.filter { it.isFile }
            ?: emptySequence()

    /** A tracked file at the checkout root, or null. Tests run from a module dir, so this resolves it. */
    fun file(name: String): File? = File(root, name).takeIf { it.isFile }

    /** The packages a module declares in one source set. */
    fun packages(
        module: String,
        sourceSet: String,
    ): Set<String> = sources(module, sourceSet).mapNotNull { PACKAGE.find(it.readText())?.groupValues?.get(1) }.toSet()

    /** Every name imported by one source set of one module, file by file. */
    fun imports(
        module: String,
        sourceSet: String,
    ): Sequence<String> = sources(module, sourceSet).flatMap { f -> IMPORT.findAll(f.readText()).map { it.groupValues[1] } }
}
