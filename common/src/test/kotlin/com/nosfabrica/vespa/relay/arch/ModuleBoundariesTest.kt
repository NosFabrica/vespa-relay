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
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The module graph and the package map, pinned. Both erode silently: a dependency added for one
 * call compiles, and a package that starts spanning two modules only shows up when somebody
 * looks for a file and cannot say which module holds it.
 */
class ModuleBoundariesTest {
    /** Scopes that put a module on another's compile classpath, as opposed to its test one. */
    private val compileScopes = setOf("api", "implementation")

    private fun projectDeps(module: String): List<Pair<String, String>> =
        PROJECT_DEP
            .findAll(Repo.buildFile(module))
            .map { it.groupValues[1] to it.groupValues[2] }
            .toList()

    /** The packages a module declares in its main sources, which is what depending on it can reach. */
    private fun owned(module: String): Set<String> = Repo.packages(module, "main")

    /** The packages one source set of a module imports from, with the imported name dropped. */
    private fun importedPackages(
        module: String,
        sourceSet: String,
    ): Set<String> = Repo.imports(module, sourceSet).map { it.substringBeforeLast('.') }.toSet()

    @Test
    fun `a dependency only ever points backwards along the include order`() {
        // settings.gradle.kts includes the modules in layering order, and that order is the rule.
        val rank = Repo.modules.withIndex().associate { (i, m) -> m to i }
        val forwards =
            Repo.modules.flatMap { module ->
                projectDeps(module)
                    .map { (_, target) -> target }
                    .filter { rank.getValue(it) >= rank.getValue(module) }
                    .map { ":$module depends on :$it, which comes later in settings.gradle.kts" }
            }

        assertTrue(forwards.isEmpty(), "the include order is the layering:\n" + forwards.joinToString("\n"))
    }

    @Test
    fun `the two processes never depend on each other`() {
        // Each is a process of its own so the mirror can restart without the relay noticing.
        val processes = Repo.modules.filter { Repo.buildFile(it).contains("\n    application\n") }
        assertEquals(setOf("relay", "sync"), processes.toSet(), "a new process module belongs in this rule")
        processes.forEach { module ->
            val named = projectDeps(module).map { it.second }.filter { it in processes }
            assertTrue(named.isEmpty(), ":$module names $named, and a process is not a library")
        }
    }

    @Test
    fun `web depends on no module of ours`() {
        // The seam is /stats.json: engines produce the document, :web renders it. A dependency
        // either way puts Ktor in an engine or the store behind a route.
        assertEquals(emptyList(), projectDeps("web"), ":web renders documents and knows nothing that makes them")
    }

    @Test
    fun `no engine module compiles against Ktor`() {
        val offenders =
            (Repo.modules - "web" - "relay").flatMap { module ->
                Repo
                    .sources(module, "main")
                    .filter { it.readText().contains("\nimport io.ktor.") }
                    .map { "$module: ${it.relativeTo(Repo.dir(module))}" }
            }

        assertTrue(offenders.isEmpty(), "Ktor lives in :web, and in :relay behind it:\n" + offenders.joinToString("\n"))
    }

    @Test
    fun `web never opens the store`() {
        val offenders =
            listOf("main", "test").flatMap { set ->
                importedPackages("web", set).filter { it.startsWith(STORE) }
            }

        assertTrue(offenders.isEmpty(), ":web is handed a document and never reads one:\n" + offenders.joinToString("\n"))
    }

    @Test
    fun `common never takes quartz's relay client`() {
        // :common is what the serving relay also reads. Talking to other relays is :peers.
        val offenders =
            listOf("main", "test").flatMap { set ->
                importedPackages("common", set).filter { it.startsWith(CLIENT) || it.startsWith(SOCKETS) }
            }

        assertTrue(offenders.isEmpty(), "the relay client belongs to :peers:\n" + offenders.joinToString("\n"))
    }

    @Test
    fun `every declared module dependency is used`() {
        // A dependency nothing imports is a false edge on the graph, and it re-exports whatever
        // that module exports: :monitor carried :web, and with it Ktor, on one test's import.
        val unused =
            Repo.modules.flatMap { module ->
                projectDeps(module).mapNotNull { (scope, target) ->
                    val set = if (scope in compileScopes) "main" else "test"
                    val reached = importedPackages(module, set).intersect(owned(target))
                    "$scope(project(\":$target\")) in :$module, imported by nothing in src/$set".takeIf { reached.isEmpty() }
                }
            }

        assertTrue(unused.isEmpty(), "declare the dependency where it is used:\n" + unused.joinToString("\n"))
    }

    @Test
    fun `a process module exports nothing`() {
        // `api` is for a module something else compiles against; a process is the end of the graph.
        val exporting =
            Repo.modules
                .filter { Repo.buildFile(it).contains("\n    application\n") }
                .filter { API.containsMatchIn(Repo.buildFile(it)) }

        assertTrue(exporting.isEmpty(), "$exporting use `api(` for a dependency nothing can consume")
    }

    @Test
    fun `every package belongs to one module`() {
        val owners = mutableMapOf<String, MutableSet<String>>()
        Repo.modules.forEach { module ->
            (Repo.packages(module, "main") + Repo.packages(module, "test")).forEach {
                owners.getOrPut(it) { mutableSetOf() } += module
            }
        }
        val split =
            owners
                .filterValues { it.size > 1 }
                .filterKeys { it != ROOT }
                .map { (pkg, modules) -> "$pkg is declared by ${modules.sorted()}" }

        assertTrue(
            split.isEmpty(),
            "a package spanning modules cannot be navigated to from its name:\n" + split.joinToString("\n"),
        )
    }

    @Test
    fun `the root package holds one entrypoint per process and nothing else`() {
        val stray =
            Repo.modules.flatMap { module ->
                listOf("main", "test").flatMap { set ->
                    Repo
                        .sources(module, set)
                        .filter { f -> ROOT_PACKAGE.containsMatchIn(f.readText()) && !f.name.endsWith("Main.kt") }
                        .map { "$module: ${it.relativeTo(Repo.dir(module))}" }
                }
            }

        assertTrue(stray.isEmpty(), "$ROOT is for the entrypoints; everything else names its subsystem:\n" + stray.joinToString("\n"))
    }

    private companion object {
        const val ROOT = "com.nosfabrica.vespa.relay"
        const val STORE = "com.nosfabrica.vespa.eventstore"
        const val CLIENT = "com.vitorpamplona.quartz.nip01Core.relay.client"
        const val SOCKETS = "com.vitorpamplona.quartz.nip01Core.relay.sockets"
        val ROOT_PACKAGE = Regex("""^package $ROOT$""", RegexOption.MULTILINE)
        val PROJECT_DEP = Regex("""(\w+)\(project\(":([A-Za-z0-9_-]+)"\)\)""")

        /** `api(` at the start of a dependency line, so a comment mentioning it does not count. */
        val API = Regex("""^\s+api\(""", RegexOption.MULTILINE)
    }
}
