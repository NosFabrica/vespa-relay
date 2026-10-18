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
package com.nosfabrica.vespa.relay.sync

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.io.File
import java.io.IOException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Only a file whose content is bad is moved aside, and a shutdown never cuts a write short. */
class PersistedJsonStateTest {
    private fun stateFile(text: String): File {
        val dir =
            kotlin.io.path
                .createTempDirectory("persisted-state")
                .toFile()
        return File(dir, "state.json").also { it.writeText(text) }
    }

    private fun asideOf(f: File) =
        f.absoluteFile.parentFile
            .listFiles { it -> it.name.startsWith("${f.name}.corrupt-") }
            .orEmpty()

    private fun state(
        f: File,
        readText: (File) -> String = { it.readText() },
    ) = PersistedJsonState(f, "test state", "test-flush", { JsonObject(emptyMap()) }, readText)

    @Test
    fun `a file the process cannot read stops the boot and stays where it is`() {
        val f = stateFile("""{"a":1}""")
        try {
            var discarded = false
            val failure =
                assertFailsWith<IOException> {
                    state(f, readText = { throw java.nio.file.AccessDeniedException(it.path) }).read(restore = {}, discard = { discarded = true })
                }
            assertContains(failure.message.orEmpty(), f.path, message = "the message names the file")
            assertContains(failure.message.orEmpty(), "chown -R 10001:10001", message = "and the likely fix")
            assertTrue(f.isFile, "a good file behind a bad permission is never moved aside")
            assertEquals(0, asideOf(f).size)
            assertFalse(discarded)
        } finally {
            f.parentFile.deleteRecursively()
        }
    }

    @Test
    fun `an Error while restoring is never taken for a bad file`() {
        val f = stateFile("""{"a":1}""")
        try {
            assertFailsWith<OutOfMemoryError> { state(f).read(restore = { throw OutOfMemoryError("test") }, discard = {}) }
            assertTrue(f.isFile, "running out of memory says nothing about the file")
        } finally {
            f.parentFile.deleteRecursively()
        }
    }

    @Test
    fun `close waits out the flusher's write and never interrupts it`() {
        val f = stateFile("{}")
        try {
            val writing = CountDownLatch(1)
            val release = CountDownLatch(1)
            val interrupted = AtomicBoolean(false)
            val flusher = AtomicReference<Thread>()
            val n = AtomicInteger()
            val state =
                PersistedJsonState(f, "test state", "test-flush", {
                    if (Thread.currentThread().name == "test-flush" && flusher.compareAndSet(null, Thread.currentThread())) {
                        writing.countDown()
                        while (true) {
                            try {
                                release.await()
                                break
                            } catch (_: InterruptedException) {
                                interrupted.set(true)
                            }
                        }
                        if (Thread.currentThread().isInterrupted) interrupted.set(true)
                    }
                    JsonObject(mapOf("n" to JsonPrimitive(n.get())))
                })
            n.set(1)
            state.changed()
            state.startPeriodicFlush(1)
            assertTrue(writing.await(10, TimeUnit.SECONDS), "the premise: the flusher is mid-write")
            n.set(2)
            state.changed()

            val closing = thread { state.close() }
            closing.join(300)
            assertTrue(closing.isAlive, "close waits for the write in progress")
            release.countDown()
            closing.join(10_000)

            assertFalse(interrupted.get(), "an interrupt landing in the write would close its channel before the fsync")
            assertFalse(flusher.get().isAlive, "the flusher has stopped by the time close returns")
            assertEquals("2", (Json.parseToJsonElement(f.readText()) as JsonObject).getValue("n").toString(), "and the last change is on disk")
        } finally {
            f.parentFile.deleteRecursively()
        }
    }

    @Test
    fun `a file whose content is bad is moved aside and the owner starts empty`() {
        for (text in listOf("{not json", """{"a":{"b":"not a number"}}""")) {
            val f = stateFile(text)
            try {
                var discarded = false
                val read =
                    state(f).read(
                        restore = { root -> ((root.getValue("a") as JsonObject).getValue("b") as JsonPrimitive).content.toLong() },
                        discard = { discarded = true },
                    )
                assertFalse(read, "for $text")
                assertTrue(discarded, "what restore had filled is emptied for $text")
                assertEquals(1, asideOf(f).size, "and the file is kept aside as evidence for $text")
            } finally {
                f.parentFile.deleteRecursively()
            }
        }
    }
}
