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
package com.nosfabrica.vespa.relay.util

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import kotlin.concurrent.thread
import kotlin.system.exitProcess

/**
 * One step of a boot run beside the main thread's own, on a thread of its own, and awaited where the
 * boot first needs it: a step that only waits (on another container, a port bind) no longer holds
 * up the steps that do not depend on it. The sync waited out its relay's start before even opening
 * its store, and bound its pages after both.
 *
 * A step that fails stops the boot AT ONCE, from its own thread, as it would have inline, rather
 * than at [await]: the main thread may be minutes into a wait of its own by then.
 */
class BootTask<T>(
    name: String,
    onFailure: (Throwable) -> Unit = { e ->
        e.printStackTrace()
        exitProcess(1)
    },
    work: () -> T,
) {
    private val result = CompletableFuture<T>()

    init {
        thread(name = "boot: $name", isDaemon = true) {
            try {
                result.complete(work())
            } catch (e: Throwable) {
                // The handler first: when it stops the process, a caller already in [await] never
                // also wakes to report the same failure.
                onFailure(e)
                result.completeExceptionally(e)
            }
        }
    }

    /** The step's result, once it has one; its failure, rethrown, if [onFailure] returned. */
    fun await(): T =
        try {
            result.get()
        } catch (e: ExecutionException) {
            throw e.cause ?: e
        }
}
