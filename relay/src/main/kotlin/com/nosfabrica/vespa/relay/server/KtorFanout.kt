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
package com.nosfabrica.vespa.relay.server

import com.vitorpamplona.quartz.nip01Core.relay.server.FanoutExecutor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * The threads that deliver Ktor connections' live events, as each event loop does for the native transport's:
 * quartz hands an accepted event once to every one of them holding a match, so the ingest thread never walks the
 * subscriptions. A connection keeps the executor [next] gave it for life, which keeps its live frames in order.
 */
class KtorFanout(
    private val executors: List<FanoutExecutor>,
) : AutoCloseable {
    init {
        require(executors.isNotEmpty()) { "a fan-out needs at least one executor" }
    }

    private val turn = AtomicInteger()

    /** The executor a new connection delivers its live events on: each in turn, so one instance per thread is shared. */
    fun next(): FanoutExecutor = executors[Math.floorMod(turn.getAndIncrement(), executors.size)]

    override fun close() = executors.forEach { (it as? AutoCloseable)?.close() }

    companion object {
        /** One thread per core, each started by its first task, so a relay serving no Ktor socket starts none. */
        fun perCore(): KtorFanout = KtorFanout(List(Runtime.getRuntime().availableProcessors()) { FanoutThread("vespa-relay-fanout-$it") })
    }
}

/** One daemon thread running its tasks in submission order, queued without bound as an event loop's are. */
internal class FanoutThread(
    private val name: String,
) : FanoutExecutor,
    AutoCloseable {
    private val executor: Lazy<ExecutorService> = lazy { Executors.newSingleThreadExecutor { r -> Thread(r, name).apply { isDaemon = true } } }

    override fun execute(task: () -> Unit) = executor.value.execute(task)

    override fun close() {
        if (executor.isInitialized()) executor.value.shutdown()
    }
}
