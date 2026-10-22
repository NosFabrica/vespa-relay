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

import java.io.File
import java.util.concurrent.ConcurrentHashMap

/**
 * Where a process is in its boot: one `boot:` line per step, stamped with the milliseconds since the
 * OS started this process — the JVM's own start-up included, which a first log line cannot show —
 * so the relay's and the sync's steps read on one scale and a slow one names itself. A restarted
 * relay took six seconds to answer its first request, and only a line per step said where they went.
 *
 * [once] is for the firsts a hot path reaches many times (the first event stored, the first
 * sign-in): it prints on the first call and costs a set lookup after.
 */
object BootClock {
    private val startedAt: Long = System.currentTimeMillis() - (procAgeMs() ?: processHandleAgeMs())

    /**
     * This process's age from the kernel's own clock: the system's uptime less the tick the process
     * started on, both counted from boot, so 10 ms resolution. Not [ProcessHandle]'s start instant,
     * which adds that tick to `/proc/stat`'s boot time, kept in whole seconds: measured, every boot
     * line read ~0.9 s late on one host, by an amount that changes when the host reboots.
     */
    private fun procAgeMs(): Long? =
        runCatching {
            // Fields after the parenthesised command name, which may itself hold spaces; the
            // start time is field 22 of stat(5), the 20th of these.
            val fields = File("/proc/self/stat").readText().substringAfterLast(") ").split(' ')
            val startTicks = fields[19].toLong()
            val uptimeSeconds = File("/proc/uptime").readText().substringBefore(' ').toDouble()
            (uptimeSeconds * 1000).toLong() - startTicks * 1000 / CLOCK_TICKS_PER_SECOND
        }.getOrNull()?.takeIf { it >= 0 }

    private fun processHandleAgeMs(): Long =
        ProcessHandle
            .current()
            .info()
            .startInstant()
            .map { System.currentTimeMillis() - it.toEpochMilli() }
            .orElse(0L)

    /** USER_HZ, which Linux fixes at 100 for /proc whatever the kernel's own tick rate. */
    private const val CLOCK_TICKS_PER_SECOND = 100L

    private val seen = ConcurrentHashMap.newKeySet<String>()

    /** Milliseconds since the process started. */
    fun elapsedMs(): Long = System.currentTimeMillis() - startedAt

    fun mark(step: String) {
        System.err.println("boot: +${elapsedMs()}ms $step")
    }

    fun once(step: String) {
        if (seen.add(step)) mark(step)
    }

    /**
     * [mark] for a process's `main`, saying so when the JVM maps no class-data archive: the image's
     * AppCDS archive is ~1.5 s of a boot, and the JVM drops it without a word when its flags no longer
     * match the ones it was dumped with, a heap over 32 GB or ZGC among them (no compressed oops).
     */
    fun main(process: String) {
        val vm = System.getProperty("java.vm.info").orEmpty()
        mark("$process: main" + if ("sharing" in vm) "" else " (class-data sharing OFF: every class loads from its jar; java.vm.info=\"$vm\")")
    }
}
