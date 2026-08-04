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
package com.nosfabrica.vespa.relay.maintenance

import com.vitorpamplona.quartz.utils.Log
import com.vitorpamplona.quartz.utils.LogLevel

/**
 * `QUARTZ_LOG_LEVEL` — quartz's own log floor (it defaults to DEBUG, which
 * prints a line per unparseable event a backfill meets).
 *
 * The one piece of ParseAudit both processes read, split out when the audit
 * moved to `:sync`: the relay wants the floor but must NOT install the audit —
 * nothing on the serving side calls `inspect()`, and an installed-but-unfed
 * audit is precisely the silently-inert configured component this codebase
 * forbids.
 */
fun applyQuartzLogLevel(env: Map<String, String>) {
    env["QUARTZ_LOG_LEVEL"]?.trim()?.uppercase()?.takeIf { it.isNotEmpty() }?.let { name ->
        val level = LogLevel.entries.firstOrNull { it.name == name }
        if (level == null) {
            System.err.println("QUARTZ_LOG_LEVEL '$name' is not one of ${LogLevel.entries.joinToString("/") { it.name }} — ignored")
        } else {
            Log.minLevel = level
        }
    }
}
