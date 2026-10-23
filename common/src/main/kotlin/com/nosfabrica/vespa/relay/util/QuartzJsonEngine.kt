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

import com.vitorpamplona.quartz.nip01Core.core.JsonEngine

/**
 * `QUARTZ_JSON_ENGINE`, the code that reads and writes every Nostr frame, event and event id:
 * `default` (Jackson) or `kite`, quartz's own. Read by both processes before anything parses.
 */
fun applyQuartzJsonEngine(env: Map<String, String>): JsonEngine {
    val name = env.strictChoice("QUARTZ_JSON_ENGINE", JsonEngine.entries.map { it.name.lowercase() }.toSet()) ?: return JsonEngine.current
    val engine = JsonEngine.valueOf(name.uppercase())
    JsonEngine.current = engine
    System.err.println("quartz: json engine $name")
    return engine
}
