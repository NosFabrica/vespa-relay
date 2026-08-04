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

/*
 * The sync process's formatting helpers, `internal` again now that only this
 * module reads them — `fmtDuration`, the one both processes print, stayed in
 * `:common`. A distinct file name on purpose: a second `Format.kt` in this
 * package would compile to the same `FormatKt` facade class as :common's and
 * collide on the classpath.
 */

/**
 * A `created_at` as a UTC day. Day resolution on purpose: it answers "is the
 * walk moving, and roughly where is it".
 */
internal fun fmtDay(seconds: Long): String =
    java.time.Instant
        .ofEpochSecond(seconds)
        .atZone(java.time.ZoneOffset.UTC)
        .toLocalDate()
        .toString()

/** 24.8M rather than 24819118: the magnitude is the point, not the digits. */
internal fun fmtCount(n: Int): String =
    when {
        n >= 1_000_000 -> "%.1fM".format(n / 1_000_000.0)
        n >= 1_000 -> "%.0fk".format(n / 1_000.0)
        else -> n.toString()
    }

/** Wall-clock seconds, the unit every `created_at` in the protocol is in. */
internal fun nowSeconds(): Long = System.currentTimeMillis() / 1000
