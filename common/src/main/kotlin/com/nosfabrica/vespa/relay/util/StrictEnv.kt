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
 * The one way both processes read a number or a switch from the environment. Unset or blank
 * is null, for the caller's default; anything else either parses inside its range or stops the
 * boot, because a value read as the default is a setting nobody can see is not applied.
 */

/** [key] as a whole number within [range]. */
fun Map<String, String>.strictInt(
    key: String,
    range: IntRange = Int.MIN_VALUE..Int.MAX_VALUE,
): Int? =
    setting(key)?.let { raw ->
        raw.toIntOrNull()?.takeIf { it in range } ?: error("$key='$raw' is not a whole number${bounds(range.first.toLong(), range.last.toLong(), Int.MIN_VALUE.toLong(), Int.MAX_VALUE.toLong())}.")
    }

/** [key] as a whole number within [range]. */
fun Map<String, String>.strictLong(
    key: String,
    range: LongRange = Long.MIN_VALUE..Long.MAX_VALUE,
): Long? =
    setting(key)?.let { raw ->
        raw.toLongOrNull()?.takeIf { it in range } ?: error("$key='$raw' is not a whole number${bounds(range.first, range.last, Long.MIN_VALUE, Long.MAX_VALUE)}.")
    }

/** [key] as a switch: `true`/`1`/`yes`/`on` or `false`/`0`/`no`/`off`, in any case. */
fun Map<String, String>.strictFlag(key: String): Boolean? =
    setting(key)?.let { raw ->
        when (raw.lowercase()) {
            "true", "1", "yes", "on" -> true
            "false", "0", "no", "off" -> false
            else -> error("$key='$raw' is not a switch. Use true or false.")
        }
    }

/** [key] as one of [choices], lowercased. */
fun Map<String, String>.strictChoice(
    key: String,
    choices: Set<String>,
): String? =
    setting(key)?.lowercase()?.also { value ->
        if (value !in choices) error("$key='$value' is not one of ${choices.sorted().joinToString()}.")
    }

private fun Map<String, String>.setting(key: String): String? = this[key]?.trim()?.takeIf { it.isNotEmpty() }

/** The range as words, empty where the type's own limits are the only bound. */
private fun bounds(
    min: Long,
    max: Long,
    floor: Long,
    ceiling: Long,
): String =
    when {
        min == floor && max == ceiling -> ""
        min == floor -> " of at most $max"
        max == ceiling -> " of at least $min"
        else -> " in $min..$max"
    }
