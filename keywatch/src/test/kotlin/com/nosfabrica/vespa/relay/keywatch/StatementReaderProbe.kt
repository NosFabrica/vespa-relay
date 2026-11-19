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
package com.nosfabrica.vespa.relay.keywatch

import com.vitorpamplona.quartz.nip01Core.core.Event
import java.io.File
import kotlin.test.Test

/**
 * Reads a JSONL of real events (one per line, as a relay sends them) and prints what the reader
 * makes of each, with the words around the key, for judging its precision by eye. Asserts nothing.
 *
 *   ./gradlew :keywatch:test --tests '*StatementReaderProbe*' -DstatementProbe=/path/claims.jsonl --rerun -i
 */
class StatementReaderProbe {
    @Test
    fun printWhatTheReaderMakesOfRealEvents() {
        val path = System.getProperty("statementProbe")
        if (path.isNullOrEmpty()) {
            println("[skip] StatementReaderProbe — set -DstatementProbe=<events.jsonl>")
            return
        }
        val events = File(path).readLines().filter { it.isNotBlank() }.map { Event.fromJson(it) }
        val read = events.map { it to StatementReader.read(it) }
        val kinds = read.flatMap { (_, s) -> s.map { it::class.simpleName } }.groupingBy { it }.eachCount()
        println("STATEMENTS ${events.size} events -> $kinds; none for ${read.count { it.second.isEmpty() }}")
        read.forEach { (e, statements) ->
            statements.forEach { s ->
                val what =
                    when (s) {
                        is TransitionClaim -> "${s.signedBy} ${s.reason}"
                        is SecondaryClaim -> "SECONDARY"
                        is ThirdPartyStatement -> "THIRD_PARTY against=${s.against}"
                    }
                val text = e.content.replace(Regex("\\s+"), " ")
                val at = text.indexOf("npub1").let { if (it < 0) text.indexOf("nprofile1") else it }.coerceAtLeast(0)
                println("KEPT ${e.id.take(10)} k${e.kind} $what | …${text.substring((at - 110).coerceAtLeast(0), at)}")
            }
        }
    }
}
