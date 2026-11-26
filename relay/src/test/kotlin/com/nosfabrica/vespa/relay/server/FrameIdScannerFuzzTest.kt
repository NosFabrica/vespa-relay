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

import io.netty.buffer.Unpooled
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * [FrameIdScanner]'s padded head against quartz's own readers on the whole frame: the event id
 * (`eventIdOfFrame`) and, failing one, the label and sub id (`subIdOfFrame`) must agree, the frame fed
 * masked, in random pieces. Quartz keeps the readers internal; they are public in its bytecode. A failure
 * names its seed.
 */
class FrameIdScannerFuzzTest {
    private val readers = Class.forName("com.vitorpamplona.quartz.nip01Core.relay.server.EventFrameIdKt")
    private val eventIdOf = readers.getMethod("eventIdOfFrame", String::class.java)
    private val subIdOf = readers.getMethod("subIdOfFrame", String::class.java)
    private val subIdClass = Class.forName("com.vitorpamplona.quartz.nip01Core.relay.server.FrameSubId")
    private val label = subIdClass.getMethod("getLabel")
    private val subId = subIdClass.getMethod("getSubId")

    /** What quartz answers [frame] by: its event id, else its label and sub id, else nothing. */
    private fun quartz(frame: String): String =
        (eventIdOf.invoke(null, frame) as String?)?.let { "id $it" }
            ?: subIdOf.invoke(null, frame)?.let { "sub ${label.invoke(it)} ${subId.invoke(it)}" }
            ?: "none"

    private fun scan(
        r: Random,
        bytes: ByteArray,
    ): String {
        val mask = r.nextInt()
        val masked = ByteArray(bytes.size) { (bytes[it].toInt() xor (mask ushr (24 - 8 * (it and 3)))).toByte() }
        val scanner = FrameIdScanner(HEAD_BYTES)
        var at = 0
        while (at < bytes.size) {
            val n = minOf(bytes.size - at, if (r.nextBoolean()) r.nextInt(1, 8) else r.nextInt(1, 70_000))
            scanner.feed(Unpooled.wrappedBuffer(masked), at, n, mask, at and 3)
            at += n
        }
        return scanner.head() + " ".repeat(300)
    }

    private val hex = "0123456789abcdef"

    private fun id(r: Random): String =
        when (r.nextInt(8)) {
            0 -> String(CharArray(64) { hex[r.nextInt(16)] }).uppercase()
            1 -> String(CharArray(63) { hex[r.nextInt(16)] })
            2 -> String(CharArray(65) { hex[r.nextInt(16)] })
            3 -> String(CharArray(62) { hex[r.nextInt(16)] }) + "\\\\"
            else -> String(CharArray(64) { hex[r.nextInt(16)] })
        }

    private fun space(r: Random) = listOf("", "", " ", "\n", "\t ", "\r\n ")[r.nextInt(6)]

    private fun string(
        r: Random,
        canBeHuge: Boolean = true,
    ): String {
        val pieces = listOf("a", "id", "\\\"", "\\\\", "\\u0069d", "\"id\":\"${id(r)}\"".replace("\"", "\\\""), "€", "😀", "{", "]", ":", ",")
        val big = if (canBeHuge && r.nextInt(30) == 0) "x".repeat(r.nextInt(60_000, 90_000)) else ""
        return "\"" + big + (0 until r.nextInt(6)).joinToString("") { pieces[r.nextInt(pieces.size)] } + "\""
    }

    private fun value(
        r: Random,
        depth: Int,
        canBeHuge: Boolean = true,
    ): String =
        when (if (depth > 3) r.nextInt(3) else r.nextInt(6)) {
            0 -> string(r, canBeHuge)
            1 -> r.nextInt(-5, 100).toString()
            2 -> listOf("true", "null", "false")[r.nextInt(3)]
            3 -> "[" + (0 until r.nextInt(4)).joinToString(",") { space(r) + value(r, depth + 1) } + "]"
            else -> obj(r, depth + 1)
        }

    private fun obj(
        r: Random,
        depth: Int,
    ): String {
        val keys = listOf("id", "id", "pubkey", "content", "tags", "kind", "\\u0069d", "ID", "id ", "i")
        return "{" +
            (0 until r.nextInt(6)).joinToString(",") {
                val key = keys[r.nextInt(keys.size)]
                val v = if (key == "id" && r.nextInt(3) > 0) "\"${id(r)}\"" else value(r, depth)
                space(r) + "\"" + key + "\"" + space(r) + ":" + space(r) + v
            } + "}"
    }

    private fun frame(r: Random): String {
        val labels = listOf("EVENT", "EVENT", "AUTH", "REQ", "COUNT", "CLOSE", "EVENTS", "EVEN", "OK", "RE\\u0051", "")
        val l = labels[r.nextInt(labels.size)]
        // A sub id longer than the kept head is the one known difference (a NOTICE where quartz would CLOSE it).
        val second =
            when (r.nextInt(4)) {
                0 -> string(r, canBeHuge = false)
                1 -> value(r, 0, canBeHuge = false)
                else -> obj(r, 0)
            }
        val rest = (0 until r.nextInt(3)).joinToString("") { "," + space(r) + value(r, 1) }
        var text = space(r) + "[" + space(r) + "\"" + l + "\"" + space(r) + "," + space(r) + second + rest + space(r) + "]"
        when (r.nextInt(6)) {
            0 -> text = text.substring(0, r.nextInt(text.length + 1))
            1 -> text = text.replaceFirst(",", "")
            2 -> text = "garbage" + text
        }
        return text
    }

    @Test
    fun `the scanner's head gets quartz's answer for the whole frame`() {
        for (seed in 1..4_000) {
            val r = Random(seed)
            val text = frame(r)
            val bytes = text.toByteArray(Charsets.UTF_8)
            assertEquals(quartz(String(bytes, Charsets.UTF_8)), quartz(scan(r, bytes)), "seed $seed: ${text.take(200)}")
        }
    }

    @Test
    fun `random bytes get the same answer too`() {
        for (seed in 1..2_000) {
            val r = Random(seed)
            val alphabet = "[]{}\",:\\ idEVNTAUTHREQ0123456789abcdef".toByteArray()
            val bytes = ByteArray(r.nextInt(0, 300)) { if (r.nextInt(10) == 0) r.nextInt(256).toByte() else alphabet[r.nextInt(alphabet.size)] }
            assertEquals(quartz(String(bytes, Charsets.UTF_8)), quartz(scan(r, bytes)), "seed $seed")
        }
    }
}
