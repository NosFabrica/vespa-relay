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
import java.io.FileOutputStream
import java.nio.channels.FileChannel
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption

/**
 * Replaces [target] with [bytes] so a crash leaves the old file or the new one, never a torn or empty
 * one: the temp file reaches the device before the rename, and the rename before this returns.
 */
fun writeDurably(
    target: File,
    bytes: ByteArray,
) {
    val dir = target.absoluteFile.parentFile
    dir.mkdirs()
    val tmp = File(dir, "${target.name}.tmp")
    try {
        FileOutputStream(tmp).use { out ->
            out.write(bytes)
            out.fd.sync()
        }
        try {
            Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }
    } catch (e: Exception) {
        tmp.delete()
        throw e
    }
    syncDirectory(dir)
}

fun writeDurably(
    target: File,
    text: String,
) = writeDurably(target, text.toByteArray(Charsets.UTF_8))

/**
 * Moves an unreadable [file] aside as `<name>.corrupt-<epochMillis>`, so the next save starts fresh
 * without overwriting the evidence. Null when there was nothing to move or the move failed.
 */
fun quarantine(
    file: File,
    nowMillis: Long = System.currentTimeMillis(),
): File? {
    if (!file.exists()) return null
    val aside = File(file.absoluteFile.parentFile, "${file.name}.corrupt-$nowMillis")
    return runCatching { Files.move(file.toPath(), aside.toPath()) }.map { aside }.getOrNull()
}

/** The rename is durable only once the directory entry is; a filesystem that cannot sync one is left as is. */
private fun syncDirectory(dir: File) {
    runCatching { FileChannel.open(dir.toPath(), StandardOpenOption.READ).use { it.force(true) } }
}
