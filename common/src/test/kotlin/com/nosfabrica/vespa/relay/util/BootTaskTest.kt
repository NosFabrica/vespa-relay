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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

class BootTaskTest {
    @Test
    fun theStepRunsBesideTheCallerAndHandsItsResultToAwait() {
        val release = CountDownLatch(1)
        val task =
            BootTask("waits", onFailure = {}) {
                release.await()
                true
            }
        // The caller is not held up by the step: it gets here while the step still waits.
        release.countDown()
        assertTrue(task.await())
    }

    @Test
    fun aFailureStopsTheBootFromItsOwnThreadWithoutWaitingForAwait() {
        val heard = CountDownLatch(1)
        var failure: Throwable? = null
        val boom = IllegalStateException("the relay states another key")
        val task =
            BootTask<Unit>("refuses", onFailure = {
                failure = it
                heard.countDown()
            }) { throw boom }
        // Heard while nobody awaits: the main thread may be minutes into a wait of its own.
        assertTrue(heard.await(5, TimeUnit.SECONDS))
        assertSame(boom, failure)
        assertEquals("the relay states another key", assertFailsWith<IllegalStateException> { task.await() }.message)
    }
}
