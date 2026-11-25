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
package com.nosfabrica.vespa.relay.sync

import java.util.concurrent.ConcurrentHashMap

/**
 * Whether a peer's reconciled set is believable enough to act on its absences. A trust-gated peer
 * reading our signed-in key through its lens answers empty and refuses nothing, so a set that
 * shrank past one part in [SHARE] is never acted on: nothing deleted, nothing pushed, until the peer
 * answers a believable set again. A real mass retraction is then the operator's to apply.
 */
internal class ShrinkGuard {
    /** What each peer held at the last answer acted on, by caller key. */
    private val held = ConcurrentHashMap<String, Int>()

    /** Deletes on [peerHeld]: not when the peer holds none of our [mine], under a tenth of it, or under a tenth of what it held before. */
    fun mayDelete(
        key: String,
        mine: Int,
        peerHeld: Int,
    ): Boolean = settle(key, peerHeld, suspect = mine > 0 && (peerHeld == 0 || (mine >= FLOOR && peerHeld * SHARE < mine) || shrank(key, peerHeld)))

    /** Pushes on [peerHeld]: not when it is under a tenth of what the peer held before, nor, where it [mayBeLensed], under a tenth of [mine]. */
    fun mayPush(
        key: String,
        mine: Int,
        peerHeld: Int,
        mayBeLensed: Boolean,
    ): Boolean = settle(key, peerHeld, suspect = shrank(key, peerHeld) || (mayBeLensed && mine > 0 && peerHeld * SHARE < mine))

    private fun shrank(
        key: String,
        peerHeld: Int,
    ): Boolean = held[key]?.let { it >= FLOOR && peerHeld * SHARE < it } == true

    /** A believable answer becomes the next one's baseline; a suspect one leaves the old baseline standing. */
    private fun settle(
        key: String,
        peerHeld: Int,
        suspect: Boolean,
    ): Boolean {
        if (!suspect) held[key] = peerHeld
        return !suspect
    }

    companion object {
        /** A set smaller than this may shrink to anything without suspicion. */
        const val FLOOR = 10

        /** A shrink below one part in this many is suspect. */
        const val SHARE = 10
    }
}
