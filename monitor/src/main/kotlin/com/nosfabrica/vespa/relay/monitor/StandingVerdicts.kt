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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import kotlinx.coroutines.CancellationException

/**
 * The one read of what this monitor stands behind that a pass publishes against; on failure it says so and
 * throws [StandingUnread]. A verdict signed blind can contradict the record, so the pass ends there.
 * A read past [RelayVerdictRecord.ReadTimeout]'s deadline is a failure like any other.
 */
internal suspend fun readStanding(
    pass: String,
    label: String,
    read: suspend () -> RelayVerdictRecord.Verdicts,
): RelayVerdictRecord.Verdicts =
    try {
        read()
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        System.err.println(
            "monitor: $pass [$label] — could not read the standing verdicts (${whyUnread(e)}); " +
                "nothing measured or published, every url is measured again next pass",
        )
        throw StandingUnread(e)
    }

/** A pass that measured and published nothing because its standing read failed, already said; its run did not complete. */
internal class StandingUnread(
    cause: Exception,
) : Exception("the standing verdicts could not be read", cause)

/** A failed verdict read in words: a timeout names its deadline, anything else its class. */
internal fun whyUnread(e: Exception): String = if (e is RelayVerdictRecord.ReadTimeout) e.message.orEmpty() else e.javaClass.simpleName
