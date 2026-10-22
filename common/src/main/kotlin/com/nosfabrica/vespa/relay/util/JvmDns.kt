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

import java.security.Security

/**
 * How long this JVM remembers a host name's answer. Both processes reach their peers by compose
 * service name (`vespa`, `relay`), and a peer that is recreated comes back under the same name and
 * maybe another address, so a remembered answer is a wrong one.
 *
 * The JDK's defaults are the trap: a FAILED lookup is remembered for 10 s
 * (`networkaddress.cache.negative.ttl` in `java.security`), so a relay that restarted was
 * unreachable to the sync's writer for up to 10 s after its name resolved again, and a good one for
 * 30 s. Docker's resolver answers locally in well under a millisecond, so asking again costs nothing.
 *
 * Security properties, read once when the JDK first resolves a name: call this before anything
 * does, first thing in `main`.
 */
fun cacheDnsBriefly() {
    Security.setProperty("networkaddress.cache.ttl", DNS_TTL_SECONDS.toString())
    Security.setProperty("networkaddress.cache.negative.ttl", "0")
}

/** Seconds a successful lookup is reused: a reconnect storm resolves once, a recreated peer is found within this. */
private const val DNS_TTL_SECONDS = 5
