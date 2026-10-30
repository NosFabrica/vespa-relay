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

import java.time.ZoneId

/**
 * Reads the JDK's time-zone rules now, while a file can still be opened. The JDK loads them from
 * `tzdb.dat` the first time anything asks for a zone, and java.util.logging's formatter is the
 * first thing that does here: Netty logs through it (no SLF4J provider is on the classpath), and
 * the first warning it logs may be a failed accept because the process has run out of file
 * descriptors. Then the rules cannot be read, their class fails to initialise for the life of the
 * process, and the error escapes Netty's warning and ends the thread that accepts connections:
 * measured, a relay flooded past its fd limit and then emptied answered nothing again, HTTP or
 * websocket, until restarted. Loaded at boot, the warning prints and the acceptor carries on.
 */
fun loadTimeZoneRules() {
    ZoneId.systemDefault().rules
}
