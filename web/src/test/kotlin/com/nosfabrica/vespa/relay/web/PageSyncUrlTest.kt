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
package com.nosfabrica.vespa.relay.web

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The search page learns where the sync is from one meta the relay writes, and only from that. */
class PageSyncUrlTest {
    private val page = "<html>\n<head>\n<title>search</title>\n</head>\n<body></body>\n</html>"

    @Test
    fun `a named sync is written into the head, escaped`() {
        val out = pageWithSyncUrl(page, "https://sync.example.com/a?\"b\"")
        assertTrue(out.contains("""<meta name="sync-url" content="https://sync.example.com/a?&quot;b&quot;" />"""), out)
        assertTrue(out.indexOf("sync-url") < out.indexOf("</head>"), "inside the head, where the page reads it")
    }

    @Test
    fun `no sync, or no head, leaves the page as it was`() {
        assertEquals(page, pageWithSyncUrl(page, null))
        assertEquals(page, pageWithSyncUrl(page, " "))
        assertEquals("<p>fragment</p>", pageWithSyncUrl("<p>fragment</p>", "https://sync.example.com"))
    }

    /** The meta the page's script reads: renaming one side without the other turns the posts stage off in silence. */
    @Test
    fun `the page's script reads the meta this writes`() {
        val script = assertNotNullResource("/web/shared/coverage.js")
        assertTrue(script.contains("""meta[name="sync-url"]"""), "shared/coverage.js no longer reads <meta name=\"sync-url\">")
    }

    private fun assertNotNullResource(path: String): String =
        PageSyncUrlTest::class.java.getResourceAsStream(path)?.use { it.readBytes().decodeToString() }
            ?: error("$path is not on the :web classpath")
}
