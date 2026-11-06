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

/**
 * [html] with `<meta name="sync-url" content="…">` before `</head>`, the address the search page asks
 * the sync at about a reader's own posts. Null leaves the page byte-identical, and the page then skips
 * that question. A page with no `</head>` is returned unchanged rather than guessed at.
 */
fun pageWithSyncUrl(
    html: String,
    url: String?,
): String {
    val value = url?.takeIf { it.isNotBlank() } ?: return html
    val at = html.indexOf("</head>")
    if (at < 0) return html
    return html.substring(0, at) + """<meta name="sync-url" content="${escapeAttribute(value)}" />""" + "\n" + html.substring(at)
}
