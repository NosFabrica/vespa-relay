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
package com.nosfabrica.vespa.relay.store

import com.nosfabrica.vespa.eventstore.SchemaDeployer
import java.net.URI

/**
 * Deploys the bundled Vespa application package on every boot and waits until Vespa serves it.
 * Against a Vespa already serving, a failed deploy keeps the process up on the schema Vespa has;
 * on a fresh Vespa it rethrows.
 */
fun deployBundledSchema(
    vespaUrl: String,
    configUrl: String,
) {
    val deployer = SchemaDeployer(configUrl)
    try {
        deployer.deploy()
    } catch (e: Exception) {
        if (!deployer.isServing(vespaUrl)) throw e
        // "schema:", not "relay:": both processes deploy.
        System.err.println(
            "schema: deploy to $configUrl failed (${e.message?.take(200)}); " +
                "serving on the schema Vespa already has — writes carrying fields it lacks will be rejected until a deploy succeeds",
        )
    }
    deployer.awaitServing(vespaUrl)
}

/** The config server sits on :19071 by convention, on the same host as the query endpoint. */
fun vespaConfigUrlFor(queryUrl: String): String {
    val u = URI.create(queryUrl)
    return URI(u.scheme, null, u.host, 19071, null, null, null).toString()
}
