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

import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import java.net.URI
import java.time.Duration

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
        // "schema:", the prefix every line about the application package carries.
        System.err.println(
            "schema: deploy to $configUrl failed (${e.message?.take(200)}); " +
                "serving on the schema Vespa already has — writes carrying fields it lacks will be rejected until a deploy succeeds",
        )
    }
    deployer.awaitServing(vespaUrl)
}

/**
 * Waits until an application serves at [vespaUrl], for a process that never deploys one: the
 * sync, since the relay is the only writer and owns the schema. A package deploys whole, so a sync
 * of another build deploying its own would replace the relay's, and the relay's writes carrying
 * fields that package lacks would be refused until the relay's next boot. Checks `/ApplicationStatus`
 * on the query endpoint, so it needs no config server; throws after [timeout].
 */
fun awaitServedSchema(
    vespaUrl: String,
    timeout: Duration = Duration.ofMinutes(10),
    poll: Duration = Duration.ofSeconds(5),
    isServing: (String) -> Boolean = SchemaDeployer(vespaConfigUrlFor(vespaUrl))::isServing,
) {
    if (isServing(vespaUrl)) return
    System.err.println("schema: nothing serves at $vespaUrl yet — waiting for the relay's deploy (up to ${timeout.toMinutes()} min)")
    val deadline = System.nanoTime() + timeout.toNanos()
    while (!isServing(vespaUrl)) {
        check(System.nanoTime() < deadline) {
            "no application served at $vespaUrl within ${timeout.toMinutes()} min. The relay deploys the schema at its boot " +
                "(AUTO_DEPLOY); start it, or deploy your own package, and this process will find it."
        }
        Thread.sleep(poll.toMillis())
    }
    System.err.println("schema: an application serves at $vespaUrl")
}

/** The config server sits on :19071 by convention, on the same host as the query endpoint. */
fun vespaConfigUrlFor(queryUrl: String): String {
    val u = URI.create(queryUrl)
    return URI(u.scheme, null, u.host, 19071, null, null, null).toString()
}
