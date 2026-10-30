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

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.app.VespaApp
import com.nosfabrica.vespa.relay.util.BootTask
import java.time.Duration

/**
 * Deploys the bundled Vespa application package unless Vespa already has it active, and returns
 * once every Vespa service runs it. Against a Vespa already serving, a failed deploy keeps the
 * process up on the schema Vespa has; on a fresh Vespa it rethrows.
 *
 * Unchanged is the common case, every restart that is not an upgrade, and it used to redeploy
 * anyway: 2-3 s of the relay's boot, and a new config generation every Vespa service reloaded.
 * Changed waits for the services, not just the container: activation returns before the content
 * nodes run the new generation, and a write carrying a field the old one lacks is refused there.
 *
 * Unchanged still owes that wait when a deploy has not settled: a relay killed while waiting for
 * its own deploy finds the package active on its next boot, with the services still converging.
 * That check runs beside the caller's next steps and is returned to be awaited before anything
 * writes; one `serviceconverge` read when they have settled, the common case.
 */
fun deployBundledSchema(
    vespaUrl: String,
    configUrl: String,
): BootTask<Boolean>? {
    val deployer = SchemaDeployer(configUrl)
    val zip = VespaApp.zipBytes()
    if (deployer.isServing(vespaUrl) && deployer.serves(zip)) {
        System.err.println("schema: Vespa already runs this build's application package; not redeploying")
        return BootTask("schema convergence") { deployer.awaitConverged(null, UNCHANGED_CONVERGE_WAIT) }
    }
    System.err.println("schema: deploying the bundled application package to $configUrl")
    try {
        val generation = SchemaDeployer.generationOf(deployer.deploy(zip))
        if (deployer.awaitConverged(generation)) System.err.println("schema: every Vespa service runs config generation ${generation ?: "(the one wanted)"}")
    } catch (e: Exception) {
        if (!deployer.isServing(vespaUrl)) throw e
        // "schema:", the prefix every line about the application package carries.
        System.err.println(
            "schema: deploy to $configUrl failed (${e.message?.take(200)}); " +
                "serving on the schema Vespa already has — writes carrying fields it lacks will be rejected until a deploy succeeds",
        )
    }
    deployer.awaitServing(vespaUrl, poll = Duration.ofMillis(250))
    return null
}

/**
 * How long an unchanged package waits for services still converging. Shorter than a deploy's own
 * wait: what it covers is the tail of a deploy an earlier boot already waited on.
 */
private val UNCHANGED_CONVERGE_WAIT = Duration.ofSeconds(30)

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
    poll: Duration = Duration.ofMillis(250),
    isServing: (String) -> Boolean = SchemaDeployer(VespaEventStore.deriveConfigUrl(vespaUrl))::isServing,
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
