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

import com.nosfabrica.vespa.eventstore.EngineTuning
import com.nosfabrica.vespa.eventstore.SchemaDeploy
import com.nosfabrica.vespa.eventstore.SchemaDeployException
import com.nosfabrica.vespa.eventstore.SchemaDeployment
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.relay.util.BootTask

/**
 * The relay's every-boot deploy: the store's [SchemaDeploy.IF_CHANGED], which compares, deploys on a
 * difference and waits for every service. A failed deploy is survived while Vespa still serves an
 * application, and rethrown on a fresh one. Returns the convergence wait an unchanged package may
 * still owe, started beside the caller's next steps, to be awaited before anything writes.
 */
fun deployBundledSchema(
    vespaUrl: String,
    configUrl: String,
    tuning: EngineTuning,
    deploy: () -> SchemaDeployment = { VespaEventStore.deploySchema(vespaUrl, configUrl, SchemaDeploy.IF_CHANGED, tuning) },
): BootTask<Boolean>? {
    val done =
        try {
            deploy()
        } catch (e: SchemaDeployException) {
            if (!e.serving) throw e
            // "schema:", the prefix every line about the application package carries.
            System.err.println(
                "schema: deploy to $configUrl failed (${e.message?.take(200)}); " +
                    "serving on the schema Vespa already has — writes carrying fields it lacks will be rejected until a deploy succeeds",
            )
            return null
        }
    if (!done.deployed) {
        System.err.println("schema: Vespa already runs this build's application package; not redeploying")
        return BootTask("schema convergence") { done.awaitConvergence() }
    }
    val generation = done.generation ?: "(the one wanted)"
    if (done.converged == false) {
        System.err.println("schema: deployed to $configUrl, but not every Vespa service runs config generation $generation yet")
    } else {
        System.err.println("schema: deployed to $configUrl; every Vespa service runs config generation $generation")
    }
    return null
}
