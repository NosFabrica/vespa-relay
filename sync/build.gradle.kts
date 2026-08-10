plugins {
    alias(libs.plugins.kotlin.jvm)
    // This module IS the mirror process: SyncMain is the entrypoint
    // (`./gradlew :sync:run`, or the `installDist` tree the Docker image runs).
    application
}

// Same rationale as :relay — JitPack pins are commit hashes and Gradle resolves
// version conflicts lexicographically, so the quartz pin must be forced in every
// module that resolves it or a transitive hash can silently win.
configurations.all {
    resolutionStrategy {
        force(libs.quartz)
    }
}

dependencies {
    // SyncEngine's constructor takes the audit and pressure types from :common,
    // and its tests build quartz filters — api keeps those visible downstream.
    api(project(":common"))
    api(libs.quartz)
    api(libs.vespa.eventstore.store)
    implementation(libs.kotlinx.coroutines)
    // SyncBands persists the per-(relay, filter) cursor map as JSON.
    implementation(libs.kotlinx.serialization.json)
    // OkHttp drives the outbound websockets quartz's NostrClient dials;
    // typesafe-config parses the strfry-style `streams { }` HOCON.
    implementation(libs.okhttp)
    implementation(libs.typesafe.config)
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.serialization.json)
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass = "com.nosfabrica.vespa.relay.router.SyncMainKt"
    applicationName = "vespa-sync"
}

tasks.test {
    useJUnitPlatform()
    // Forwarded, not inherited: a system property on the Gradle command line
    // reaches the DAEMON, and the tests run in a forked JVM that never sees it.
    // Every gate here is a PROBE — it dials the public internet, drives a local
    // relay, or builds a corpus in the hundreds of thousands — so each one stays
    // off unless asked for by name. The heap goes up only when one is asked for:
    // a probe's id set is millions of entries, and the default fork cannot hold it.
    val probeGates =
        listOf(
            "realRelayProbe",
            "negProbe",
            "strfryProbe",
            "strfryUrl",
            "strfryDir",
            "strfryCount",
            "issue91Probe",
            "issue91Relay",
            "issue91Author",
            "issue91Page",
        )
    probeGates.forEach { gate ->
        System.getProperty(gate)?.let {
            systemProperty(gate, it)
            maxHeapSize = "3g"
        }
    }
}
