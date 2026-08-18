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
    // The peer vocabulary — the websocket client, Tor, discovery, the verdict
    // record, the config, the ingest queue.
    api(project(":peers"))
    // The other plane. This process HOSTS the monitor today: SyncMain builds
    // both engines over one PeerClient. The dependency is one-directional and
    // has to stay that way — :monitor knowing about :sync is what would make
    // the two inseparable again.
    api(project(":monitor"))
    // This process serves its own status page: what it has walked, what its
    // streams are doing, and the glossary for both. api because SyncStatus
    // takes a StatsSnapshot in its signature.
    api(project(":web"))
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
    // The status site's routes are asserted in-process, the same way the
    // relay's are, rather than eyeballed against a running container.
    testImplementation(libs.ktor.server.test.host)
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass = "com.nosfabrica.vespa.relay.SyncMainKt"
    applicationName = "vespa-sync"
}

tasks.test {
    useJUnitPlatform()
    // Forwarded, not inherited: a system property on the Gradle command line
    // reaches the DAEMON, and the tests run in a forked JVM that never sees it.
    // These dial the public internet or write to a relay, so they stay off unless asked for by name.
    //
    // EVERY property a probe in this module reads has to appear here. A missing
    // one does not fail — the probe skips itself with its own "[skip]" line, which
    // reads exactly like a probe that was never asked for.
    System.getProperty("enforceLocalRelay")?.let { systemProperty("enforceLocalRelay", it) }
    System.getProperty("enforceProbe")?.let { systemProperty("enforceProbe", it) }
    System.getProperty("enforceProviderRelay")?.let { systemProperty("enforceProviderRelay", it) }
    System.getProperty("prodScaleDir")?.let { systemProperty("prodScaleDir", it) }
    System.getProperty("prodScaleProbe")?.let { systemProperty("prodScaleProbe", it) }
    System.getProperty("realRelayProbe")?.let { systemProperty("realRelayProbe", it) }
    System.getProperty("seed10040")?.let { systemProperty("seed10040", it) }
    System.getProperty("seed10040Url")?.let { systemProperty("seed10040Url", it) }
    System.getProperty("visitPoolProbe")?.let { systemProperty("visitPoolProbe", it) }
}
