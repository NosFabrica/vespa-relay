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
    // These four dial the public internet, so they stay off unless asked for
    // by name — `RealRelayDrainProbe`, `DeleteMissingBandProbe`,
    // `InFlightReportProbe` and `PagingCursorProbe`.
    System.getProperty("realRelayProbe")?.let { systemProperty("realRelayProbe", it) }
    System.getProperty("deleteMissingBandProbe")?.let { systemProperty("deleteMissingBandProbe", it) }
    System.getProperty("inFlightProbe")?.let { systemProperty("inFlightProbe", it) }
    System.getProperty("pagingCursorProbe")?.let { systemProperty("pagingCursorProbe", it) }
    // Same forwarding, same reason. `SyncBandsProdScaleProbe` builds a ~14MB
    // corpus and hands the two files to the relay-side probe through
    // `prodScaleDir`, so both properties have to cross into the fork.
    System.getProperty("prodScaleProbe")?.let { systemProperty("prodScaleProbe", it) }
    System.getProperty("prodScaleDir")?.let { systemProperty("prodScaleDir", it) }
    // The fold's two live probes, and the same trap: without these lines the
    // switch reaches the daemon, the forked JVM never sees it, and the probe
    // prints its own `[skip]` line — which reads exactly like a probe that was
    // asked for and had nothing to say. `AliasFoldLiveProbe` runs a real pass
    // against real relays; `AliasFoldOnionProbe` needs a Tor SOCKS proxy.
    System.getProperty("liveFoldProbe")?.let { systemProperty("liveFoldProbe", it) }
    System.getProperty("liveFoldGroups")?.let { systemProperty("liveFoldGroups", it) }
    System.getProperty("onionFoldProbe")?.let { systemProperty("onionFoldProbe", it) }
    System.getProperty("onionFoldSocks")?.let { systemProperty("onionFoldSocks", it) }
    System.getProperty("onionFoldUrls")?.let { systemProperty("onionFoldUrls", it) }
    System.getProperty("selfConsistency")?.let { systemProperty("selfConsistency", it) }
    System.getProperty("selfConsistencyUrls")?.let { systemProperty("selfConsistencyUrls", it) }
    System.getProperty("authGatedProbe")?.let { systemProperty("authGatedProbe", it) }
    System.getProperty("authGatedUrl")?.let { systemProperty("authGatedUrl", it) }
    System.getProperty("authRefusalProbe")?.let { systemProperty("authRefusalProbe", it) }
    System.getProperty("authRefusalUrls")?.let { systemProperty("authRefusalUrls", it) }
    System.getProperty("authRefusalCensus")?.let { systemProperty("authRefusalCensus", it) }
}
