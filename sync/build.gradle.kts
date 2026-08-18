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
    // These dial the public internet, so they stay off unless asked for
    // by name — `RealRelayDrainProbe` and `PagingCursorProbe`.
    System.getProperty("realRelayProbe")?.let { systemProperty("realRelayProbe", it) }
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
    // A whole stability pass over real relays — the only test that can prove
    // `Silence` classifies what OkHttp and the JDK actually say, rather than
    // what a fake page says. Same forwarding, same trap.
    System.getProperty("liveConsistency")?.let { systemProperty("liveConsistency", it) }
    System.getProperty("liveConsistencyUrls")?.let { systemProperty("liveConsistencyUrls", it) }
    // The two planes end to end against real relays: fitness verdicts onto
    // records, the roster read back, a small VisitPool run on it.
    System.getProperty("visitPoolProbe")?.let { systemProperty("visitPoolProbe", it) }
    // Seeds one synthetic 10040 into a LOCAL relay so the `certified` gate
    // and the monitor's 10040 source can be watched live. Same trap.
    System.getProperty("seed10040")?.let { systemProperty("seed10040", it) }
    // The verdicts-panel seed: a monitor corpus in a LOCAL relay, so the stats
    // page's one protocol-speaking panel can be driven against a real store.
    System.getProperty("seedVerdicts")?.let { systemProperty("seedVerdicts", it) }
    System.getProperty("seedVerdictsUrl")?.let { systemProperty("seedVerdictsUrl", it) }
    System.getProperty("seedVerdictsNsec")?.let { systemProperty("seedVerdictsNsec", it) }
    System.getProperty("seedVerdictsCount")?.let { systemProperty("seedVerdictsCount", it) }
    System.getProperty("seedVerdictsLegacy")?.let { systemProperty("seedVerdictsLegacy", it) }
    System.getProperty("seed10040Url")?.let { systemProperty("seed10040Url", it) }
    // Stages the enforce-mode retraction scenario — a provider on a real
    // relay, a phantom score only in our store. Same trap.
    System.getProperty("enforceProbe")?.let { systemProperty("enforceProbe", it) }
    System.getProperty("enforceProviderRelay")?.let { systemProperty("enforceProviderRelay", it) }
    System.getProperty("enforceLocalRelay")?.let { systemProperty("enforceLocalRelay", it) }
}
