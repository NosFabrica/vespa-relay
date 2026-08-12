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
    // These two dial the public internet, so they stay off unless asked for by
    // name — `RealRelayDrainProbe` and `DeleteMissingBandProbe`.
    System.getProperty("realRelayProbe")?.let { systemProperty("realRelayProbe", it) }
    System.getProperty("deleteMissingBandProbe")?.let { systemProperty("deleteMissingBandProbe", it) }
    // Same forwarding, same reason. `SyncBandsProdScaleProbe` builds a ~14MB
    // corpus and hands the two files to the relay-side probe through
    // `prodScaleDir`, so both properties have to cross into the fork.
    System.getProperty("prodScaleProbe")?.let { systemProperty("prodScaleProbe", it) }
    System.getProperty("prodScaleDir")?.let { systemProperty("prodScaleDir", it) }
}
