plugins {
    alias(libs.plugins.kotlin.jvm)
}

// Same rationale as every other module: JitPack pins are commit hashes and
// Gradle resolves a version conflict lexicographically, so the quartz pin must
// be forced wherever it is resolved or a transitive hash can silently win.
configurations.all {
    resolutionStrategy {
        force(libs.quartz)
    }
}

dependencies {
    // fmtDuration, the quartz log floor, and ServingPressure — which ingest
    // yields to. :peers sits ABOVE :common and must stay there: :common is what
    // the SERVING relay also reads, and nothing here is.
    api(project(":common"))
    // Every type this module hands out is written in quartz's — a relay url, a
    // filter, an event, a signer — so a consumer compiles against quartz
    // whether it declares it or not.
    api(libs.quartz)
    api(libs.vespa.eventstore.store)
    implementation(libs.kotlinx.coroutines)
    // RouterConfig is parsed out of the strfry-style `streams { }` HOCON;
    // RelayVerdictRecord and the refusal filters serialise their own state.
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.typesafe.config)
    // OkHttp drives the outbound websockets quartz's NostrClient dials, and the
    // Tor transport is an OkHttp client with a SOCKS proxy.
    api(libs.okhttp)
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.serialization.json)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
    // No probe forwards here: nothing in this module reads a system property.
    // Add one the day a probe does — a property that is not forwarded reaches
    // the Gradle DAEMON and never the forked test JVM, so the probe skips
    // itself with its own "[skip]" line and reads exactly like one nobody asked
    // for. `ProbeSwitchesAreForwardedTest` in :sync pins that across every
    // module, so this comment is a note rather than the guard.
}
