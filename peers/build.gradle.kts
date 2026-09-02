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
    // Forwarded, not inherited: a system property on the Gradle command line
    // reaches the DAEMON, and the tests run in a forked JVM that never sees it.
    // A missing forward does not fail — the probe skips itself with its own
    // "[skip]" line, which reads exactly like a probe nobody asked for, and the
    // build goes green over a measurement that never ran. That happened here on
    // the first run of `RelayListLiveProbe`, with this file's own comment
    // predicting it. `ProbeSwitchesAreForwardedTest` in :sync is the guard.
    System.getProperty("liveListKind")?.let { systemProperty("liveListKind", it) }
    System.getProperty("liveListProbe")?.let { systemProperty("liveListProbe", it) }
    System.getProperty("liveListRelay")?.let { systemProperty("liveListRelay", it) }
    System.getProperty("liveListVespa")?.let { systemProperty("liveListVespa", it) }
    //
    // The benches here DO need a bigger heap than the 512m default — a
    // six-figure corpus of signed events, plus a signer per author, is a few
    // GB — and unit tests must not pay for that on every CI box. So it is
    // opt-in and unset by default: `-PtestHeap=6g` alongside BENCH_VESPA_URL.
    providers.gradleProperty("testHeap").orNull?.let { maxHeapSize = it }
}
