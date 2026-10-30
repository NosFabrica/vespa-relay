plugins {
    id("vespa-relay.kotlin-module")
    application
}

dependencies {
    // implementation, not api: this module is a process, nothing compiles against it.
    implementation(project(":common"))
    implementation(project(":peers"))
    // This process hosts the monitor. The dependency must stay one-directional.
    implementation(project(":monitor"))
    implementation(project(":web"))
    implementation(libs.quartz)
    implementation(libs.vespa.eventstore.store)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.okhttp)
    implementation(libs.typesafe.config)
    testImplementation(libs.kotlinx.serialization.json)
    testImplementation(libs.ktor.server.test.host)
}

application {
    // Netty loads its native libraries itself; JDK 24+ warns at every boot unless native access is granted.
    applicationDefaultJvmArgs = listOf("--enable-native-access=ALL-UNNAMED")
    mainClass = "com.nosfabrica.vespa.relay.SyncMainKt"
    applicationName = "vespa-sync"
}

tasks.test {
    // ProbeSwitchesAreForwardedTest checks this list; it runs under `:common:archTest`.
    forwardProbeSwitches(
        "abortCensusMinutes",
        "abortCensusNsec",
        "abortCensusUrls",
        "enforceLocalRelay",
        "enforceProbe",
        "enforceProviderRelay",
        "pagesProbe",
        "pagesUrl",
        "prodScaleDir",
        "prodScaleProbe",
        "reachNoAuth",
        "reachNsec",
        "reachUrls",
        "realRelayProbe",
        "relayReachProbe",
        "seed10040",
        "seed10040Url",
        "unpageableLegs",
        "unpageableProbe",
        "visitPoolProbe",
    )
    forwardProbeEnv("WIDTH_RESCUE_VESPA")
}
