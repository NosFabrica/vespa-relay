plugins {
    id("vespa-relay.kotlin-module")
    application
}

dependencies {
    // implementation, not api: this module is a process, nothing compiles against it. Never :sync:
    // the mirror reads the verdicts from the store, and the monitor knows no mirror type.
    implementation(project(":common"))
    implementation(project(":peers"))
    implementation(project(":web"))
    implementation(libs.quartz)
    implementation(libs.vespa.eventstore.store)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
    // RelayDocument fetches NIP-11 over http. Declared rather than taken from :peers'
    // api, so the graph says where the dependency is used.
    implementation(libs.okhttp)
    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.kotlinx.serialization.json)
    testImplementation(libs.okhttp)
}

application {
    // Netty loads its native libraries itself; JDK 24+ warns at every boot unless native access is granted.
    applicationDefaultJvmArgs = listOf("--enable-native-access=ALL-UNNAMED")
    mainClass = "com.nosfabrica.vespa.relay.MonitorMainKt"
    applicationName = "vespa-monitor"
}

tasks.test {
    // ProbeSwitchesAreForwardedTest checks this list; it runs under `:common:archTest`.
    forwardProbeSwitches(
        "authGatedProbe",
        "authGatedUrl",
        "authRefusalCensus",
        "authRefusalProbe",
        "authRefusalUrls",
        "complianceProbe",
        "complianceUrls",
        "liveBudget",
        "liveBudgetUrls",
        "liveConsistency",
        "liveConsistencyUrls",
        "liveFoldGroups",
        "liveFoldProbe",
        "onionFoldProbe",
        "onionFoldSocks",
        "onionFoldUrls",
        "profileCold",
        "profileConcurrency",
        "profileDirect",
        "profileHosts",
        "profileOut",
        "profileProbe",
        "profileRecords",
        "profileSeed",
        "replayOut",
        "replayProbe",
        "replayUrls",
        "seedVerdicts",
        "seedVerdictsCount",
        "seedVerdictsLegacy",
        "seedVerdictsNsec",
        "seedVerdictsUrl",
        "selfConsistency",
        "selfConsistencyUrls",
        "shapesProbe",
        "shapesUrls",
        "writeOrderReplayUrls",
        "writeOrderTsv",
    )
}
