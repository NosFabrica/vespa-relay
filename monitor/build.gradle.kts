plugins {
    id("vespa-relay.kotlin-module")
}

dependencies {
    // Never :sync. What the monitor needs from the mirror arrives through MonitorEngine's constructor.
    // api: MonitorEngine's constructor and RelayVerdictRecord are written in this module's types.
    api(project(":peers"))
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
    // RelayDocument fetches NIP-11 over http. Declared rather than taken from :peers'
    // api, so the graph says where the dependency is used.
    implementation(libs.okhttp)
    // Tests only: this plane produces a document and renders nothing, so :web is not on
    // its compile classpath. MonitorStatusTest serves the shared page over the document.
    testImplementation(project(":web"))
    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.kotlinx.serialization.json)
    testImplementation(libs.okhttp)
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
