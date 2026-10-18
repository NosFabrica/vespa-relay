plugins {
    id("vespa-relay.kotlin-module")
}

dependencies {
    api(project(":common"))
    // api: this module's signatures are written in quartz's types.
    api(libs.quartz)
    api(libs.vespa.eventstore.store)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.typesafe.config)
    // api: PeerClient.httpFor and TorTransport.clientFor hand out an OkHttpClient.
    api(libs.okhttp)
    testImplementation(libs.kotlinx.serialization.json)
}

tasks.test {
    // ProbeSwitchesAreForwardedTest checks this list; it runs under `:common:archTest`.
    forwardProbeSwitches(
        "liveListKind",
        "liveListProbe",
        "liveListRelay",
        "liveListVespa",
    )
}
