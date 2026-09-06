plugins {
    alias(libs.plugins.kotlin.jvm)
}

// JitPack pins are commit hashes and Gradle picks the lexicographically higher one, so force ours.
configurations.all {
    resolutionStrategy {
        force(libs.quartz)
    }
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
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.serialization.json)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
    // Every property a probe reads must be forwarded here: the forked test JVM does not
    // inherit them, and a probe missing its switch skips itself silently.
    // ProbeSwitchesAreForwardedTest checks the list; it runs under `:common:archTest`.
    System.getProperty("liveListKind")?.let { systemProperty("liveListKind", it) }
    System.getProperty("liveListProbe")?.let { systemProperty("liveListProbe", it) }
    System.getProperty("liveListRelay")?.let { systemProperty("liveListRelay", it) }
    System.getProperty("liveListVespa")?.let { systemProperty("liveListVespa", it) }
    // The benches need a few GB of heap; unit tests must not. Opt in with -PtestHeap=6g.
    providers.gradleProperty("testHeap").orNull?.let { maxHeapSize = it }
}
