plugins {
    id("vespa-relay.kotlin-module")
    application
}

dependencies {
    // Standalone on purpose: a reader of one relay, nothing of ours. It never opens a store and
    // never writes; what it decides goes to a file. docs/keywatch.md.
    implementation(libs.quartz)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.okhttp)
}

application {
    mainClass = "com.nosfabrica.vespa.relay.KeyWatchMainKt"
    applicationName = "vespa-keywatch"
}
