plugins {
    alias(libs.plugins.kotlin.jvm)
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
    // api: RelayIdentity returns quartz signer types and SchemaDeploy wraps the
    // store's deployer — consumers see both in these declarations' signatures.
    api(libs.quartz)
    api(libs.vespa.eventstore.store)
    // ParseAudit writes its report as JSON.
    implementation(libs.kotlinx.serialization.json)
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.serialization.json)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
