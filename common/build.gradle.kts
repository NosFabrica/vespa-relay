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
    // api: RelayIdentity returns quartz signer types, so consumers see quartz
    // in this module's signatures. The store is implementation-only —
    // SchemaDeploy's surface is all Strings, the SchemaDeployer stays local,
    // and both apps declare the store themselves for their own use.
    api(libs.quartz)
    implementation(libs.vespa.eventstore.store)
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
