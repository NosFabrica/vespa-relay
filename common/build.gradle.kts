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
    // in this module's signatures. The store went the same way when
    // STORE_WRITERS arrived — the writer topology is a fact about this
    // deployment that BOTH processes must hand to `open()`, and its type is the
    // store's. SchemaDeploy is still all Strings and the SchemaDeployer stays
    // local; both apps also declare the store themselves for their own use.
    api(libs.quartz)
    api(libs.vespa.eventstore.store)
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
