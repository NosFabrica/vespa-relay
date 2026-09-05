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
    // Deliberately no dependency on :common; that would drag Ktor into every module.
    // api: the routes this module hands out are written in Ktor's types.
    api(libs.ktor.server.core)
    // NIP-98 http auth, for the admin gate on the pulse page. Quartz rather
    // than :common because the dependency can only run one way and Ktor must
    // not reach :common — and because verifying an Authorization header is
    // this module's business, beside the routes it protects.
    implementation(libs.quartz)
    api(libs.ktor.server.netty)
    implementation(libs.ktor.server.cors)
    implementation(libs.ktor.server.compression)
    api(libs.kotlinx.serialization.json)
    testImplementation(kotlin("test"))
    testImplementation(libs.ktor.server.test.host)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}

/**
 * The browser code's own suite, in plain node; `src/test/js/run.mjs` lists the
 * suites. Fails rather than skips when node is missing, so the build cannot go
 * green over 14,000 untested lines.
 */
val jsTest =
    tasks.register<Exec>("jsTest") {
        group = "verification"
        description = "The web UI's own tests, in plain node."
        workingDir = projectDir
        commandLine("node", "src/test/js/run.mjs")
        inputs.dir(layout.projectDirectory.dir("src/test/js"))
        inputs.dir(layout.projectDirectory.dir("src/main/resources"))
        outputs.upToDateWhen { false }
    }

// On `check`, so `build`, the pre-push hook and CI all run it.
tasks.named("check") { dependsOn(jsTest) }
