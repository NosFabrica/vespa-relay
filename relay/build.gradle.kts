plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

// JitPack pins are commit hashes and Gradle picks the lexicographically higher one, so force ours.
configurations.all {
    resolutionStrategy {
        force(libs.quartz)
    }
}

dependencies {
    api(project(":common"))
    api(project(":web"))
    api(libs.quartz)
    api(libs.vespa.eventstore.store)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.websockets)
    implementation(libs.ktor.server.cors)
    implementation(libs.ktor.server.compression)
    implementation(libs.ktor.server.netty)
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.serialization.json)
    testImplementation(libs.ktor.server.test.host)
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass = "com.nosfabrica.vespa.relay.RelayMainKt"
    applicationName = "vespa-relay"
}

// The NIP-11 `version` field reads this generated resource.
val generatedVersionDir = layout.buildDirectory.dir("generated/version")
val generateVersionProperties =
    tasks.register("generateVersionProperties") {
        val version = libs.versions.app.get()
        val outFile = generatedVersionDir.map { it.file("relay-version.properties") }
        inputs.property("version", version)
        outputs.file(outFile)
        doLast {
            outFile.get().asFile.apply {
                parentFile.mkdirs()
                writeText("version=$version\n")
            }
        }
    }

sourceSets.main {
    resources.srcDir(generatedVersionDir)
}

tasks.named("processResources") {
    dependsOn(generateVersionProperties)
}

tasks.test {
    useJUnitPlatform()
    // ComposePassesEnvTest reads these two files, which live outside every
    // source set. Undeclared, Gradle calls this task up to date after either
    // changes and the check silently stops running — which is the same class of
    // silence the test itself exists to catch.
    inputs
        .files(rootProject.file(".env.example"), rootProject.file("docker-compose.yml"))
        .withPropertyName("deploymentDescriptors")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    // Every property a probe reads must be forwarded here: the forked test JVM does not
    // inherit them, and a probe missing its switch skips itself silently.
    System.getProperty("prodScaleProbe")?.let { systemProperty("prodScaleProbe", it) }
    System.getProperty("prodScaleDir")?.let { systemProperty("prodScaleDir", it) }
    System.getProperty("searchExpansionBench")?.let { systemProperty("searchExpansionBench", it) }
    System.getProperty("itVespa")?.let { systemProperty("itVespa", it) }
    System.getProperty("itCorpus")?.let { systemProperty("itCorpus", it) }
}
