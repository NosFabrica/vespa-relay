plugins {
    id("vespa-relay.kotlin-module")
    application
}

dependencies {
    // implementation, not api: this module is a process, nothing compiles against it.
    implementation(project(":common"))
    implementation(project(":web"))
    // The ingest pipeline: the relay's publishes take the path the mirror's downloads take.
    implementation(project(":peers"))
    implementation(libs.quartz)
    implementation(libs.vespa.eventstore.store)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.websockets)
    implementation(libs.ktor.server.cors)
    implementation(libs.ktor.server.compression)
    implementation(libs.ktor.server.netty)
    testImplementation(libs.kotlinx.serialization.json)
    testImplementation(libs.ktor.server.test.host)
}

application {
    // Netty loads its native libraries itself; JDK 24+ warns at every boot unless native access is granted.
    applicationDefaultJvmArgs = listOf("--enable-native-access=ALL-UNNAMED")
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
    // ComposePassesEnvTest reads these two files, which live outside every
    // source set. Undeclared, Gradle calls this task up to date after either
    // changes and the check silently stops running — which is the same class of
    // silence the test itself exists to catch.
    inputs
        .files(rootProject.file(".env.example"), rootProject.file("docker-compose.yml"))
        .withPropertyName("deploymentDescriptors")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    // ProbeSwitchesAreForwardedTest checks this list; it runs under `:common:archTest`.
    forwardProbeSwitches(
        "itCorpus",
        "itRelay",
        "itVespa",
        "prodScaleDir",
        "prodScaleProbe",
        "searchExpansionBench",
    )
}
