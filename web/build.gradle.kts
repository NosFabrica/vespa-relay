plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    // Ktor and nothing else. This module deliberately does NOT depend on
    // :common: the layering rule is that :common holds what the SERVING relay
    // reads, and a page server has no business in that set — the dependency
    // would drag Ktor and Netty into the one module every other module sees.
    //
    // api, not implementation: every route this module hands out is written in
    // Ktor's own types (`Route`, `ApplicationCall`, `ContentType`), so a
    // consumer that mounts one compiles against Ktor whether it declares it or
    // not.
    api(libs.ktor.server.core)
    api(libs.ktor.server.netty)
    implementation(libs.ktor.server.cors)
    implementation(libs.ktor.server.compression)
    // StatsSnapshot merges two writers' halves of one JSON document.
    api(libs.kotlinx.serialization.json)
    testImplementation(kotlin("test"))
    // The asset and page routes are asserted in-process, over the real
    // classpath, rather than eyeballed against a running container.
    testImplementation(libs.ktor.server.test.host)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}

/**
 * The browser code's own suite, in plain node — no framework, no dependencies,
 * matching the pages it tests. `src/test/js/run.mjs` documents what each one
 * covers.
 *
 * ## Why this is a Gradle task and not a command in the README
 *
 * It was a command in the README, and for that whole time it ran NOWHERE
 * automatically: not in `./gradlew build`, not in the pre-push hook, not in CI.
 * Twenty suites over ~14,000 lines of browser code, executed when somebody
 * remembered. The Kotlin half of this repo has never been on those terms and
 * there is no reason the JavaScript half should be — a wrong denominator in
 * `shared/sync.js` is as shippable as a wrong one in Kotlin, and five of them
 * once shipped behind a single card.
 *
 * ## Why it FAILS rather than skips without node
 *
 * A suite that quietly stands down when its runtime is absent is the same
 * silence this repo refuses everywhere else: the build goes green and reports
 * nothing about 14,000 lines. If node is missing the message says so and names
 * it, which is a five-minute fix, where a skipped suite is a bug found in
 * production.
 */
val jsTest =
    tasks.register<Exec>("jsTest") {
        group = "verification"
        description = "The web UI's own tests, in plain node."
        workingDir = projectDir
        commandLine("node", "src/test/js/run.mjs")
        // Re-run when either side moves: the suites themselves, and the files
        // they assert over.
        inputs.dir(layout.projectDirectory.dir("src/test/js"))
        inputs.dir(layout.projectDirectory.dir("src/main/resources"))
        outputs.upToDateWhen { false }
    }

// On `check` rather than on `test`: `test` is the JVM task and Gradle's own
// reporting is built around it, while `check` is the umbrella meaning "every
// verification this module has". `build` runs `check`, so the suite now runs
// wherever the Kotlin tests do — the pre-push hook and CI included.
tasks.named("check") { dependsOn(jsTest) }
