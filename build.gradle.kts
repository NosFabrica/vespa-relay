import com.diffplug.gradle.spotless.SpotlessExtensionPredeclare
import java.io.File

plugins {
    // The Kotlin plugin arrives through build-logic's classpath, shared by every module.
    alias(libs.plugins.diffplug.spotless)
}

val ktlintVersion = libs.versions.ktlint.get()

allprojects {
    group = "com.nosfabrica.vespa.relay"

    apply(plugin = "com.diffplug.spotless")

    if (project === rootProject) {
        spotless { predeclareDeps() }
        configure<SpotlessExtensionPredeclare> {
            kotlin { ktlint(ktlintVersion) }
            kotlinGradle { ktlint(ktlintVersion) }
        }
    } else {
        spotless {
            kotlin {
                target("src/**/*.kt")
                ktlint(ktlintVersion)
                licenseHeaderFile(
                    rootProject.file(".spotless/copyright.kt"),
                    "@file:|package|import|class|object|sealed|open|interface|abstract ",
                )
            }
            kotlinGradle {
                target("*.gradle.kts")
                ktlint(ktlintVersion)
            }
        }
    }
}

// Installs .git-hooks where git reads them: the common dir's hooks/, which a linked worktree shares.
val installGitHook =
    tasks.register<Copy>("installGitHook") {
        val root = rootProject.rootDir
        val dotGit = File(root, ".git")
        // CI and image builds have no developer to protect, and a Docker context has no .git at all.
        onlyIf { System.getenv("CI") == null && dotGit.exists() }
        val hooksDir =
            providers.provider {
                val gitDir =
                    if (dotGit.isFile) {
                        root.resolve(dotGit.readText().trim().removePrefix("gitdir:").trim())
                    } else {
                        dotGit
                    }
                val common = gitDir.resolve("commondir")
                val commonDir = if (common.isFile) gitDir.resolve(common.readText().trim()) else gitDir
                commonDir.resolve("hooks").canonicalFile
            }
        from(File(root, ".git-hooks/pre-commit"))
        from(File(root, ".git-hooks/pre-push"))
        into(hooksDir)
        filePermissions { unix("0755") }
    }

subprojects {
    tasks.matching { it.name == "compileKotlin" }.configureEach {
        dependsOn(installGitHook)
    }
}
