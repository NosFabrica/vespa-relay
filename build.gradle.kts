import com.diffplug.gradle.spotless.SpotlessExtensionPredeclare
import java.io.File

plugins {
    // Loaded once here; per-module loading gives each subproject its own classloader copy.
    alias(libs.plugins.kotlin.jvm) apply false
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

// Installs .git-hooks into .git/hooks, resolving the worktree layout where .git is a file.
val installGitHook =
    tasks.register<Copy>("installGitHook") {
        val dotGit = File(rootProject.rootDir, ".git")
        val hooksDir: File =
            if (dotGit.isFile) {
                val gitDir = File(dotGit.readText().trim().replace("gitdir: ", ""))
                File(gitDir, "hooks")
            } else {
                File(dotGit, "hooks")
            }
        from(File(rootProject.rootDir, ".git-hooks/pre-commit"))
        from(File(rootProject.rootDir, ".git-hooks/pre-push"))
        into(hooksDir)
        filePermissions { unix("0777") }
    }

subprojects {
    tasks.matching { it.name == "compileKotlin" }.configureEach {
        dependsOn(installGitHook)
    }
}
