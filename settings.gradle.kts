plugins {
    // Auto-provisiona el JDK 21 del toolchain (build.gradle.kts) cuando la máquina que corre el
    // build no lo tiene instalado; no cambia con qué JDK se corre Gradle mismo.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "payments-worker"
