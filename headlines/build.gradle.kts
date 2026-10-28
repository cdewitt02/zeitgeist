plugins {
    id("headline-pulse.java-conventions")
    application
}

dependencies {
    implementation(project(":common"))
    implementation(libs.kafka.clients)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.yaml)
    implementation(libs.rome)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback.classic)
}

application {
    mainClass = "headlinepulse.headlines.Main"
    applicationDefaultJvmArgs = listOf("-Xmx256m")
}
