plugins {
    id("headline-pulse.java-conventions")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.kafka.clients)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback.classic)
}

tasks.register<JavaExec>("createTopics") {
    group = "application"
    description = "Creates the Kafka topics and brings their configs up to date. Safe to rerun."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass = "headlinepulse.tools.CreateTopics"
}

tasks.register<JavaExec>("checkGaps") {
    group = "verification"
    description = "Reports gaps and duplicates in bsky.raw between two times: --args=\"<from> <to>\""
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass = "headlinepulse.tools.CheckGaps"
}
