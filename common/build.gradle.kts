plugins {
    id("headline-pulse.java-conventions")
    `java-library`
}

dependencies {
    implementation(libs.kafka.clients)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.jsr310)
    implementation(libs.jackson.yaml)
    // Parses topics.yaml, which uses YAML aliases that Jackson's YAML reader can't expand.
    implementation(libs.snakeyaml)
}
