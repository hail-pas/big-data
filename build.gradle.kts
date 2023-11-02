plugins {
    id("java")
    idea
}

description = "Burnish Bigdata"

idea {
    module.isDownloadJavadoc = true
    module.isDownloadSources = true
}

allprojects {
    group = "cn.burnish"
    version = "1.0-SNAPSHOT"

}


subprojects {
    apply(plugin = "java")

    val flinkVersion = "1.17.0"
//    val log4jVersion = "2.17.2"

    java.sourceCompatibility = JavaVersion.VERSION_11
    java.targetCompatibility = JavaVersion.VERSION_11

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
    }

    repositories {
        mavenCentral()
        maven {
            url = uri("https://maven.aliyun.com/repository/public/")
            mavenContent {
                snapshotsOnly()
            }
        }
    }

    dependencies{
        compileOnly("org.projectlombok:lombok:1.18.24")
        annotationProcessor("org.projectlombok:lombok:1.18.22")
        implementation("org.apache.flink:flink-java:${flinkVersion}")
        implementation("org.apache.flink:flink-streaming-java:${flinkVersion}")
        implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")
    }
}