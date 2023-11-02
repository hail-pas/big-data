plugins {
    id("java")
    id("application")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(mapOf("path" to ":common")))
    compileOnly("org.apache.flink:flink-connector-jdbc:3.1.1-1.17")
    // https://mvnrepository.com/artifact/com.aliyun.datahub/aliyun-sdk-datahub
    implementation("com.aliyun.datahub:aliyun-sdk-datahub:2.25.1")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

application {
    mainClass = "cn.burnish.bigdata.traceroute.Main"
}

//tasks.test {
//    useJUnitPlatform()
//}