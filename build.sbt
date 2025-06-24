version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.14"

val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "CaseStudyLastFm"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion % "test",
  "org.apache.logging.log4j" % "log4j-api" % "2.23.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.23.1",
  "com.holdenkarau" %% "spark-testing-base" % "3.5.1_1.5.3" % "test",
  "com.typesafe" % "config" % "1.4.3"
)

Test / fork := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")