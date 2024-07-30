
import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "spark-consumer",
    idePackagePrefix := Some("org.example.application"),
    assembly / assemblyJarName := "data-consumer-1.0.jar",
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("org.apache.spark.sql.sources.**" -> "my.project.shaded.apache.spark.@1").inAll
    ),
    assembly / assemblyMergeStrategy := {
      case "reference.conf" => MergeStrategy.concat
      case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql" % "3.4.1",
  "org.apache.spark" %% "spark-streaming" % "3.4.1",
  "org.opensearch.client" % "opensearch-spark-30_2.12" % "1.2.0",
  "com.typesafe" % "config" % "1.4.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.1",
  "org.apache.kafka" % "kafka-clients" % "3.4.1",
  "org.opensearch.client" % "opensearch-rest-high-level-client" % "1.2.0"

)

