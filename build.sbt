// Databricks-friendly Spark project (dependencies are mostly Provided)

ThisBuild / organization := "gov.va.occ"
ThisBuild / version := "0.1.0-SNAPSHOT"
// DBR 16.4 (Scala 2.13 image) ships with Scala 2.13.10
ThisBuild / scalaVersion := "2.13.10"

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-encoding",
  "utf8"
)

// DBR 16.4 LTS is powered by Apache Spark 3.5.2
lazy val sparkVersion = "3.5.2"
// Open-source Delta version that matches Spark 3.5.x. For Databricks Runtime,
// you typically keep this as Provided and let the cluster supply Delta.
// DBR 16.4 LTS includes Delta Lake 3.3.1
lazy val deltaVersion = "3.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "CDWPipe",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
      "io.delta"         %% "delta-spark" % deltaVersion % Provided,

      // Test
      "org.scalatest"    %% "scalatest"   % "3.2.19" % Test
    ),

    // Assembly (fat JAR) for Databricks job clusters
    Compile / mainClass := Some("gov.va.occ.CDWPipe.Main"),
    assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf"              => MergeStrategy.concat
      case x                             => (assembly / assemblyMergeStrategy).value(x)
    }
  )
