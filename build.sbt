ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.13"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "com.github.mrpowers" %% "spark-daria" % "0.39.0"
  //"org.apache.spark" %% "spark-hive" % "3.1.2"
)

lazy val root = (project in file("."))
  .settings(
    name := "project2"
  )
