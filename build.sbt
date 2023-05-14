ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "ergasia_4"
  )

val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
// libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion