name := "SparkSbtScala"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.3.2"

idePackagePrefix := Some("org.muks.example")

import sbt.Keys.libraryDependencies

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)