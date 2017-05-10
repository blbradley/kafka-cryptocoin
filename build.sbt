name := "kafka-cryptocoin"
organization := "coinsmith"
scalaVersion := "2.12.1"

val akkaVersion = "2.5.0"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-http" % "10.0.5"
libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.2.1-cp1"
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "3.2.1" classifier ""
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.5.1"
libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "1.6.4"
libraryDependencies += "com.pusher" % "pusher-java-client" % "1.5.0"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test"
)

resolvers ++= Seq(
  "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
)

enablePlugins(GitVersioning)
enablePlugins(DockerPlugin)

import ReleaseTransformations._
// default release process with publishing, tagging, and pushing removed
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  setNextVersion,
  commitNextVersion
)
