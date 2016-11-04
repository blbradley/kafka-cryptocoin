name := "kafka-cryptocoin"
organization := "coinsmith"
scalaVersion := "2.11.8"

val akkaVersion = "2.4.12"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.21"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-http-experimental" % "2.4.11"
libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.1-cp1"
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "3.0.1" classifier ""
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.5.0"
libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "1.6.2"
libraryDependencies += "org.glassfish.tyrus.bundles" % "tyrus-standalone-client-jdk" % "1.13"
libraryDependencies += "org.glassfish.tyrus" % "tyrus-container-grizzly-client" % "1.13"
libraryDependencies += "com.pusher" % "pusher-java-client" % "1.2.1"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.glassfish.tyrus.tests" % "tyrus-test-tools" % "1.13" % "test",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test"
)

resolvers ++= Seq(
  "clojars.org" at "http://clojars.org/repo",
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
