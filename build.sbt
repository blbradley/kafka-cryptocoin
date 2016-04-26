name := "kafka-cryptocoin"
organization := "coinsmith"
scalaVersion := "2.11.7"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.2"
libraryDependencies += "com.typesafe.akka" %% "akka-http-experimental" % "2.4.2"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.2"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.3.0"
libraryDependencies += "org.glassfish.tyrus.bundles" % "tyrus-standalone-client-jdk" % "1.12"
libraryDependencies += "org.glassfish.tyrus" % "tyrus-container-grizzly-client" % "1.12"

dependencyOverrides += "io.netty" % "netty-all" % "4.1.0.Beta8"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.4.2" % "test"
libraryDependencies += "net.manub" %% "scalatest-embedded-kafka" % "0.4.3" % "test"

parallelExecution in Test := false

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
