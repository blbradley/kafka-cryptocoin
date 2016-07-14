name := "kafka-cryptocoin"
organization := "coinsmith"
scalaVersion := "2.11.8"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.21"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.8"
libraryDependencies += "com.typesafe.akka" %% "akka-http-experimental" % "2.4.8"
libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % "2.4.8"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.4.0"
libraryDependencies += "org.glassfish.tyrus.bundles" % "tyrus-standalone-client-jdk" % "1.13"
libraryDependencies += "org.glassfish.tyrus" % "tyrus-container-grizzly-client" % "1.13"
libraryDependencies += "com.pusher" % "pusher-java-client" % "1.2.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.4.8" % "test"
libraryDependencies += "net.manub" %% "scalatest-embedded-kafka" % "0.7.0" % "test"

resolvers += "clojars.org" at "http://clojars.org/repo"

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
