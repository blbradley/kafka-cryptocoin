name := "kafka-cryptocoin"
scalaVersion := "2.11.7"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.1"
libraryDependencies += "com.xeiam.xchange" % "xchange-core" % "3.1.0"
libraryDependencies += "com.xeiam.xchange" % "xchange-bitstamp" % "3.1.0"
libraryDependencies += "com.xeiam.xchange" % "xchange-bitfinex" % "3.1.0"

import ReleaseTransformations._
// default release process with publishing removed
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  setNextVersion,
  commitNextVersion,
  pushChanges
)
