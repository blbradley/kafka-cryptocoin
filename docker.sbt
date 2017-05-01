import java.util.Date

docker <<= docker.dependsOn(sbt.Keys.`package`.in(Compile, packageBin))

dockerfile in docker := {
  val jarFile = artifactPath.in(Compile, packageBin).value
  val classpath = (managedClasspath in Compile).value
  val mainclass = mainClass.in(Compile, packageBin).value.getOrElse(sys.error("Expected exactly one main class"))
  val jarTargetName = {
    val filename = jarFile.getName
    val (name, ext) = filename.splitAt(filename lastIndexOf ".")
    git.gitCurrentBranch.value match {
      case _ if git.gitUncommittedChanges.value =>
        git.defaultFormatDateVersion(Some(name), new Date) + ext
      case "master" => version.value
      case _ => name + "-" + git.gitHeadCommit.value.get.take(8) + ext
    }
  }
  val jarTarget = s"/app/${jarTargetName}"
  val classpathString = classpath.files.map("/app/" + _.getName)
    .mkString(":") + ":" + jarTarget
  new Dockerfile {
    from("openjdk")
    add(classpath.files, "/app/")
    add(jarFile, jarTarget)
    entryPointShell("exec", "java", "$JAVA_OPTS", "-cp", classpathString, mainclass)
  }
}

val dockerTags = settingKey[Seq[Option[String]]]("Docker tags to build")
dockerTags := {
  val branchTag = git.gitCurrentBranch.value match {
    case "master" => "latest"
    case "develop" => "develop"
    case _ => version.value
  }

  git.gitCurrentTags.value.map { Option(_) } :+ Option(branchTag)
}

imageNames in docker := dockerTags.value.map { t =>
  ImageName(
    namespace = Some(organization.value),
    repository = name.value,
    tag = t
  )
}
