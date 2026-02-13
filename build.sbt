name := "lib-query"

organization := "com.bryzek"

version := "0.1.15"

ThisBuild / javacOptions ++= Seq("-source", "17", "-target", "17")

ThisBuild / organization := "com.bryzek"
ThisBuild / homepage := Some(url("https://github.com/mbryzek/lib-query"))
ThisBuild / licenses := Seq("MIT" -> url("https://github.com/mbryzek/lib-query/blob/main/LICENSE"))
ThisBuild / developers := List(
  Developer("mbryzek", "Michael Bryzek", "mbryzek@alum.mit.edu", url("https://github.com/mbryzek"))
)
ThisBuild / scmInfo := Some(
  ScmInfo(url("https://github.com/mbryzek/lib-query"), "scm:git@github.com:mbryzek/lib-query.git")
)

ThisBuild / publishTo := sonatypePublishToBundle.value
ThisBuild / sonatypeCredentialHost := "central.sonatype.com"
ThisBuild / sonatypeRepository := "https://central.sonatype.com/api/v1/publisher"
ThisBuild / publishMavenStyle := true

ThisBuild / scalaVersion := "3.8.1"

lazy val allScalacOptions = Seq(
  "-Werror",
  "-Wunused:locals",
  "-Wunused:params",
  "-Wimplausible-patterns",
  "-Wunused:imports",
  "-Wunused:privates",
  "-Wunused:linted"
)

lazy val root = project
  .in(file("."))
  .settings(
    resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases",
    scalafmtOnCompile := true,
    Compile / packageDoc / mappings := Seq(),
    Compile / packageDoc / publishArtifact := true,
    testOptions += Tests.Argument("-oDF"),
    scalacOptions ++= allScalacOptions,
    libraryDependencies ++= Seq(
      "org.playframework.anorm" %% "anorm-postgres" % "2.7.0",
      "org.typelevel" %% "cats-core" % "2.13.0",
      "org.scalatestplus.play" %% "scalatestplus-play" % "7.0.1" % Test,
    ),
  )
