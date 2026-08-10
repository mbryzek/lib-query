name := "lib-query"

version := "0.1.33"

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

ThisBuild / scalaVersion := "3.8.4"

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
    // ISS-356: `-oDF` prints a per-test duration to stdout; `-u` writes one JUnit XML file per
    // suite, which is the only correct per-suite attribution this build produces. stdout carries
    // no per-line suite marker, so once suites run in parallel a reader (devops/bin/test-timings)
    // can only attribute a duration to whichever "[info] ClassName:" header was printed most
    // recently by ANY thread -- which on a real run named the wrong suite for every test it
    // reported as slow.
    testOptions ++= Seq(
      Tests.Argument("-oDF"),
      Tests.Argument(TestFrameworks.ScalaTest, "-u", (target.value / "test-reports").getAbsolutePath),
    ),
    scalacOptions ++= allScalacOptions,
    libraryDependencies ++= Seq(
      "org.playframework.anorm" %% "anorm-postgres" % "3.1.0",
      "org.typelevel" %% "cats-core" % "2.13.0",
      "joda-time" % "joda-time" % "2.14.3",
      "org.scalatestplus.play" %% "scalatestplus-play" % "7.0.2" % Test,
    ),
  )
