name := "lib-query"

organization := "com.mbryzek"

ThisBuild / scalaVersion := "3.3.3"

ThisBuild / javacOptions ++= Seq("-source", "17", "-target", "17")

version := "0.0.13"

lazy val allScalacOptions = Seq(
  "-feature",
  "-Xfatal-warnings",
)

lazy val root = project
  .in(file("."))
  .settings(
    resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases",
    scalafmtOnCompile := true,
    Compile / doc / sources := Seq.empty,
    Compile / packageDoc / publishArtifact := false,
    testOptions += Tests.Argument("-oDF"),
    scalacOptions ++= allScalacOptions,
    libraryDependencies ++= Seq(
      "org.playframework.anorm" %% "anorm-postgres" % "2.7.0",
      "org.typelevel" %% "cats-core" % "2.12.0",
      "org.scalatestplus.play" %% "scalatestplus-play" % "7.0.1" % Test,
    ),
  )
