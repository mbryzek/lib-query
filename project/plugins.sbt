logLevel := Level.Warn

// Staying on sbt 1 until sbt-pgp publishes final sbt 2.x builds
// (only `_sbt2.0.0-M*` milestone artifacts exist; nothing for sbt 2.0.x final).
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.1")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")

// The security pins, the lib-* build settings and the test-health plugins (BryzekPins,
// BryzekLibSettings, CiTestListsPlugin, CiTestRerunPlugin), from scala-build: https://github.com/mbryzek/scala-build
resolvers += "Bryzek Maven repository" at "https://maven.bryzek.com"
libraryDependencies += "com.bryzek" %% "scala-build-sbt" % "0.0.33"
