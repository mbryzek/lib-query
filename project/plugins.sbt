logLevel := Level.Warn

// Staying on sbt 1 until sbt-pgp / sbt-sonatype publish final sbt 2.x builds
// (only `_sbt2.0.0-M*` milestone artifacts exist; nothing for sbt 2.0.x final).
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.1")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")
