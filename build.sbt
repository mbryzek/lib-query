name := "lib-query"

version := "0.2.20"

// Publishing metadata, the guava pin, the browser-stack exclusions, the compiler gate and the
// root project's test reporting are shared by every lib-* repository and DECLARED in
// project/BryzekLibSettings.scala, a copy of devops's
// templates/scala-libs/project/BryzekLibSettings.scala that ci/build.sh holds byte-identical to
// it. That file carries why each setting is what it is; a change is edited there and copied here.
BryzekLibSettings.buildSettings("lib-query")

ThisBuild / scalaVersion := "3.8.4"

// The Jackson 2, Jackson 3 and logback security pins are DECLARED in project/BryzekPins.scala, a
// copy of devops's templates/scala-libs/project/BryzekPins.scala that ci/build.sh holds
// byte-identical to it, together with the pin specs under src/test/scala/com/bryzek/pins. That
// file carries why each floor is what it is; an advisory bump is edited there and copied here.
//
// Applied as `dependencyOverrides`, which governs THIS build's resolution only -- sbt writes none
// of it into the published POM -- so it decides what this repo compiles and tests against and
// imposes no floor on a consumer. No Jackson 3 resolves here; logback
// arrives only through `scalatestplus-play % Test`.
ThisBuild / dependencyOverrides ++= BryzekPins.jackson2 ++ BryzekPins.logback

lazy val root = project
  .in(file("."))
  .settings(BryzekLibSettings.rootSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.playframework.anorm" %% "anorm-postgres" % "3.1.0",
      // The JDBC driver reaches this build only through anorm-postgres, which declares
      // org.postgresql:postgresql 42.7.11 at compile scope -- inside the affected range of
      // GHSA-j92g-9f8w-j867, where a channel-binding SCRAM handshake silently DOWNGRADES to one
      // without channel binding when the server certificate uses a signature algorithm the driver
      // cannot hash, so a connection that asked to be bound to the certificate is not, and says
      // nothing. anorm-postgres 3.1.0 is its newest release, so there is no upstream version to
      // move to and the driver has to be named here.
      //
      // DECLARED, NOT `dependencyOverrides`. An override is resolution-local: it fixes this
      // build's classpath and writes nothing into the published POM, so every consumer would keep
      // resolving 42.7.11 through the anorm-postgres edge and inherit the same advisory from a
      // library that reads as fixed. A direct compile-scope dependency is what reaches them.
      //
      // 42.7.13 rather than the advisory's own 42.7.12 floor: both are out of range, and 42.7.13
      // is what platform and acumen already pin, so the driver this library's tests resolve is the
      // one its consumers actually run and the next dependency sweep has nothing to bump.
      "org.postgresql" % "postgresql" % "42.7.14",
      "org.typelevel" %% "cats-core" % "2.13.0",
      "joda-time" % "joda-time" % "2.15.1",
      // org.lz4:lz4-java reaches the test classpath only here, transitively:
      // scalatestplus-play -> play-ws -> play -> pekko-serialization-jackson -> lz4-java.
      // Nothing on that classpath can call it. Pekko loads an LZ4 codec reflectively only when
      // pekko.serialization.jackson.compression.algorithm is set to lz4 (it ships `off`, and this
      // repo has no application.conf to override it), and the JacksonSerializer that would read
      // the setting is reached only through pekko remoting/clustering/persistence, none of which
      // resolves here. org.lz4 is dead upstream -- its newest version is a relocation POM, so
      // there is no version to bump to -- which makes dropping the jar the only way to keep
      // advisories against it off this build. Excluded on this dependency rather than through a
      // build-wide `excludeDependencies`, which writes an <exclusions> block onto every
      // compile-scope dependency in the published POM; this one is test scope, so it reaches no
      // consumer. ISS-4734
      ("org.scalatestplus.play" %% "scalatestplus-play" % "7.0.2" % Test)
        .exclude("org.lz4", "lz4-java")
    )
  )
