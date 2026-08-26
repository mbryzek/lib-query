package com.bryzek.util

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, LoggerContext}
import ch.qos.logback.core.CoreConstants
import ch.qos.logback.core.read.ListAppender
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** The logback pair this build resolves is pinned in build.sbt rather than inherited, and both
  * ways that pin can be wrong resolve cleanly and say nothing.
  *
  * The version the pin exists for cannot be observed as behaviour here. GHSA-25qh-j22f-pwp8 is
  * reached through logback-core's conditional configuration processing: an `<if>` element whose
  * condition Janino compiles and runs, out of a configuration file an attacker can write or select
  * through an environment variable. Observing the fix would mean putting Janino on this test
  * classpath and handing logback a hostile configuration -- adding the library the advisory names
  * as its own precondition, to a build that has neither it nor any logback configuration at all.
  * So this reads the resolved version instead, which is the whole of what the override controls:
  * remove it and play-test's own 1.5.18 is what resolves.
  *
  * The pair, by contrast, is behaviour, and it is the failure worth catching. logback publishes
  * classic and core as one train -- classic subclasses core's appender, model and joran types --
  * so overriding one coordinate and not the other resolves without complaint and throws
  * NoSuchMethodError from inside logback the first time a logger is configured. Driving an event
  * from a classic `Logger` into a core `ListAppender` links the two against each other and fails
  * here instead.
  *
  * The context is built here rather than taken from SLF4J deliberately: the JVM's bound context is
  * shared with anything else this suite starts, and configuring that one would outlive this spec.
  */
class LogbackPinSpec extends AnyWordSpec with Matchers {

  /** The first release in which logback-core no longer runs code named by whoever controls the
    * configuration file (GHSA-25qh-j22f-pwp8, fixed in 1.5.19, and in 1.3.16 on the older line).
    * This is the advisory's floor rather than the pin's, so moving the pin does not touch it.
    */
  private val Floor = (1, 5, 19)

  private val VersionPattern = """^(\d+)\.(\d+)\.(\d+).*$""".r

  "the resolved logback pair" must {

    "resolve logback-core at or above the release that fixed conditional-config code execution" in {
      // Read off the jar manifest rather than through logback's own accessor: `EnvUtil
      // .logbackVersion` is deprecated, and the `VersionUtil` that replaced it does not exist in
      // the version this pin displaces -- so using either one turns "somebody deleted the
      // override" into a compile error about a missing method instead of the refusal below.
      val reported = Option(classOf[CoreConstants].getPackage.getImplementationVersion).getOrElse(
        fail("the logback-core jar declares no Implementation-Version, so the pin cannot be checked")
      )

      val resolved = reported match {
        case VersionPattern(major, minor, patch) => (major.toInt, minor.toInt, patch.toInt)
        case other => fail(s"logback-core reported an unparseable version: $other")
      }

      withClue(s"resolved logback-core $reported, which GHSA-25qh-j22f-pwp8 affects: ") {
        scala.math.Ordering[(Int, Int, Int)].gteq(resolved, Floor) mustBe true
      }
    }

    "link logback-classic against the logback-core it resolved with" in {
      val context = new LoggerContext()
      try {
        val appender = new ListAppender[ILoggingEvent]()
        appender.setContext(context)
        appender.start()

        val logger = context.getLogger(classOf[LogbackPinSpec])
        logger.setLevel(Level.INFO)
        logger.addAppender(appender)
        logger.info("pinned")

        appender.list.size mustBe 1
        appender.list.get(0).getMessage mustBe "pinned"
      } finally {
        context.stop()
      }
    }
  }
}
