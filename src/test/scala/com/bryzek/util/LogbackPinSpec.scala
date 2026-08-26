package com.bryzek.util

import ch.qos.logback.classic.joran.JoranConfigurator
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, LoggerContext}
import ch.qos.logback.core.CoreConstants
import ch.qos.logback.core.read.ListAppender
import ch.qos.logback.core.status.Status
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.Logger.ROOT_LOGGER_NAME
import scala.jdk.CollectionConverters.*

/** The logback pair this build resolves is pinned in build.sbt rather than inherited, and every way
  * that pin can be wrong resolves cleanly and says nothing.
  *
  * One of the two advisories the pin answers cannot be observed as behaviour here.
  * GHSA-25qh-j22f-pwp8 is reached through logback-core's conditional configuration processing: an
  * `<if>` element whose condition Janino compiles and runs, out of a configuration file an attacker
  * can write or select through an environment variable. Observing the fix would mean putting Janino
  * on this test classpath and handing logback a hostile configuration -- adding the library the
  * advisory names as its own precondition, to a build that has neither it nor any logback
  * configuration at all. So this reads the resolved version instead, which is the whole of what the
  * override controls: remove it and play-test's own 1.5.18 is what resolves.
  *
  * The other one is behaviour, and it is asserted as behaviour. logback-core below 1.5.25 resolves
  * an `<appender-ref>` out of the appender bag without ever asking whether the configuration
  * DECLARED an appender of that name (GHSA-qqpg-mvqg-649v). What that costs a correct configuration
  * is total and unreported: one reference to a name nothing declares -- a typo, a substituted
  * property that resolved to nothing -- and the referring logger ends up with NO appenders at all,
  * the correctly declared ones beside it included, while the status trail shows only the INFO lines
  * of a configuration that appears to have worked. There is no warning and no error to find
  * afterwards. 1.5.25 adds the declaration check the advisory is about, and with it the undeclared
  * reference is warned about and skipped while its neighbours are attached. An unfixed logback
  * fails both of those assertions -- nothing is attached, and nothing is said about why.
  *
  * A PARTIAL pin is caught twice over, because it is the failure worth catching and neither half of
  * it can be read off a version number. logback publishes classic and core as one train -- classic
  * subclasses core's appender, model and joran types -- so overriding one coordinate and not the
  * other resolves without complaint and throws NoSuchMethodError from inside logback the first time
  * a logger is configured; driving an event from a classic `Logger` into a core `ListAppender`
  * links the two against each other and fails here instead. And the declaration guard spans the
  * pair: its analyser lives in logback-core but logback-classic is what registers it with the
  * processor, so logback-core alone at 1.5.25+ resolves cleanly, links cleanly, and leaves the
  * guard registered by nobody -- the declared-appender set is then empty for the whole
  * configuration, so EVERY appender-ref is warned about and skipped and the logger is left with
  * nothing. The attachment assertion fails in that state; the warning one does not, because the
  * warning it looks for is exactly what a wrongly-skipped reference produces.
  *
  * Every context here is built rather than taken from SLF4J deliberately: the JVM's bound context
  * is shared with anything else this suite starts, and configuring that one resets it, detaching
  * and stopping every appender anything else in this test JVM has attached to it.
  */
class LogbackPinSpec extends AnyWordSpec with Matchers {

  /** The first release in which logback-core no longer runs code named by whoever controls the
    * configuration file (GHSA-25qh-j22f-pwp8, fixed in 1.5.19, and in 1.3.16 on the older line).
    * This is the advisory's floor rather than the pin's, so moving the pin does not touch it.
    */
  private val Floor = (1, 5, 19)

  private val VersionPattern = """^(\d+)\.(\d+)\.(\d+).*$""".r

  /** One declared appender and one reference to a name nothing declares, in that order, so that a
    * configuration whose appender-refs are abandoned wholesale is distinguishable from one whose
    * undeclared reference alone is skipped.
    */
  private val ConfigWithUndeclaredRef =
    """<configuration debug="false">
      |  <appender name="DECLARED" class="ch.qos.logback.core.read.ListAppender"/>
      |  <root level="ERROR">
      |    <appender-ref ref="UNDECLARED"/>
      |    <appender-ref ref="DECLARED"/>
      |  </root>
      |</configuration>""".stripMargin

  /** A context of its own, never the JVM's: configuring the shared `LoggerContext` resets it, which
    * detaches and stops every appender anything else in this test JVM has attached to it.
    */
  private def configure(xml: String): LoggerContext = {
    val context = new LoggerContext()
    val configurator = new JoranConfigurator()
    configurator.setContext(context)
    configurator.doConfigure(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)))
    context
  }

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

    "attach the appenders a configuration declares even when it also references one it does not" in {
      val context = configure(ConfigWithUndeclaredRef)
      val attached = context.getLogger(ROOT_LOGGER_NAME).iteratorForAppenders.asScala.toList
      attached.map(_.getName) mustBe List("DECLARED")
    }

    "report the undeclared reference rather than dropping it silently" in {
      val context = configure(ConfigWithUndeclaredRef)
      val statuses = context.getStatusManager.getCopyOfStatusList.asScala.toList
      statuses.filter(_.getLevel == Status.ERROR).map(_.getMessage) mustBe Nil
      statuses.filter(_.getLevel == Status.WARN).map(_.getMessage).exists(_.contains("UNDECLARED")) mustBe true
    }
  }
}
