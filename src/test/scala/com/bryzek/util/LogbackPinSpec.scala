package com.bryzek.util

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import ch.qos.logback.core.status.Status
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.Logger.ROOT_LOGGER_NAME
import scala.jdk.CollectionConverters.*

/** The logback this build resolves is pinned in build.sbt rather than inherited, and the pin is
  * silent in both directions when it is wrong.
  *
  * logback-core below 1.5.25 resolves an `<appender-ref>` out of the appender bag without ever
  * asking whether the configuration DECLARED an appender of that name (GHSA-qqpg-mvqg-649v). What
  * that costs a correct configuration is total and unreported: one reference to a name nothing
  * declares -- a typo, a substituted property that resolved to nothing -- and the referring logger
  * ends up with NO appenders at all, the correctly declared ones beside it included, while the
  * status trail shows only the INFO lines of a configuration that appears to have worked. There is
  * no warning and no error to find afterwards. 1.5.25 adds the declaration check the advisory is
  * about, and with it the undeclared reference is warned about and skipped while its neighbours are
  * attached.
  *
  * An unfixed logback fails both assertions -- nothing is attached, and nothing is said about why.
  * A PARTIAL pin fails the first one alone, which is why it is here rather than a version
  * comparison: the guard and its analyser are in logback-core, but logback-classic is what
  * registers the analyser with the processor, so logback-core alone at 1.5.25+ resolves cleanly,
  * links cleanly, and leaves the guard registered by nobody -- the declared-appender set is then
  * empty for the whole configuration, so EVERY appender-ref is warned about and skipped and the
  * logger is left with nothing. The second assertion passes in that state, because the warning it
  * looks for is exactly what a wrongly-skipped reference produces.
  */
class LogbackPinSpec extends AnyWordSpec with Matchers {

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

  "the resolved logback" must {

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
