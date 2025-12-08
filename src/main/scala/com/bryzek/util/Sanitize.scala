package com.bryzek.util

import cats.data.Validated.{Invalid, Valid}
import cats.data.ValidatedNec
import cats.implicits._

object Sanitize {

  private val AcceptedChars: Set[String] = "-_.,{}():=!><'0123456789abcdefghijklmnopqrstuvwxyz".split("").toSet
  def assertSafe(v: String): Unit = {
    validate(v) match {
      case Valid(_) => ()
      case Invalid(e) => sys.error(s"Value[$v] contains unsafe characters: ${e.toList.mkString(", ")}")
    }
  }

  def validate(v: String): ValidatedNec[String, Unit] = {
    val invalid = v.trim.toLowerCase().split("").filterNot(_.isEmpty).filterNot(AcceptedChars.contains).distinct
    if (invalid.isEmpty) {
      ().validNec
    } else {
      s"Invalid character(s): ${invalid.mkString("'", "', '", "'")}".invalidNec
    }
  }

}
