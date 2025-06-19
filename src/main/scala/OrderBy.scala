package com.mbryzek.util

import cats.data.ValidatedNec
import cats.implicits.*

case class OrderBy(sql: Option[String])

object OrderBy {
  private val SafeFunctions = Set("lower", "upper", "abs")
  private val FunctionPattern = """(\w+)\((.*)\)""".r

  def apply(value: String, validValues: Option[Set[String]] = None): OrderBy = {
    parse(value, validValues).valueOr(e => throw new IllegalArgumentException(e.toNonEmptyList.toList.mkString(", ")))
  }

  def parse(value: String, validValues: Option[Set[String]] = None): ValidatedNec[String, OrderBy] = {
    val terms = value.split(",").map(_.trim).filterNot(_.isEmpty)

    terms.toList
      .traverse(parseTerm(_, validValues))
      .map { v =>
        if (v.isEmpty) OrderBy(None) else OrderBy(Some(v.mkString(", ")))
      }
  }

  private case class TermWithCast(term: String, cast: Option[String]) {
    def withCast(term: String): String = {
      cast match {
        case Some(c) => s"$term::$c"
        case None => term
      }
    }
  }

  private def removeCast(term: String): ValidatedNec[String, TermWithCast] = {
    val i = term.indexOf("::")
    if (i < 0) {
      TermWithCast(term.trim, None).validNec
    } else {
      val cast = term.substring(i + 2).trim
      validateTerm("cast", cast, validValues = None).map { _ =>
        TermWithCast(term.substring(0, i).trim, Some(cast))
      }
    }
  }

  private def parseTerm(originalTerm: String, validValues: Option[Set[String]]): ValidatedNec[String, String] = {
    removeCast(originalTerm).andThen { twc =>
      val (isDesc, value) = twc.term match {
        case s if s.startsWith("-") => (true, s.substring(1))
        case s => (false, s)
      }

      validateTerm("column name", value, validValues).map { sanitized =>
        val t = twc.withCast(sanitized)
        if (isDesc) s"$t desc" else t
      }
    }
  }

  private def validateTerm(what: String, term: String, validValues: Option[Set[String]]): ValidatedNec[String, String] =
    term match {
      case FunctionPattern(func, inner) if SafeFunctions.contains(func.toLowerCase) =>
        validateTerm("column name", inner, validValues).map(sanitized => s"$func($sanitized)")
      case s =>
        if (!s.matches("^[a-zA-Z0-9_]+(\\.[a-zA-Z0-9_]+)?$")) {
          s"Invalid $what: '$s'".invalidNec
        } else {
          // For table.column format, only validate the column part if validValues is specified
          val columnName = s.split("\\.").last
          validValues match {
            case Some(values) if !values.contains(columnName) =>
              s"Invalid sort field '$columnName'. Valid values are: ${values.mkString(", ")}".invalidNec
            case _ => s.validNec
          }
        }
    }
}
