package io.flow.postgresql

import cats.data.ValidatedNec
import cats.data.Validated.{Invalid, Valid}
import cats.implicits._

case class OrderBy(sql: Option[String])

object OrderBy {
  private val SafeFunctions = Set("lower", "upper")
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

  private def parseTerm(term: String, validValues: Option[Set[String]]): ValidatedNec[String, String] = {
    val (isDesc, value) = term.trim match {
      case s if s.startsWith("-") => (true, s.substring(1))
      case s => (false, s)
    }

    validateTerm(value, validValues).map { sanitized =>
      if (isDesc) s"$sanitized desc" else sanitized
    }
  }

  private def validateTerm(term: String, validValues: Option[Set[String]]): ValidatedNec[String, String] = term match {
    case FunctionPattern(func, inner) if SafeFunctions.contains(func.toLowerCase) =>
      validateTerm(inner, validValues).map(sanitized => s"$func($sanitized)")
    case s =>
      if (!s.matches("^[a-zA-Z0-9_]+(\\.[a-zA-Z0-9_]+)?$")) {
        s"Invalid column name: '$s'".invalidNec
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
