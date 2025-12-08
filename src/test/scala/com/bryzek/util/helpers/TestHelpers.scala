package helpers

import cats.data.ValidatedNec
import cats.data.Validated.{Invalid, Valid}

import scala.util.{Try, Success, Failure}

trait TestHelpers {

  def expectValidNec[S, T](value: ValidatedNec[S, T]): T = {
    value match {
      case Invalid(e) => sys.error(s"Expected valid but got: ${e.toNonEmptyList.toList.mkString(", ")}")
      case Valid(v) => v
    }
  }

  def expectInvalidNec[T](value: ValidatedNec[T, ?]): Seq[T] = {
    value match {
      case Invalid(e) => e.toNonEmptyList.toList
      case Valid(v) => sys.error(s"Expected invalid but got: $v")
    }
  }

  def expectSuccess[T](f: => T): T = {
    Try(f) match {
      case Success(r) => r
      case Failure(ex) => sys.error(s"Expected success but got failure: ${ex.getMessage}")
    }
  }

  def expectFailure(f: => Any): Throwable = {
    Try(f) match {
      case Success(_) => sys.error("Expected failure")
      case Failure(ex) => ex
    }
  }
}
