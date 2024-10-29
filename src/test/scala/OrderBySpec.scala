package io.flow.postgresql

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}
import helpers.BaseSpec

class OrderBySpec extends BaseSpec {

  "parse" must {
    "empty" in {
      expectValidNec {
        OrderBy.parse("")
      }.sql mustBe None
    }

    "valid identifiers" in {
      expectValidNec {
        OrderBy.parse("id, name")
      }.sql.get mustBe "id, name"
    }

    "valid identifiers with sort" in {
      expectValidNec {
        OrderBy.parse("-id, -name")
      }.sql.get mustBe "id desc, name desc"
    }

    "invalid identifiers" in {
      expectInvalidNec {
        OrderBy.parse("drop table users")
      } mustBe Seq("Invalid character(s): ' '")
    }

    "lowercase sort" in {
      def parse(validValues: Option[Set[String]] = None) = {
        expectValidNec {
          OrderBy.parse("lower(name)", validValues = validValues)
        }.sql.get
      }

      parse(None) mustBe "lower(name)"
      parse(Some(Set("name"))) mustBe "lower(name)"
    }
  }

}
