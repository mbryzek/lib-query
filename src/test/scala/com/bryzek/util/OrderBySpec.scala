package com.bryzek.util

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

      expectValidNec {
        OrderBy.parse("users.id")
      }.sql.get mustBe "users.id"
    }

    "valid identifiers with sort" in {
      expectValidNec {
        OrderBy.parse("-id, -name")
      }.sql.get mustBe "id desc, name desc"
    }

    "valid identifiers with quoted reserved words" in {
      expectValidNec {
        OrderBy.parse("-\"order\"")
      }.sql.get mustBe "\"order\" desc"
    }

    "valid identifiers with type cast" must {
      "valid" in {
        expectValidNec {
          OrderBy.parse("name::text")
        }.sql.get mustBe "name::text"
      }

      "invalid" in {
        expectInvalidNec {
          OrderBy.parse("name::drop table users")
        } mustBe Seq("Invalid cast: 'drop table users'")
      }
    }

    "invalid identifiers" in {
      expectInvalidNec {
        OrderBy.parse("drop table users")
      } mustBe Seq("Invalid column name: 'drop table users'")
    }

    "lowercase sort" in {
      def parse(validValues: Option[Set[String]]) = {
        expectValidNec {
          OrderBy.parse("lower(name)", validValues = validValues)
        }.sql.get
      }

      parse(None) mustBe "lower(name)"
      parse(Some(Set("name"))) mustBe "lower(name)"
    }

    "uppercase sort" in {
      def parse(validValues: Option[Set[String]]) = {
        expectValidNec {
          OrderBy.parse("UPPER(name)", validValues = validValues)
        }.sql.get
      }

      parse(None) mustBe "UPPER(name)"
      parse(Some(Set("name"))) mustBe "UPPER(name)"
    }
  }

  "nullsLast" must {
    "not applied by default" in {
      expectValidNec {
        OrderBy.parse("-id, -name")
      }.sql.get mustBe "id desc, name desc"
    }

    "appended to desc terms when nullsLast=true" in {
      expectValidNec {
        OrderBy.parse("-id, -name", nullsLast = true)
      }.sql.get mustBe "id desc nulls last, name desc nulls last"
    }

    "not appended to asc terms when nullsLast=true" in {
      expectValidNec {
        OrderBy.parse("id, name", nullsLast = true)
      }.sql.get mustBe "id, name"
    }

    "mixed asc and desc" in {
      expectValidNec {
        OrderBy.parse("id, -name", nullsLast = true)
      }.sql.get mustBe "id, name desc nulls last"
    }
  }

}
