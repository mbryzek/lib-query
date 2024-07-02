package io.flow.postgresql

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import helpers.TestHelpers
import scala.util.Try

class SanitizeSpec extends AnyWordSpec with Matchers with TestHelpers {

  "simple query" in {
    expectValidNec {
      Sanitize.validate("data->>'contact_id'")
    }
    expectSuccess {
      Query("select 1").and("balance != 0")
    }
    expectSuccess {
      Query("select 1").or(Seq("balance != 0"))
    }
  }

  "case insensitive" in {
    expectSuccess {
      Query("select 1").or(Seq("NOT true"))
    }
  }
}
