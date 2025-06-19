package com.mbryzek.util

import helpers.BaseSpec

class SanitizeSpec extends BaseSpec {

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
