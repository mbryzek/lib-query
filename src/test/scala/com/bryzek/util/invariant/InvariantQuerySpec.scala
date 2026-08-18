package com.bryzek.util.invariant

import com.bryzek.util.Query
import helpers.BaseSpec

class InvariantQuerySpec extends BaseSpec {

  private val details = InvariantWithDetails("orphaned_rows", Query("select id from users").equals("status", "bad"))
  private val counted = Invariant("counted", Query("select count(*) from users"))

  "Invariant has no details" in {
    counted.queryDetails mustBe None
    counted.queryCount.sql() mustBe "select count(*) from users"
  }

  "InvariantWithDetails counts the rows its details select" in {
    details.queryDetails.map(_.sql()) mustBe Some(details.query.sql())
    details.queryCount.sql().trim mustBe s"select count(*) from (${details.query.sql()}) q"
  }

  "InvariantWithDetails carries the detail query bindings into the count" in {
    details.queryCount.bindings mustBe details.query.bindings
    details.queryCount.interpolate() must include("'bad'")
  }

  "withPrefix namespaces the name and nothing else" in {
    val prefixed = details.withPrefix("user_")
    prefixed.name mustBe "user_orphaned_rows"
    prefixed.queryCount.sql() mustBe details.queryCount.sql()

    counted.withPrefix("user_").name mustBe "user_counted"
  }
}
