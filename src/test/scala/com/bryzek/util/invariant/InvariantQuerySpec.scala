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
    details.queryCount.sql() mustBe s"select count(*) from (${details.query.sql()}) q"
  }

  // The wrapper does not own the detail query's sql, and a caller that wrote its own with a
  // margin has already stripped it. Re-stripping eats the leading `|` of a `||` continuation
  // and renders `| 'x'` -- a bitwise-or against a string literal Postgres has no operator for.
  "InvariantWithDetails leaves a detail query's own margin-stripped sql alone" in {
    val concatenated = InvariantWithDetails(
      "leading_concatenation",
      Query(
        """select u.id
          | || ' name ' || u.name
          | || ' status ' || u.status from users u""".stripMargin
      )
    )

    concatenated.query.sql() must include(" || ' name ' || u.name")
    concatenated.queryCount.sql() must include(" || ' name ' || u.name")
    concatenated.queryCount.sql() must not(include("\n| ' name '"))
    concatenated.queryCount.sql() mustBe s"select count(*) from (${concatenated.query.sql()}) q"
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
