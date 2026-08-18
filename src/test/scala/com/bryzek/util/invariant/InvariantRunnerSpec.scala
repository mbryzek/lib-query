package com.bryzek.util.invariant

import com.bryzek.util.Query
import helpers.BaseSpec

class InvariantRunnerSpec extends BaseSpec {

  private def query(name: String) = Invariant(name, Query("select 1"))

  "clampExampleLimit bounds what any caller can ask for" in {
    InvariantRunner.clampExampleLimit(0) mustBe 1
    InvariantRunner.clampExampleLimit(-10) mustBe 1
    InvariantRunner.clampExampleLimit(25) mustBe 25
    InvariantRunner.clampExampleLimit(InvariantRunner.MaxExampleLimit + 1) mustBe InvariantRunner.MaxExampleLimit
    InvariantRunner.clampExampleLimit(InvariantRunner.DefaultExampleLimit) mustBe InvariantRunner.DefaultExampleLimit
  }

  "sorted puts failures first, then errors, then successes" in {
    val ok = InvariantResult.ZeroCount(query("a_ok"), 1)
    val errors = InvariantResult.ErrorsFound(query("b_errors"), 1, 5, None)
    val failed = InvariantResult.Failure(query("c_failed"), 1, new RuntimeException("boom"))

    InvariantResult.sorted(Seq(ok, errors, failed)).map(_.invariant.name) mustBe Seq(
      "c_failed",
      "b_errors",
      "a_ok"
    )
  }

  "sorted puts the slowest first within a group, then breaks ties by name" in {
    val slow = InvariantResult.ZeroCount(query("slow"), 500)
    val fast = InvariantResult.ZeroCount(query("fast"), 1)
    val tiedB = InvariantResult.ZeroCount(query("B_tied"), 1)

    InvariantResult.sorted(Seq(fast, tiedB, slow)).map(_.invariant.name) mustBe Seq("slow", "B_tied", "fast")
  }
}
