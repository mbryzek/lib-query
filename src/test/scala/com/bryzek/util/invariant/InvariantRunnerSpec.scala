package com.bryzek.util.invariant

import com.bryzek.util.Query
import helpers.BaseSpec

import java.sql.Connection

class InvariantRunnerSpec extends BaseSpec {

  private def query(name: String) = Invariant(name, Query("select 1"))
  private def detailed(name: String) = InvariantWithDetails(name, Query("select 'x'"))

  /** Answers each `withConnection` with the next step instead of running its block, so a spec
    * decides what the count and the examples do without a database. The runner asks for the count
    * first and the examples second.
    */
  private class ScriptedConnections(steps: (() => Any)*) extends InvariantConnections {
    private val remaining = steps.iterator
    var calls: Int = 0

    override def withConnection[A](block: Connection => A): A = {
      calls += 1
      remaining.next()().asInstanceOf[A]
    }
  }

  /** Spends real time without sleeping, so a recorded duration has something to include. */
  private def spend(millis: Long): Unit = {
    val until = System.currentTimeMillis() + millis
    while (System.currentTimeMillis() < until) {}
  }

  private def only(connections: InvariantConnections, invariant: InvariantQuery): InvariantResult = {
    new InvariantRunner(connections).executeAll(Seq(invariant)) match {
      case Seq(result) => result
      case other => fail(s"expected one result, got $other")
    }
  }

  "a count of zero never runs the examples query" in {
    val connections = new ScriptedConnections(() => 0L)
    only(connections, detailed("green")) mustBe a[InvariantResult.ZeroCount]
    connections.calls mustBe 1
  }

  "a red check reports its count and examples" in {
    val connections = new ScriptedConnections(() => 3L, () => Seq("b", "a"))
    only(connections, detailed("red")) match {
      case r: InvariantResult.ErrorsFound =>
        r.count mustBe 3L
        r.examples mustBe Some(Seq("b", "a"))
      case other => fail(s"expected ErrorsFound, got $other")
    }
  }

  "a red check with no detail query has no examples to fail on" in {
    val connections = new ScriptedConnections(() => 3L)
    only(connections, query("red")) match {
      case r: InvariantResult.ErrorsFound => r.examples mustBe None
      case other => fail(s"expected ErrorsFound, got $other")
    }
    connections.calls mustBe 1
  }

  "a failed count is that check's failure, with the original error" in {
    val boom = new RuntimeException("relation does not exist")
    only(new ScriptedConnections(() => throw boom), detailed("broken")) match {
      case r: InvariantResult.Failure => r.error mustBe boom
      case other => fail(s"expected Failure, got $other")
    }
  }

  "a failed examples query is that check's failure, and keeps the count and the cause" in {
    val cancelled = new java.sql.SQLException("canceling statement due to statement timeout", "57014")
    only(new ScriptedConnections(() => 3L, () => throw cancelled), detailed("red")) match {
      case r: InvariantResult.Failure =>
        r.error.getCause mustBe cancelled
        r.error.getMessage must include("Found 3 violation(s)")
        r.error.getMessage must include("canceling statement due to statement timeout")
      case other => fail(s"expected Failure, got $other")
    }
  }

  "a failed examples query leaves the other checks in the run reporting" in {
    val connections = new ScriptedConnections(
      () => 3L,
      () => throw new RuntimeException("boom"),
      () => 0L
    )
    new InvariantRunner(connections)
      .executeAll(Seq(detailed("red"), detailed("green")))
      .map(r => r.invariant.name -> r.getClass.getSimpleName) mustBe Seq(
      "red" -> "Failure",
      "green" -> "ZeroCount"
    )
  }

  "a red check's duration includes its examples" in {
    val connections = new ScriptedConnections(
      () => 3L,
      () => {
        spend(50)
        Seq("a")
      }
    )
    only(connections, detailed("red")).durationMillis must be >= 50L
  }

  "a failed examples query's duration includes the time it took to fail" in {
    val connections = new ScriptedConnections(
      () => 3L,
      () => {
        spend(50)
        throw new RuntimeException("boom")
      }
    )
    only(connections, detailed("red")).durationMillis must be >= 50L
  }

  "clampExampleLimit bounds what any caller can ask for" in {
    InvariantRunner.clampExampleLimit(0) mustBe 1
    InvariantRunner.clampExampleLimit(-10) mustBe 1
    InvariantRunner.clampExampleLimit(25) mustBe 25
    InvariantRunner.clampExampleLimit(InvariantRunner.MaxExampleLimit + 1) mustBe InvariantRunner.MaxExampleLimit
    InvariantRunner.clampExampleLimit(InvariantRunner.DefaultExampleLimit) mustBe InvariantRunner.DefaultExampleLimit
  }

  // ISS-13029: this is the whole reason a detail query may not carry a top-level `order by`.
  // Ordering the fetched strings costs nothing -- at most MaxExampleLimit of them, already in
  // memory -- while ordering in sql makes the runner's own `limit` unreachable and turns a sample
  // into a full scan with a disk spill.
  "orderExamples sorts the sample after the limit rather than in sql" in {
    InvariantRunner.orderExamples(Seq("c", "a", "b")) mustBe Seq("a", "b", "c")
    InvariantRunner.orderExamples(Seq("club-2 x", "club-1 y", "club-1 a")) mustBe Seq(
      "club-1 a",
      "club-1 y",
      "club-2 x"
    )
    InvariantRunner.orderExamples(Nil) mustBe Nil
    InvariantRunner.orderExamples(Seq("only")) mustBe Seq("only")
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
