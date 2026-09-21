package com.bryzek.util.invariant

import anorm.SqlParser

import java.sql.Connection
import scala.util.{Failure, Success, Try}

/** The connection source the runner executes through.
  *
  * Every application already has one of these — a decorated Play `Database` — and this is the
  * one method the runner needs of it, so the lib stays free of Play and of any one application's
  * database wiring.
  */
trait InvariantConnections {
  def withConnection[A](block: Connection => A): A
}

object InvariantRunner {

  /** What a caller that expresses no preference gets — enough to see the shape of a failure in an
    * email or a terminal without dumping a whole table into it.
    */
  val DefaultExampleLimit: Int = 10

  /** The hard server-side ceiling. An investigating session needs the whole failing population,
    * not a sample of it (a 45-row failure whose visible 10 covered 4 of 7 clubs is what motivated
    * this), but the detail query still has to be bounded.
    */
  val MaxExampleLimit: Int = 100

  /** Every path into `executeAll` goes through this, so no caller — API parameter, scheduled
    * email, or a future one — can ask the database for an unbounded detail query, and none can
    * ask for zero examples either.
    */
  def clampExampleLimit(requested: Int): Int = math.min(math.max(requested, 1), MaxExampleLimit)

  /** The examples are ordered HERE, in Scala, after the limit — never by the detail query.
    *
    * A detail query that ends in `order by <expr>` makes the `limit` this runner appends
    * unreachable: the sort key is an expression over the outer relation, so Postgres puts a
    * blocking Sort of the whole scanned relation underneath any semi join in the predicate and the
    * sample can no longer stop at the first violating row. Measured on a million-row fixture of
    * `court_reserve.sales_details`, `limit 1` went from 1.8s (merge semi join, early stop) to 6.7s
    * (external merge sort, 350MB spilled to disk, then a million index probes); in production the
    * same ordering took one check's probe route from 4.5-12.5s to 31-58s, past the 30s deadline
    * every reader of it uses. The cost lands only while the check is RED, which is exactly when
    * the examples are wanted.
    *
    * Sorting the fetched strings instead buys the readable, repeatable output an `order by 1` was
    * reaching for at no cost to the plan: there are at most [[MaxExampleLimit]] of them, and they
    * are already in memory. It cannot make the SAMPLE deterministic — an unordered `limit` picks
    * an arbitrary subset either way — so what it fixes is how the sample READS, not which rows it
    * holds. ISS-13029
    */
  def orderExamples(examples: Seq[String]): Seq[String] = examples.sorted
}

/** Executes invariants and reports what each one found.
  *
  * Which invariants to run — and which to skip, snoozed or otherwise — is the application's
  * decision and arrives as the argument. Everything downstream of the results (email, API
  * response) is the application's too.
  */
class InvariantRunner(connections: InvariantConnections) {

  def executeAll(
    invariants: Seq[InvariantQuery],
    exampleLimit: Int = InvariantRunner.DefaultExampleLimit
  ): Seq[InvariantResult] = {
    val limit = InvariantRunner.clampExampleLimit(exampleLimit)
    InvariantResult.sorted(invariants.map { i =>
      val start = System.currentTimeMillis()
      def duration = System.currentTimeMillis() - start

      Try {
        connections.withConnection { c =>
          i.queryCount.as(SqlParser.long(1).single)(using c)
        }
      } match {
        case Success(count) if count == 0 => InvariantResult.ZeroCount(i, duration)
        case Success(count) => InvariantResult.ErrorsFound(i, duration, count, fetchExamples(i, limit))
        case Failure(ex) => InvariantResult.Failure(i, duration, ex)
      }
    })
  }

  private def fetchExamples(invariant: InvariantQuery, limit: Int): Option[Seq[String]] = {
    invariant.queryDetails.map { q =>
      connections.withConnection { c =>
        InvariantRunner.orderExamples(q.limit(limit).as(SqlParser.str(1).*)(using c))
      }
    }
  }
}
