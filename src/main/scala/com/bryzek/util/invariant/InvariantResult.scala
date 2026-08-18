package com.bryzek.util.invariant

/** What running one [[InvariantQuery]] produced. */
sealed trait InvariantResult {
  def invariant: InvariantQuery
  def durationMillis: Long
}

object InvariantResult {

  /** The invariant holds: the count came back zero. */
  case class ZeroCount(override val invariant: InvariantQuery, override val durationMillis: Long)
    extends InvariantResult

  /** The invariant is violated by `count` rows, with `examples` sampled from the detail query
    * when the invariant has one.
    */
  case class ErrorsFound(
    override val invariant: InvariantQuery,
    override val durationMillis: Long,
    count: Long,
    examples: Option[Seq[String]]
  ) extends InvariantResult

  /** The check itself failed — a bad query, a missing table, a timeout. Distinct from
    * `ErrorsFound` because it says nothing about whether the invariant holds.
    */
  case class Failure(override val invariant: InvariantQuery, override val durationMillis: Long, error: Throwable)
    extends InvariantResult

  /** Worst first, then slowest, then by name: a reader of a report or an email sees the checks
    * that failed outright, then the ones with violations, then the ones that passed, and within
    * each group the expensive ones first.
    */
  def sorted(results: Seq[InvariantResult]): Seq[InvariantResult] = {
    def ord(i: InvariantResult, index: Int) = (index, 0 - i.durationMillis, i.invariant.name.toLowerCase)

    results.sortBy {
      case i: InvariantResult.Failure => ord(i, 0)
      case i: InvariantResult.ErrorsFound => ord(i, 1)
      case i: InvariantResult.ZeroCount => ord(i, 2)
    }
  }
}
