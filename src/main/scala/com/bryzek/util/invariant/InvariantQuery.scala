package com.bryzek.util.invariant

import com.bryzek.util.Query

/** One data-integrity assertion, expressed as a query that returns the rows that violate it.
  *
  * A healthy database answers zero for every one of these. `queryCount` is what the runner
  * executes on every check; `queryDetails`, when there is one, is what it samples examples from
  * once the count is non-zero.
  */
sealed trait InvariantQuery {
  def name: String
  def withPrefix(prefix: String): InvariantQuery
  def queryCount: Query
  def queryDetails: Option[Query]
}

/** An invariant that can only be counted — there is nothing per-row to show. */
case class Invariant(name: String, queryCount: Query) extends InvariantQuery {
  override def withPrefix(prefix: String): InvariantQuery = this.copy(name = prefix + name)
  override def queryDetails: Option[Query] = None
}

/** An invariant whose query selects the offending rows, so a failure can be shown as well as
  * counted. The count is derived from the detail query rather than written twice, which is what
  * keeps the number and the examples describing the same population.
  */
case class InvariantWithDetails(name: String, query: Query) extends InvariantQuery {
  override def withPrefix(prefix: String): InvariantQuery = this.copy(name = prefix + name)
  override def queryDetails: Option[Query] = Some(query)
  override def queryCount: Query = {
    Query(
      s"""
         |select count(*) from (${query.sql()}) q
         |""".stripMargin,
      bindings = query.bindings
    )
  }
}
