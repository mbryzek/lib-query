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
  // Built without a margin. `stripMargin` over an interpolated string strips the INTERPOLATED
  // sql too, and a detail query whose own source used a margin has already been stripped once:
  // a continuation line reading `| || ' x ' || y` comes back as `| ' x ' || y`, a bitwise-or
  // against a string literal that Postgres has no operator for. The wrapper does not own this
  // sql and must not re-strip it.
  //
  // Built from `orderBy = None`. An ordering cannot change a cardinality, so the count never needs
  // one -- and paying for one is the bug ISS-13029 is about, a blocking sort of the whole scanned
  // relation underneath the predicate. `generateSql` renders the `orderBy` field into the string
  // this wrapper reads, so dropping the field here is what keeps it out of the count. An ordering
  // written INTO a detail query's own sql is invisible from here and is the caller's to leave out;
  // platform asserts that in `AllInvariantsSpec`.
  override def queryCount: Query = Query(
    s"select count(*) from (${query.copy(orderBy = None).sql()}) q",
    bindings = query.bindings
  )
}
