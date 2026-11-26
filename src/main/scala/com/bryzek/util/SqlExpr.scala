package com.bryzek.util

/** One entry of a select list: a column or an aggregate over one.
  *
  * `sql` is what the statement renders (`i.status`, `count(i.id)`), and `name` is the bare name it binds and reads
  * back under (`status`, `count_id`). A bind name is never the qualified `sql`, so a value bound for `i.status` is
  * `{status}`, never the dotted `{i.status}` anorm cannot fill.
  */
trait SqlExpr {
  def sql: String
  def name: String

  /** How this entry renders in a select list. A column's result is already named `name`; an aggregate is aliased. */
  def selectSql: String = sql

  def asc: SortTerm = SortTerm(sortable, descending = false)
  def desc: SortTerm = SortTerm(sortable, descending = true)

  /** The expression an `order by` renders, checked unless lib-query built it. */
  protected def sortable: String = {
    Sanitize.assertSafe(sql)
    sql
  }
}

/** A column of a table, rendered qualified (`i.status`, `issues.status`) and bound under its bare `name`.
  *
  * lib-query declares only what a query needs from a column; play-core's generated `Column` implements it.
  */
trait SqlColumn extends SqlExpr {

  /** This column under another qualifier, for a derived table or CTE that carries it (`k.body`):
    * `${c.Body.at("k")}`, where the text would otherwise read `k.${c.Body.name}`.
    */
  def at(qualifier: String): SqlColumn = SqlColumn.Qualified(qualifier, name)

  /** `i.status = 'fixed'`: a predicate over a stored literal, quoted and escaped here, for hand-written SQL that
    * names a value rather than binding one (`${i.Status.is(IssueStatus.Fixed)}`).
    */
  def is(value: Any): String = s"${SqlColumn.checked(this)} = ${SqlLiteral.render(value)}"
  def isNot(value: Any): String = s"${SqlColumn.checked(this)} != ${SqlLiteral.render(value)}"

  /** `i.status in ('fixed', 'deployed')`; with no values it is `false`, which `in ()` cannot be. */
  def in(values: Any*): String =
    if (values.isEmpty) "false" else s"${SqlColumn.checked(this)} in (${values.map(SqlLiteral.render).mkString(", ")})"

  /** `i.status not in ('fixed', 'deployed')`; with no values it is `true`. */
  def notIn(values: Any*): String =
    if (values.isEmpty) "true"
    else s"${SqlColumn.checked(this)} not in (${values.map(SqlLiteral.render).mkString(", ")})"
}

object SqlColumn {
  private[util] final case class Qualified(qualifier: String, name: String) extends SqlColumn {
    Sanitize.assertSafe(qualifier)
    Sanitize.assertSafe(name)
    override def sql: String = s"$qualifier.$name"
    override def toString: String = sql
  }

  private[util] def checked(column: SqlColumn): String = {
    Sanitize.assertSafe(column.sql)
    column.sql
  }
}

/** A value written into SQL text rather than bound: a number or boolean as it is, anything else (a generated enum, a
  * string) as its `toString`, single-quoted with every quote inside it doubled, so a missing quote cannot compile.
  */
object SqlLiteral {
  def render(value: Any): String = value match {
    case v: (Int | Long | Short | Byte | BigInt | BigDecimal | Boolean) => v.toString
    case v => "'" + v.toString.replace("'", "''") + "'"
  }
}

/** A table as a `from` or `join` target: `issues.issues i`, or `issues.issues` unaliased. */
trait SqlTable {
  def sql: String
}

/** An aggregate over a column, `count(i.id)`, named `count_id` unless renamed with [[as]]. Built only by [[Sql]], which
  * checks the column it renders, so its `sql` is never caller text.
  */
final case class SqlAggregate private[util] (sql: String, name: String) extends SqlExpr {
  Sanitize.assertSafe(name)

  override def selectSql: String = s"$sql as $name"

  override protected def sortable: String = sql

  def as(name: String): SqlAggregate = copy(name = name)
}

/** The aggregates a typed query writes, so no aggregate in a select list or a `having` is text. */
object Sql {
  def count(): SqlAggregate = SqlAggregate("count(*)", "count")
  def count(column: SqlColumn): SqlAggregate = over("count", column)
  def countDistinct(column: SqlColumn): SqlAggregate = {
    Sanitize.assertSafe(column.sql)
    SqlAggregate(s"count(distinct ${column.sql})", s"count_distinct_${column.name}")
  }
  def sum(column: SqlColumn): SqlAggregate = over("sum", column)
  def max(column: SqlColumn): SqlAggregate = over("max", column)
  def min(column: SqlColumn): SqlAggregate = over("min", column)

  /** The bare names of `columns`, for an insert's column list or a `returning`: `id, event_key, kind`. */
  def names(columns: SqlExpr*): String = columns.map(bare).mkString(", ")

  /** The bind placeholders of `columns`, under their bare names: `{id}, {event_key}, {kind}`. */
  def binds(columns: SqlExpr*): String = columns.map(c => s"{${bare(c)}}").mkString(", ")

  private def bare(column: SqlExpr): String = {
    Sanitize.assertSafe(column.name)
    column.name
  }

  private def over(function: String, column: SqlColumn): SqlAggregate = {
    Sanitize.assertSafe(column.sql)
    SqlAggregate(s"$function(${column.sql})", s"${function}_${column.name}")
  }
}

/** One term of an `order by`, built from a column or aggregate: `i.created_at desc nulls last`. */
final case class SortTerm private[util] (expr: String, descending: Boolean, nulls: Option[SortTerm.Nulls] = None) {
  def nullsFirst: SortTerm = copy(nulls = Some(SortTerm.Nulls.First))
  def nullsLast: SortTerm = copy(nulls = Some(SortTerm.Nulls.Last))

  def sql: String = {
    val direction = if (descending) " desc" else ""
    val placement = nulls.fold("") {
      case SortTerm.Nulls.First => " nulls first"
      case SortTerm.Nulls.Last => " nulls last"
    }
    s"$expr$direction$placement"
  }
}

object SortTerm {
  enum Nulls {
    case First, Last
  }
}
