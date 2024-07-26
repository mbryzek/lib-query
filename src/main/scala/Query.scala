package io.flow.postgresql

import anorm.{NamedParameter, ResultSetParser, Row, SQL, SimpleSql, on}
import io.flow.postgresql.Parameter.TypeUnit

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

case class QueryFilter(sql: String)

case class Query(
  baseQuery: String,
  filters: Seq[QueryFilter] = Nil,
  bindings: Seq[BoundParameter] = Nil,
  limit: Option[Long] = None,
  offset: Option[Long] = None,
  orderBy: Option[String] = None,
  groupBy: Option[String] = None,
  debugging: Boolean = false
) {
  def as[T](
    parser: ResultSetParser[T]
  )(implicit
    c: java.sql.Connection
  ): T = {
    Try {
      anormSql().as(parser)
    } match {
      case Success(v) => v
      case Failure(ex) => {
        ex.printStackTrace(System.err)
        sys.error(s"Error executing SQL Query: ${ex.getMessage}\n${sql()}")
      }
    }
  }

  def executeUpdate(implicit
    c: java.sql.Connection
  ): Int = {
    Try {
      anormSql().executeUpdate()(c)
    } match {
      case Success(v) => v
      case Failure(ex) => {
        maybeReportError(ex)
        throw ex
      }
    }
  }

  def execute(implicit
    c: java.sql.Connection
  ): Boolean = {
    Try {
      anormSql().execute()(c)
    } match {
      case Success(v) => v
      case Failure(ex) => {
        maybeReportError(ex)
        throw ex
      }
    }
  }

  private def maybeReportError(ex: Throwable): Unit = {
    if (ex.getMessage.contains("syntax error")) {
      println(s"SQL Syntax Error. Query:\n${generateSql()}")
      ex.printStackTrace(System.err)
    }
  }

  def anormSql(): SimpleSql[Row] = {
    SQL(sql()).on(bindings.map { b =>
      b.param match {
        case TypeUnit => NamedParameter(b.name, Option.empty[String])
        case _ => NamedParameter(b.name, b.param.value)
      }
    } *)
  }

  def withDebugging: Query = {
    this.copy(debugging = true)
  }

  def sql(): String = {
    val query = generateSql()
    if (debugging) {
      println(s"SQL QUERY: $query")
      if (bindings.nonEmpty) {
        println(s"BIND VARIABLES:")
        bindings.foreach { bp =>
          println(s" - ${bp.name}: ${bp.param.value}")
        }
        println(s"\n\nINTERPOLATED: ${interpolate()}")
      }
    }
    query
  }

  private def generateSql(): String = {
    Seq(
      Some(baseQuery),
      filters.toList match {
        case Nil => None
        case all => Some("where " + all.map(_.sql).mkString(" and "))
      },
      groupBy.map { v => s"group by $v" },
      orderBy.map { v => s"order by $v" },
      limit.map { v => s"limit $v" },
      offset.map { v => s"offset $v" }
    ).flatten.mkString(" ")
  }

  def interpolate(): String = {
    bindings.foldLeft(generateSql()) { case (s, bp) =>
      (bp.param.cast.toSeq.map { c => s"::$c" } :+ "").distinct
        .map { c =>
          s"\\{${bp.name}}$c"
        }
        .foldLeft(s) { case (s, pattern) =>
          s.replaceAll(pattern, bp.param.interpolationValue)
        }
    }
  }

  @tailrec
  private def operation(op: String, name: String, value: Any): Query = {
    value match {
      case None => this
      case Some(v) => operation(op, name, v)
      case v => {
        Sanitize.assertSafe(name)
        val (q, p) = bindVar(name, v)
        q.withFilter(QueryFilter(s"$name $op ${p.bindFragment}"))
      }
    }
  }

  def equals(name: String, value: Any): Query = operation("=", name, value)
  def greaterThan(name: String, value: Any): Query = operation(">", name, value)
  def greaterThanOrEquals(name: String, value: Any): Query = operation(">=", name, value)
  def lessThan(name: String, value: Any): Query = operation("<", name, value)
  def lessThanOrEquals(name: String, value: Any): Query = operation("<=", name, value)

  def and(condition: String): Query = {
    and(Seq(condition))
  }

  def and(conditions: Option[String]): Query = {
    conditions.map(and).getOrElse(this)
  }

  def and(conditions: Seq[String]): Query = {
    withFilters(conditions.map(QueryFilter.apply))
  }

  def or(conditions: Seq[String]): Query = {
    conditions.foreach { c =>
      c.split("\\s+").foreach(Sanitize.assertSafe)
    }
    withFilter(QueryFilter(s"(${conditions.mkString(" or ")})"))
  }

  def isNull(column: String): Query = {
    Sanitize.assertSafe(column)
    withFilter(QueryFilter(s"$column is null"))
  }

  def isNotNull(column: String): Query = {
    Sanitize.assertSafe(column)
    withFilter(QueryFilter(s"$column is not null"))
  }

  def nullBoolean(column: String, value: Option[Boolean]): Query = {
    value.map(nullBoolean(column, _)).getOrElse(this)
  }

  def nullBoolean(column: String, value: Boolean): Query = {
    if (value) {
      isNotNull(column)
    } else {
      isNull(column)
    }
  }

  def optionalIn(column: String, values: Option[Seq[Any]]): Query = {
    values.map(in(column, _)).getOrElse(this)
  }

  def in(column: String, values: Seq[Any]): Query = {
    buildIn("in", "false", column, values)
  }

  def optionalNotIn(column: String, values: Option[Seq[Any]]): Query = {
    values.map(notIn(column, _)).getOrElse(this)
  }

  def notIn(column: String, values: Seq[Any]): Query = {
    buildIn("not in", "true", column, values)
  }

  private def buildIn(op: String, default: String, column: String, values: Seq[Any]): Query = {
    Sanitize.assertSafe(column)
    if (values.isEmpty) {
      // matches nothing
      and(default)
    } else {
      val (q: Query, params: Seq[BoundParameter]) = values.foldLeft((this, List.empty[BoundParameter])) {
        case ((q, names), v) =>
          val (newQ, param) = q.bindVar(column, v)
          (newQ, names :+ param)
      }
      q.withFilter(
        QueryFilter(
          s"$column $op (${params.map(_.bindFragment).mkString(", ")})"
        )
      )
    }
  }

  def optionalIn2(columns: (String, String), values: Option[Seq[(Any, Any)]]): Query = {
    values.map(in2(columns, _)).getOrElse(this)
  }

  def in2(columns: (String, String), values: Seq[(Any, Any)]): Query = {
    bindInList(Seq(columns._1, columns._2), values.flatMap { v => Seq(v._1, v._2) })
  }

  def optionalIn3(columns: (String, String, String), values: Option[Seq[(Any, Any, Any)]]): Query = {
    values.map(in3(columns, _)).getOrElse(this)
  }

  def in3(columns: (String, String, String), values: Seq[(Any, Any, Any)]): Query = {
    bindInList(Seq(columns._1, columns._2, columns._3), values.flatMap { v => Seq(v._1, v._2, v._3) })
  }

  def optionalIn4(
    columns: (String, String, String, String),
    values: Option[Seq[(Any, Any, Any, Any)]]
  ): Query = {
    values.map(in4(columns, _)).getOrElse(this)
  }

  def in4(columns: (String, String, String, String), values: Seq[(Any, Any, Any, Any)]): Query = {
    bindInList(Seq(columns._1, columns._2, columns._3, columns._4), values.flatMap { v => Seq(v._1, v._2, v._3, v._4) })
  }

  def optionalIn5(
    columns: (String, String, String, String, String),
    values: Option[Seq[(Any, Any, Any, Any, Any)]]
  ): Query = {
    values.map(in5(columns, _)).getOrElse(this)
  }

  def in5(
    columns: (String, String, String, String, String),
    values: Seq[(Any, Any, Any, Any, Any)]
  ): Query = {
    bindInList(
      Seq(columns._1, columns._2, columns._3, columns._4, columns._5),
      values.flatMap { v => Seq(v._1, v._2, v._3, v._4, v._5) }
    )
  }

  def in(name: String, subQuery: Query): Query = {
    Sanitize.assertSafe(name)
    val (q, sql) = subQuery.bindings
      .foldLeft((this, subQuery.sql())) { case ((q, sql), bp) =>
        val (newQ, localParam) = q.bindVar(bp.name, bp.param.original)
        val modSql = if (localParam.name != bp.name) {
          // We have a conflict on the name of a bind variable in the subquery.
          // Modify the subquery to use the new unique bind variable name.
          sql.replaceAll(s"\\{${bp.name}}", s"{${localParam.name}}")
        } else {
          sql
        }
        (newQ, modSql)
      }

    q.withFilter(
      QueryFilter(s"$name in ($sql)")
    )
  }

  def optionalLimit(v: Option[Long]): Query = {
    v.map(limit(_)).getOrElse(this)
  }

  def limit(limit: Long): Query = {
    this.copy(limit = Some(limit))
  }

  def optionalOffset(v: Option[Long]): Query = {
    v.map(offset(_)).getOrElse(this)
  }

  def offset(offset: Long): Query = {
    this.copy(offset = Some(offset))
  }

  def orderBy(value: Option[String]): Query = {
    value.map(orderBy(_)).getOrElse(this)
  }

  def orderBy(sort: String): Query = {
    this.copy(orderBy = Some(sort))
  }

  def groupBy(by: Option[String]): Query = {
    by.map(groupBy(_)).getOrElse(this)
  }

  def groupBy(by: String): Query = {
    this.copy(groupBy = Some(by))
  }

  def bind(name: String, value: Any): Query = bindVar(name, value)._1

  private def bindVar(name: String, value: Any): (Query, BoundParameter) = {
    internalBind(bindVarName(name), Parameter.from(value), 1)
  }

  private def bindVarName(name: String): String = {
    rewriteJson(stripCast(name))
  }

  // remove any user provided cast (e.g. "name::text" => name)
  private def stripCast(value: String): String = {
    val i = value.indexOf("::")
    if (i > 0) {
      value.take(i)
    } else {
      value
    }
  }

  private def rewriteJson(value: String): String = {
    val v = Seq("->>", "->", "'", "\\(", "\\)")
      .foldLeft(value) { case (v, pattern) =>
        v.replaceAll(pattern, "_")
      }
      .replaceAll("_+", "_")

    if (v.endsWith("_")) {
      v.dropRight(1)
    } else {
      v
    }
  }

  @tailrec
  private def internalBind(name: String, param: Parameter[?], count: Int): (Query, BoundParameter) = {
    val candidate = count match {
      case 1 => name
      case _ => s"${name}_$count"
    }
    bindings.find(_.name == candidate) match {
      case Some(_) => internalBind(name, param, count + 1)
      case None =>
        val bp = BoundParameter(candidate, param)
        (
          this.copy(bindings = bindings :+ bp),
          bp
        )
    }
  }

  private def withFilter(filter: QueryFilter): Query = {
    withFilters(Seq(filter))
  }

  private def withFilters(newFilters: Seq[QueryFilter]): Query = {
    this.copy(filters = filters ++ newFilters)
  }

  private def bindInList(columns: Seq[String], values: Seq[Any]): Query = {
    columns.foreach(Sanitize.assertSafe)
    val (q: Query, params: Seq[BoundParameter]) =
      columns.zip(values).foldLeft((this, List.empty[BoundParameter])) { case ((q, names), (n, v)) =>
        val (newQ, param) = q.bindVar(n, v)
        (newQ, names :+ param)
      }
    q.withFilter(
      QueryFilter(s"(${columns.mkString(", ")}) in ((${params.map(_.bindFragment).mkString(", ")}))")
    )
  }

}
