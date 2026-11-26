package com.bryzek.util

import anorm.{Column, RowParser, SqlParser}

/** anorm's row parsers, taking the column or aggregate a statement selected rather than its name as text:
  * `str(i.Id) ~ long(i.NumberValue) ~ get[DateTime](i.CreatedAt)`, where the text would read
  * `SqlParser.str(i.Id.name)`. Each reads the entry's bare `name`, which is what a qualified column (`i.id`) and an
  * aggregate (`count(i.id) as count_id`) are both labelled in the result.
  *
  * `import com.bryzek.util.SqlParsers.*` beside, never with, `import anorm.SqlParser.*`: both define `str` and the rest.
  */
object SqlParsers {
  def str(e: SqlExpr): RowParser[String] = SqlParser.str(e.name)
  def long(e: SqlExpr): RowParser[Long] = SqlParser.long(e.name)
  def int(e: SqlExpr): RowParser[Int] = SqlParser.int(e.name)
  def bool(e: SqlExpr): RowParser[Boolean] = SqlParser.bool(e.name)
  def double(e: SqlExpr): RowParser[Double] = SqlParser.double(e.name)
  def get[T](e: SqlExpr)(using Column[T]): RowParser[T] = SqlParser.get[T](e.name)
}
