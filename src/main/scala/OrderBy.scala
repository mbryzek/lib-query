package io.flow.postgresql

import cats.data.Validated.{Invalid, Valid}
import cats.data.ValidatedNec
import cats.implicits._

sealed trait OrderBy {
  def sql: Option[String]
}

object OrderBy {
  private case object NoOrderBy extends OrderBy {
    override def sql: Option[String] = None
  }
  private case class HasOrderBy(clause: String) extends OrderBy {
    override def sql: Option[String] = Some(clause)
  }

  def apply(sort: String): OrderBy = {
    parse(sort) match {
      case Invalid(e) => sys.error(s"Invalid sort: '$sort' ${e.toList.distinct.mkString(", ")}")
      case Valid(o) => o
    }
  }

  def parse(sort: String, validValues: Option[Set[String]] = None): ValidatedNec[String, OrderBy] = {
    parseOrder(sort).toList match {
      case Nil => NoOrderBy.validNec
      case all => {
        all
          .map { s =>
            (validateValidValues(s, validValues), Sanitize.validate(s.sort)).mapN { case (_, _) => () }
          }
          .sequence
          .map { _ =>
            OrderBy.HasOrderBy(all.map(_.sql).mkString(", "))
          }
      }
    }
  }

  private def validateValidValues(
    sort: SortWithOrder,
    validValues: Option[Set[String]]
  ): ValidatedNec[String, Unit] = {
    validValues match {
      case None => ().validNec
      case Some(vv) if vv.contains(sort.sort) => ().validNec
      case Some(vv) => {
        s"Invalid sort key '${sort.sort}'. Must be one of: ${vv.toList.sorted.mkString(", ")}".invalidNec
      }
    }
  }

  private sealed trait SortOrder
  private object SortOrder {
    case object Ascending extends SortOrder
    case object Descending extends SortOrder
  }
  private case class SortWithOrder(sort: String, order: SortOrder) {
    def sql: String = order match {
      case SortOrder.Ascending => sort
      case SortOrder.Descending => s"$sort desc"
    }
  }
  private def parseOrder(sort: String): Seq[SortWithOrder] = {
    sort.split(",").toSeq.map(_.trim.toLowerCase).filterNot(_.isEmpty).map { w =>
      w.take(1) match {
        case "-" => SortWithOrder(w.drop(1), SortOrder.Descending)
        case _ => SortWithOrder(w, SortOrder.Ascending)
      }
    }
  }
}
