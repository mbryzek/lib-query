package io.flow.postgresql

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, LocalDate}
import play.api.libs.json.{JsValue, Json}

import java.util.UUID
import scala.annotation.tailrec

sealed trait Parameter[T] {
  def original: T
  def value: String
  def interpolationValue: String
  def cast: Option[String]
}

case class BoundParameter(name: String, param: Parameter[?]) {
  def bindFragment: String = {
    val cast = param.cast match {
      case None => ""
      case Some(c) => s"::$c"
    }
    s"{${name}}$cast"
  }
}

object Parameter {
  def from[T](instances: Seq[T]): Seq[Parameter[?]] = instances.map(from(_))

  @tailrec
  final def from[T](instance: T): Parameter[?] = {
    instance match {
      case i: BigDecimal => TypeBigDecimal(i)
      case i: Int => TypeInt(i)
      case i: Long => TypeLong(i)
      case i: Number => TypeNumber(i)
      case i: UUID => TypeUUID(i)
      case i: LocalDate => TypeLocalDate(i)
      case i: DateTime => TypeDateTime(i)
      case i: String => TypeString(i)
      case i: Boolean => TypeBoolean(i)
      case i: JsValue => TypeJsValue(i)
      case None => TypeUnit
      case Some(v) => from(v)
      case other => sys.error(s"Cannot convert instance of type[${other.getClass.getName}] to Parameter")
    }

  }

  case class TypeBigDecimal(override val original: BigDecimal) extends Parameter[BigDecimal] {
    override def value: String = original.toString()
    override def interpolationValue: String = value
    override def cast: Option[String] = Some("decimal")
  }
  case class TypeInt(override val original: Int) extends Parameter[Int] {
    override def value: String = original.toString
    override def interpolationValue: String = value
    override def cast: Option[String] = Some("integer")
  }
  case class TypeLong(override val original: Long) extends Parameter[Long] {
    override def value: String = original.toString
    override def interpolationValue: String = value
    override def cast: Option[String] = Some("long")
  }
  case class TypeNumber(override val original: Number) extends Parameter[Number] {
    override def value: String = original.toString
    override def interpolationValue: String = value
    override def cast: Option[String] = Some("numeric")
  }
  case class TypeUUID(override val original: UUID) extends Parameter[UUID] {
    override def value: String = original.toString
    override def interpolationValue: String = s"'$value'"
    override def cast: Option[String] = Some("uuid")
  }
  case class TypeLocalDate(override val original: LocalDate) extends Parameter[LocalDate] {
    override def value: String = ISODateTimeFormat.date().print(original)
    override def interpolationValue: String = s"'$value'"
    override def cast: Option[String] = Some("date")
  }
  case class TypeDateTime(override val original: DateTime) extends Parameter[DateTime] {
    override def value: String = ISODateTimeFormat.dateTime().print(original)
    override def interpolationValue: String = s"'$value'"
    override def cast: Option[String] = Some("timestamptz")
  }
  case class TypeString(override val original: String) extends Parameter[String] {
    override def value: String = original
    override def interpolationValue: String = s"'$value'"
    override def cast: Option[String] = None
  }
  case class TypeBoolean(override val original: Boolean) extends Parameter[Boolean] {
    override def value: String = original.toString
    override def interpolationValue: String = value
    override def cast: Option[String] = Some("boolean")
  }
  case class TypeJsValue(override val original: JsValue) extends Parameter[JsValue] {
    override def value: String = Json.stringify(original)
    override def interpolationValue: String = s"'$value'"
    override def cast: Option[String] = Some("json")
  }
  case object TypeUnit extends Parameter[Unit] {
    override val original: Unit = ()
    override def value: String = "null"
    override def interpolationValue: String = value
    override def cast: Option[String] = None
  }
}
