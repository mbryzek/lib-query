package com.bryzek.util

import anorm.NamedParameter
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, LocalDate, LocalTime}
import play.api.libs.json.{JsValue, Json}

import java.util.UUID
import scala.annotation.tailrec

sealed trait Parameter[T] {
  def original: T
  def value: String
  def interpolationValue: String
  def cast: Option[String]

  /** Build the anorm bind for this parameter. Default sends [[value]] as a String (the legacy behavior);
    * [[Parameter.TypeBytes]] overrides to pass the raw `Array[Byte]` so anorm uses JDBC `setBytes` instead of forcing a
    * hex-encoded String through the wire. Saves O(N) intermediate String allocation per bytea bind — material on
    * multi-MB blobs.
    */
  def toNamedParameter(name: String): NamedParameter = NamedParameter(name, value)
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
      case i: LocalTime => TypeLocalTime(i)
      case i: DateTime => TypeDateTime(i)
      case i: String => TypeString(i)
      case i: Boolean => TypeBoolean(i)
      case i: JsValue => TypeJsValue(i)
      case i: Array[Byte] => TypeBytes(i)
      case None => TypeUnit
      case Some(v) => from(v)
      case i: Product => TypeString(i.toString)
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
    override def cast: Option[String] = Some("bigint")
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
  case class TypeLocalTime(override val original: LocalTime) extends Parameter[LocalTime] {
    override def value: String = original.toString("HH:mm:ss.SSS")
    override def interpolationValue: String = s"'$value'"
    override def cast: Option[String] = Some("time")
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
  case class TypeBytes(override val original: Array[Byte]) extends Parameter[Array[Byte]] {
    // PostgreSQL bytea hex literal format. Fast path: a single StringBuilder
    // sized to the exact output length, with a direct 4-bit nibble→ASCII lookup
    // per byte. Avoids the per-byte Formatter/String allocation of the prior
    // `map(b => f"...").mkString` form, which produced ~70 MB of garbage per
    // 1 MB of input.
    override def value: String = {
      val len = original.length
      val sb = new java.lang.StringBuilder(2 + len * 2)
      sb.append("\\x")
      var i = 0
      while (i < len) {
        val b = original(i) & 0xff
        sb.append(TypeBytes.HexChars(b >>> 4))
        sb.append(TypeBytes.HexChars(b & 0xf))
        i += 1
      }
      sb.toString
    }
    override def interpolationValue: String = s"'$value'"
    override def cast: Option[String] = Some("bytea")
    // Skip the hex String entirely on the bind path — anorm's
    // `byteArrayToStatement` calls `PreparedStatement.setBytes` and PostgreSQL
    // accepts the binary value with the redundant `::bytea` cast as a no-op.
    override def toNamedParameter(name: String): NamedParameter = NamedParameter(name, original)
  }
  object TypeBytes {
    private val HexChars: Array[Char] = "0123456789abcdef".toCharArray
  }
  case object TypeUnit extends Parameter[Unit] {
    override val original: Unit = ()
    override def value: String = "null"
    override def interpolationValue: String = value
    override def cast: Option[String] = None
    // Bind as a typed-null String so PostgreSQL has a definite SQL type.
    override def toNamedParameter(name: String): NamedParameter = NamedParameter(name, Option.empty[String])
  }
}
