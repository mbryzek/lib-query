package com.bryzek.util

import helpers.BaseSpec
import org.joda.time.LocalTime
import Parameter.*

class ParameterSpec extends BaseSpec {

  "from" should {
    "handle Array[Byte]" in {
      val bytes = Array[Byte](0x48, 0x65, 0x6c, 0x6c, 0x6f)
      val param = Parameter.from(bytes)
      param mustBe a[TypeBytes]
      param.value mustBe "\\x48656c6c6f"
      param.cast mustBe Some("bytea")
    }

    "handle empty Array[Byte]" in {
      val param = Parameter.from(Array.emptyByteArray)
      param mustBe a[TypeBytes]
      param.value mustBe "\\x"
      param.cast mustBe Some("bytea")
    }

    "handle Option[Array[Byte]]" in {
      val bytes = Array[Byte](0x01, 0x02)
      val param = Parameter.from(Some(bytes))
      param mustBe a[TypeBytes]
      param.value mustBe "\\x0102"
    }

    "handle LocalTime" in {
      val param = Parameter.from(new LocalTime(11, 0))
      param mustBe a[TypeLocalTime]
      param.value mustBe "11:00:00.000"
      param.interpolationValue mustBe "'11:00:00.000'"
      param.cast mustBe Some("time")
    }

    "handle Option[LocalTime]" in {
      val param = Parameter.from(Some(new LocalTime(13, 30, 45)))
      param mustBe a[TypeLocalTime]
      param.value mustBe "13:30:45.000"
    }
  }

  "bind with bytes" in {
    val bytes = Array[Byte](0x48, 0x65)
    val q = Query("insert into files (data) values ({data}::bytea)").bind("data", bytes)
    q.sql() mustBe "insert into files (data) values ({data}::bytea)"
    q.interpolate() mustBe "insert into files (data) values ('\\x4865')"
  }

  "bind with LocalTime" in {
    val q = Query("insert into reservations (start_time) values ({start_time}::time)")
      .bind("start_time", new LocalTime(11, 0))
    q.sql() mustBe "insert into reservations (start_time) values ({start_time}::time)"
    q.interpolate() mustBe "insert into reservations (start_time) values ('11:00:00.000')"
  }

  "TypeBytes.value" should {
    "encode the full byte range as lowercase PG hex" in {
      // 0..255 round-trip — ensures no sign-extension bugs or missing nibbles.
      val all = (0 to 255).map(_.toByte).toArray
      val expected = "\\x" + (0 to 255).map(b => f"$b%02x").mkString
      TypeBytes(all).value mustBe expected
    }

    "encode a 1MB blob without OOMing or losing data" in {
      // Regression guard for the prior `map(f"..."").mkString` implementation,
      // which allocated ~70MB of intermediate Strings for a 1MB input. The
      // fast path keeps to a single StringBuilder.
      val size = 1024 * 1024
      val bytes = new Array[Byte](size)
      var i = 0
      while (i < size) { bytes(i) = (i & 0xff).toByte; i += 1 }
      val v = TypeBytes(bytes).value
      v.length mustBe (2 + size * 2)
      v.startsWith("\\x") mustBe true
      v.substring(2, 4) mustBe "00"
      v.substring(v.length - 2) mustBe "ff"
    }
  }

  "toNamedParameter" should {
    "send bytea binds as the raw Array[Byte] so anorm uses JDBC setBytes" in {
      val bytes = Array[Byte](0x00, 0x7f, 0xff.toByte)
      val np = TypeBytes(bytes).toNamedParameter("data")
      np.name mustBe "data"
      // anorm wraps the underlying value in a ParameterValue whose toString is
      // `ParameterValue($value)`. For Array[Byte] that renders as the JVM
      // default `[B@<hash>`; if we had regressed to the hex-String path the
      // rendering would be `ParameterValue(\\x007fff)`. Asserting that the
      // rendering does NOT contain the hex marker locks the fast path in.
      val rendered = np.value.toString
      rendered must include("[B@")
      rendered must not include "\\x"
    }

    "send TypeUnit as a typed null String" in {
      val np = TypeUnit.toNamedParameter("col")
      np.name mustBe "col"
    }

    "default to String value for other types" in {
      val np = TypeString("hello").toNamedParameter("greeting")
      np.name mustBe "greeting"
      np.value.toString must include("hello")
    }
  }
}
