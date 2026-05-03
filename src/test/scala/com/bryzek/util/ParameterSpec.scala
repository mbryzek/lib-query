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
}
