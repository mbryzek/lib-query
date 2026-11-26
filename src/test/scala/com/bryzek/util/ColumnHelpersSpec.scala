package com.bryzek.util

import helpers.BaseSpec

class ColumnHelpersSpec extends BaseSpec {

  private case class Col(qualifier: String, name: String) extends SqlColumn {
    override def sql: String = s"$qualifier.$name"
  }

  private enum Status {
    case Fixed, Deployed
    override def toString: String = this match {
      case Fixed => "fixed"
      case Deployed => "deployed"
    }
  }

  private val Id = Col("i", "id")
  private val State = Col("i", "status")
  private val Count = Col("i", "count")

  "is and isNot quote a value and leave a number as it is" in {
    State.is(Status.Fixed) mustBe "i.status = 'fixed'"
    State.isNot("open") mustBe "i.status != 'open'"
    Count.is(3) mustBe "i.count = 3"
    Count.is(3L) mustBe "i.count = 3"
    Count.isNot(true) mustBe "i.count != true"
  }

  "a quote inside a literal is doubled" in {
    State.is("it's") mustBe "i.status = 'it''s'"
  }

  "in and notIn render a literal list" in {
    State.in(Status.Fixed, Status.Deployed) mustBe "i.status in ('fixed', 'deployed')"
    State.notIn(Seq(Status.Fixed)*) mustBe "i.status not in ('fixed')"
    Count.in(1, 2) mustBe "i.count in (1, 2)"
  }

  "an empty in is false and an empty notIn is true" in {
    State.in() mustBe "false"
    State.notIn() mustBe "true"
  }

  "at renders the column under another qualifier and keeps its bare name" in {
    val k = State.at("k")
    k.sql mustBe "k.status"
    k.name mustBe "status"
    s"$k" mustBe "k.status"
    k.is("open") mustBe "k.status = 'open'"
  }

  "at refuses a qualifier that is not safe" in {
    an[RuntimeException] must be thrownBy State.at("k; drop")
  }

  "names and binds render bare names for an insert" in {
    Sql.names(Id, State, Sql.count(Id)) mustBe "id, status, count_id"
    Sql.binds(Id, State) mustBe "{id}, {status}"
    Sql.names() mustBe ""
  }

  "SqlParsers build a parser per entry" in {
    val parser = SqlParsers.str(Id) ~ SqlParsers.long(Sql.count(Id)) ~ SqlParsers.get[Option[String]](State.at("k")) ~
      SqlParsers.bool(State) ~ SqlParsers.int(Count) ~ SqlParsers.double(Count)
    parser must not be null
  }
}
