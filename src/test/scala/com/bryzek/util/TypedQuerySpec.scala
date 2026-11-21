package com.bryzek.util

import helpers.{BaseSpec, RecordingConnection}

class TypedQuerySpec extends BaseSpec {

  private case class Col(qualifier: String, name: String) extends SqlColumn {
    override def sql: String = s"$qualifier.$name"
  }

  private case class Tbl(sql: String) extends SqlTable

  private object r {
    val table: Tbl = Tbl("assistant.runs r")
    val Id: Col = Col("r", "id")
    val IssueId: Col = Col("r", "issue_id")
    val TenantId: Col = Col("r", "tenant_id")
    val Status: Col = Col("r", "status")
    val EndedAt: Col = Col("r", "ended_at")
  }

  private object o {
    val table: Tbl = Tbl("assistant.runs o")
    val Id: Col = Col("o", "id")
    val IssueId: Col = Col("o", "issue_id")
    val EndedAt: Col = Col("o", "ended_at")
    val Status: Col = Col("o", "status")
  }

  private object i {
    val table: Tbl = Tbl("issues.issues i")
    val Id: Col = Col("i", "id")
    val Status: Col = Col("i", "status")
  }

  "SqlColumn comparisons render the column qualified and bind its bare name" in {
    val q = Query("select 1 from issues.issues i")
      .equals(i.Status, "open")
      .notEquals(i.Id, "x")
      .greaterThan(i.Id, "a")
      .greaterThanOrEquals(i.Id, "b")
      .lessThan(i.Id, "c")
      .lessThanOrEquals(i.Id, "d")
    q.sql() mustBe
      "select 1 from issues.issues i where i.status = {status} and i.id != {id} and i.id > {id_2}" +
      " and i.id >= {id_3} and i.id < {id_4} and i.id <= {id_5}"
    q.interpolate() mustBe
      "select 1 from issues.issues i where i.status = 'open' and i.id != 'x' and i.id > 'a'" +
      " and i.id >= 'b' and i.id < 'c' and i.id <= 'd'"
  }

  "SqlColumn equals with None adds nothing and with Some binds the value" in {
    Query("select 1").equals(i.Status, None).sql() mustBe "select 1"
    Query("select 1").equals(i.Status, Some("open")).sql() mustBe "select 1 where i.status = {status}"
  }

  "two columns of one name from different tables bind under distinct names" in {
    val q = Query("select 1").equals(r.Status, "failed").equals(i.Status, "open")
    q.sql() mustBe "select 1 where r.status = {status} and i.status = {status_2}"
    RecordingConnection.fill(q) mustBe Seq("failed", "open")
  }

  "null checks" in {
    Query("select 1").isNull(o.EndedAt).isNotNull(r.EndedAt).sql() mustBe
      "select 1 where o.ended_at is null and r.ended_at is not null"
    Query("select 1").nullBoolean(o.EndedAt, true).nullBoolean(r.EndedAt, Some(false)).sql() mustBe
      "select 1 where o.ended_at is not null and r.ended_at is null"
    Query("select 1").nullBoolean(o.EndedAt, None).sql() mustBe "select 1"
  }

  "in and notIn over values bind each value under the bare name" in {
    val q = Query("select 1").in(i.Id, Seq("a", "b")).notIn(i.Status, Seq("closed"))
    q.sql() mustBe "select 1 where i.id in ({id}, {id_2}) and i.status not in ({status})"
    RecordingConnection.fill(q) mustBe Seq("a", "b", "closed")
    Query("select 1").in(i.Id, Nil).sql() mustBe "select 1 where false"
    Query("select 1").notIn(i.Id, Nil).sql() mustBe "select 1 where true"
    Query("select 1").optionalIn(i.Id, None).optionalNotIn(i.Id, None).sql() mustBe "select 1"
    Query("select 1").optionalIn(i.Id, Some(Seq("a"))).sql() mustBe "select 1 where i.id in ({id})"
  }

  "in and notIn over a subquery" in {
    val sub = Query.select(o.IssueId)(o.table).equals(o.Status, "running")
    val q = Query("select 1").equals(i.Status, "open").in(i.Id, sub).notIn(r.IssueId, sub)
    q.sql() mustBe
      "select 1 where i.status = {status} and i.id in (select o.issue_id from assistant.runs o where o.status = {status_2})" +
      " and r.issue_id not in (select o.issue_id from assistant.runs o where o.status = {status_3})"
    RecordingConnection.fill(q) mustBe Seq("open", "running", "running")
  }

  "in2 to in5 bind under bare names" in {
    val q = Query("select 1").in2((r.Id, r.IssueId), Seq(("a", "b")))
    q.sql() mustBe "select 1 where (r.id, r.issue_id) in (values ({id}, {issue_id}))"
    Query("select 1").optionalIn2((r.Id, r.IssueId), None).sql() mustBe "select 1"
    Query("select 1").in3((r.Id, r.IssueId, r.Status), Seq(("a", "b", "c"))).sql() mustBe
      "select 1 where (r.id, r.issue_id, r.status) in (values ({id}, {issue_id}, {status}))"
    Query("select 1").in4((r.Id, r.IssueId, r.Status, r.TenantId), Seq(("a", "b", "c", "d"))).sql() mustBe
      "select 1 where (r.id, r.issue_id, r.status, r.tenant_id) in (values ({id}, {issue_id}, {status}, {tenant_id}))"
    Query("select 1")
      .in5((r.Id, r.IssueId, r.Status, r.TenantId, r.EndedAt), Seq(("a", "b", "c", "d", "e")))
      .sql() mustBe
      "select 1 where (r.id, r.issue_id, r.status, r.tenant_id, r.ended_at)" +
      " in (values ({id}, {issue_id}, {status}, {tenant_id}, {ended_at}))"
    Query("select 1").in2((r.Id, r.IssueId), Nil).sql() must include("where false")
  }

  "greaterThan2 over columns" in {
    val q = Query("select 1").greaterThan2((r.EndedAt, r.Id), ("t", "x"))
    q.sql() mustBe "select 1 where (r.ended_at, r.id) > ({ended_at}, {id})"
    an[RuntimeException] must be thrownBy Query("select 1").greaterThan2((r.EndedAt, r.Id), (None, "x"))
  }

  "sameAs compares two columns" in {
    Query("select 1").sameAs(o.IssueId, r.IssueId).sql() mustBe "select 1 where o.issue_id = r.issue_id"
  }

  "exists and notExists absorb the subquery's bindings" in {
    val sub = Query.select(o.Id)(o.table).sameAs(o.IssueId, r.IssueId).equals(o.Status, "running")
    val q = Query.select(r.IssueId)(r.table).equals(r.Status, "failed").notExists(sub).exists(sub)
    q.sql() mustBe
      "select r.issue_id from assistant.runs r where r.status = {status}" +
      " and not exists (select o.id from assistant.runs o where o.issue_id = r.issue_id and o.status = {status_2})" +
      " and exists (select o.id from assistant.runs o where o.issue_id = r.issue_id and o.status = {status_3})"
    RecordingConnection.fill(q) mustBe Seq("failed", "running", "running")
  }

  "exists over a subquery ending in a comment keeps its closing paren" in {
    val sub = Query.select(o.Id)(o.table).matchNoRows(Some("nothing"))
    Query("select 1").exists(sub).sql() must endWith("-- nothing\n)")
  }

  "join and leftJoin render between the base and the where" in {
    val q = Query
      .select(r.IssueId)(r.table)
      .join(i.table, i.Id -> r.IssueId)
      .leftJoin(o.table, o.IssueId -> r.IssueId, o.Status -> r.Status)
      .equals(i.Status, "open")
    q.sql() mustBe
      "select r.issue_id from assistant.runs r join issues.issues i on i.id = r.issue_id" +
      " left join assistant.runs o on o.issue_id = r.issue_id and o.status = r.status where i.status = {status}"
  }

  "a join with no columns is refused" in {
    an[IllegalArgumentException] must be thrownBy Query.select(r.Id)(r.table).join(i.table)
  }

  "a joined subquery keeps its join when absorbed" in {
    val sub = Query.select(o.Id)(o.table).join(i.table, i.Id -> o.IssueId).equals(i.Status, "open")
    Query("select 1").exists(sub).sql() mustBe
      "select 1 where exists (select o.id from assistant.runs o join issues.issues i on i.id = o.issue_id" +
      " where i.status = {status})"
  }

  "select renders columns and aliased aggregates" in {
    Query.select(r.IssueId, Sql.count().as("n"))(r.table).groupBy(r.IssueId).sql() mustBe
      "select r.issue_id, count(*) as n from assistant.runs r group by r.issue_id"
    Query
      .select(
        r.Status,
        r.TenantId,
        Sql.count(r.Id),
        Sql.countDistinct(r.IssueId),
        Sql.sum(r.Id),
        Sql.max(r.EndedAt),
        Sql.min(r.EndedAt)
      )(r.table)
      .groupBy(r.Status, r.TenantId)
      .sql() mustBe
      "select r.status, r.tenant_id, count(r.id) as count_id, count(distinct r.issue_id) as count_distinct_issue_id," +
      " sum(r.id) as sum_id, max(r.ended_at) as max_ended_at, min(r.ended_at) as min_ended_at" +
      " from assistant.runs r group by r.status, r.tenant_id"
  }

  "an unsafe column or table is refused" in {
    an[RuntimeException] must be thrownBy Query("select 1").equals(Col("i", "id; drop"), 1)
    an[RuntimeException] must be thrownBy Query.select(r.Id)(Tbl("x; drop table y"))
    an[RuntimeException] must be thrownBy Sql.count(Col("i", "id;"))
  }

  "having over an aggregate binds under its name and ands onto an existing having" in {
    val q = Query
      .select(r.IssueId, Sql.count(r.Id))(r.table)
      .groupBy(r.IssueId)
      .havingGreaterThan(Sql.count(r.Id), 1)
      .havingLessThanOrEquals(Sql.count(r.Id), 5)
    q.sql() mustBe
      "select r.issue_id, count(r.id) as count_id from assistant.runs r group by r.issue_id" +
      " having count(r.id) > {count_id}::integer and count(r.id) <= {count_id_2}::integer"
    q.interpolate() must endWith("having count(r.id) > 1 and count(r.id) <= 5")
    Query("select 1").havingEquals(Sql.count(), None).sql() mustBe "select 1"
    Query("select 1").having("true").havingEquals(Sql.max(r.EndedAt).as("latest"), "t").sql() mustBe
      "select 1 having true and max(r.ended_at) = {latest}"
    Query("select 1").havingNotEquals(Sql.count(), 1).sql() must include("count(*) != {count}")
    Query("select 1").havingGreaterThanOrEquals(Sql.count(), 1).sql() must include("count(*) >= {count}")
    Query("select 1").havingLessThan(Sql.count(), 1).sql() must include("count(*) < {count}")
  }

  "OrderBy from constants" in {
    val ob = OrderBy(r.EndedAt.desc.nullsLast, r.Id.desc, i.Status.asc.nullsFirst, Sql.count().desc)
    ob.sql mustBe Some("r.ended_at desc nulls last, r.id desc, i.status nulls first, count(*) desc")
    Query("select 1").orderBy(OrderBy(r.Id.asc)).limit(1).sql() mustBe "select 1 order by r.id limit 1"
    Query("select 1").orderBy(OrderBy(None)).sql() mustBe "select 1"
    an[RuntimeException] must be thrownBy Col("i", "id desc; drop").asc
  }

  "the S3 example of the design renders as written" in {
    val q = Query
      .select(r.IssueId)(r.table)
      .join(i.table, i.Id -> r.IssueId)
      .equals(r.TenantId, "t1")
      .equals(r.Status, "failed")
      .equals(i.Status, "open")
      .notExists(Query.select(o.Id)(o.table).sameAs(o.IssueId, r.IssueId).isNull(o.EndedAt))
      .orderBy(OrderBy(r.EndedAt.desc.nullsLast, r.Id.desc))
      .limit(1)
    q.sql() mustBe
      "select r.issue_id from assistant.runs r join issues.issues i on i.id = r.issue_id" +
      " where r.tenant_id = {tenant_id} and r.status = {status} and i.status = {status_2}" +
      " and not exists (select o.id from assistant.runs o where o.issue_id = r.issue_id and o.ended_at is null)" +
      " order by r.ended_at desc nulls last, r.id desc limit 1"
    RecordingConnection.fill(q) mustBe Seq("t1", "failed", "open")
  }
}
