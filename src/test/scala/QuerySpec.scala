package io.flow.postgresql

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class QuerySpec extends AnyWordSpec with Matchers {

  "simple query" in {
    val q = Query("select 1")
    q.sql() mustBe "select 1"
    q.interpolate() mustBe "select 1"
  }

  "equals" in {
    val q = Query("select 1 from users").equals("id", 1)
    q.sql() mustBe "select 1 from users where id = {id}::integer"
    q.interpolate() mustBe "select 1 from users where id = 1"
  }

  "and" must {
    "single" in {
      val q = Query("select 1 from users").and("true").and("1=2")
      q.sql() mustBe "select 1 from users where true and 1=2"
      q.interpolate() mustBe "select 1 from users where true and 1=2"
    }

    "multi" in {
      val q = Query("select 1 from users").and(Seq("true", "1=2"))
      q.sql() mustBe "select 1 from users where true and 1=2"
      q.interpolate() mustBe "select 1 from users where true and 1=2"
    }
  }

  "optional and" must {
    "defined" in {
      val q = Query("select 1 from users").and(Some("true"))
      q.sql() mustBe "select 1 from users where true"
      q.interpolate() mustBe "select 1 from users where true"
    }

    "empty" in {
      val q = Query("select 1 from users").and(None)
      q.sql() mustBe "select 1 from users"
      q.interpolate() mustBe "select 1 from users"
    }
  }

  "or" must {
    "multi" in {
      val q = Query("select 1 from users").or(Seq("true", "1=2"))
      q.sql() mustBe "select 1 from users where (true or 1=2)"
      q.interpolate() mustBe "select 1 from users where (true or 1=2)"
    }
  }

  "multiple bind variables with same name" in {
    val q = Query("select 1 from users").equals("id", 1).equals("id", 2)
    q.sql() mustBe "select 1 from users where id = {id}::integer and id = {id_2}::integer"
    q.interpolate() mustBe "select 1 from users where id = 1 and id = 2"
  }

  "isNull" in {
    val q = Query("select 1 from users").isNull("id")
    q.sql() mustBe "select 1 from users where id is null"
    q.interpolate() mustBe "select 1 from users where id is null"
  }

  "isNotNull" in {
    val q = Query("select 1 from users").isNotNull("id")
    q.sql() mustBe "select 1 from users where id is not null"
    q.interpolate() mustBe "select 1 from users where id is not null"
  }

  "nullBoolean" must {
    "option: true" in {
      val q = Query("select 1 from users").nullBoolean("id", Some(true))
      q.sql() mustBe "select 1 from users where id is not null"
      q.interpolate() mustBe "select 1 from users where id is not null"
    }

    "option: false" in {
      val q = Query("select 1 from users").nullBoolean("id", Some(false))
      q.sql() mustBe "select 1 from users where id is null"
      q.interpolate() mustBe "select 1 from users where id is null"
    }

    "true" in {
      val q = Query("select 1 from users").nullBoolean("id", value = true)
      q.sql() mustBe "select 1 from users where id is not null"
      q.interpolate() mustBe "select 1 from users where id is not null"
    }

    "false" in {
      val q = Query("select 1 from users").nullBoolean("id", value = false)
      q.sql() mustBe "select 1 from users where id is null"
      q.interpolate() mustBe "select 1 from users where id is null"
    }
  }

  "in" must {
    "list" in {
      val q = Query("select 1 from users").in("users.id", Seq("a", "b"))
      q.sql() mustBe "select 1 from users where users.id in ({users.id}, {users.id_2})"
      q.interpolate() mustBe "select 1 from users where users.id in ('a', 'b')"
    }

    "query" in {
      val q = Query("select 1 from users").in("users.id", Query("select user_id from emails"))
      q.sql() mustBe "select 1 from users where users.id in (select user_id from emails)"
      q.interpolate() mustBe "select 1 from users where users.id in (select user_id from emails)"
    }
  }

  "optionalIn" must {
    "not specified" in {
      val q = Query("select 1 from users").optionalIn("id", None)
      q.sql() mustBe "select 1 from users"
      q.interpolate() mustBe "select 1 from users"
    }

    "specified" in {
      val q = Query("select 1 from users").optionalIn("id", Some(Seq("a", "b")))
      q.sql() mustBe "select 1 from users where id in ({id}, {id_2})"
      q.interpolate() mustBe "select 1 from users where id in ('a', 'b')"
    }
  }

  "notIn" must {
    "list" in {
      val q = Query("select 1 from users").notIn("id", Seq("a", "b"))
      q.sql() mustBe "select 1 from users where id not in ({id}, {id_2})"
      q.interpolate() mustBe "select 1 from users where id not in ('a', 'b')"
    }

    "query" in {
      val q = Query("select 1 from users").notIn("users.id", Query("select user_id from emails"))
      q.sql() mustBe "select 1 from users where users.id not in (select user_id from emails)"
      q.interpolate() mustBe "select 1 from users where users.id not in (select user_id from emails)"
    }

  }

  "optionalNotIn" must {
    "not specified" in {
      val q = Query("select 1 from users").optionalNotIn("id", None)
      q.sql() mustBe "select 1 from users"
      q.interpolate() mustBe "select 1 from users"
    }

    "specified" in {
      val q = Query("select 1 from users").optionalNotIn("id", Some(Seq("a", "b")))
      q.sql() mustBe "select 1 from users where id not in ({id}, {id_2})"
      q.interpolate() mustBe "select 1 from users where id not in ('a', 'b')"
    }
  }

  "in2" must {
    "simple" in {
      val q = Query("select 1 from users").in2(("users.id", "users.name"), Seq(("a", "mike")))
      q.sql() mustBe "select 1 from users where (users.id, users.name) in (({users.id}, {users.name}))"
      q.interpolate() mustBe "select 1 from users where (users.id, users.name) in (('a', 'mike'))"
    }

    "same column name" in {
      val q = Query("select 1 from users").in2(("id", "id"), Seq(("a", "mike")))
      q.sql() mustBe "select 1 from users where (id, id) in (({id}, {id_2}))"
      q.interpolate() mustBe "select 1 from users where (id, id) in (('a', 'mike'))"
    }
  }

  "optionalIn2" must {
    "defined" in {
      val q = Query("select 1 from users").optionalIn2(("id", "name"), Some(Seq(("a", "mike"))))
      q.sql() mustBe "select 1 from users where (id, name) in (({id}, {name}))"
      q.interpolate() mustBe "select 1 from users where (id, name) in (('a', 'mike'))"
    }

    "empty" in {
      val q = Query("select 1 from users").optionalIn2(("id", "id"), None)
      q.sql() mustBe "select 1 from users"
      q.interpolate() mustBe "select 1 from users"
    }
  }

  "in3" must {
    "simple" in {
      val q = Query("select 1 from users").in3(("id", "name", "last"), Seq(("a", "mike", "bryzek")))
      q.sql() mustBe "select 1 from users where (id, name, last) in (({id}, {name}, {last}))"
      q.interpolate() mustBe "select 1 from users where (id, name, last) in (('a', 'mike', 'bryzek'))"
    }

    "same column name" in {
      val q = Query("select 1 from users").in3(("id", "name", "name"), Seq(("a", "mike", "bryzek")))
      q.sql() mustBe "select 1 from users where (id, name, name) in (({id}, {name}, {name_2}))"
      q.interpolate() mustBe "select 1 from users where (id, name, name) in (('a', 'mike', 'bryzek'))"
    }
  }

  "optionalIn3" must {
    "defined" in {
      val q = Query("select 1 from users").optionalIn3(("id", "name", "last"), Some(Seq(("a", "mike", "bryzek"))))
      q.sql() mustBe "select 1 from users where (id, name, last) in (({id}, {name}, {last}))"
      q.interpolate() mustBe "select 1 from users where (id, name, last) in (('a', 'mike', 'bryzek'))"
    }

    "empty" in {
      val q = Query("select 1 from users").optionalIn3(("id", "name", "last"), None)
      q.sql() mustBe "select 1 from users"
      q.interpolate() mustBe "select 1 from users"
    }
  }

  "in4" must {
    "simple" in {
      val q = Query("select 1 from users").in4(("id", "name", "last", "middle"), Seq(("a", "mike", "bryzek", "maciej")))
      q.sql() mustBe "select 1 from users where (id, name, last, middle) in (({id}, {name}, {last}, {middle}))"
      q.interpolate() mustBe "select 1 from users where (id, name, last, middle) in (('a', 'mike', 'bryzek', 'maciej'))"
    }

    "same column name" in {
      val q = Query("select 1 from users").in4(("id", "name", "name", "name"), Seq(("a", "mike", "bryzek", "c")))
      q.sql() mustBe "select 1 from users where (id, name, name, name) in (({id}, {name}, {name_2}, {name_3}))"
      q.interpolate() mustBe "select 1 from users where (id, name, name, name) in (('a', 'mike', 'bryzek', 'c'))"
    }
  }

  "optionalIn4" must {
    "defined" in {
      val q = Query("select 1 from users").optionalIn4(
        ("id", "name", "last", "middle"),
        Some(Seq(("a", "mike", "bryzek", "maciej")))
      )
      q.sql() mustBe "select 1 from users where (id, name, last, middle) in (({id}, {name}, {last}, {middle}))"
      q.interpolate() mustBe "select 1 from users where (id, name, last, middle) in (('a', 'mike', 'bryzek', 'maciej'))"
    }

    "empty" in {
      val q = Query("select 1 from users").optionalIn4(("id", "name", "last", "middle"), None)
      q.sql() mustBe "select 1 from users"
      q.interpolate() mustBe "select 1 from users"
    }
  }

  "in5" must {
    "simple" in {
      val q = Query("select 1 from users").in5(
        ("id", "name", "last", "middle", "alias"),
        Seq(("a", "mike", "bryzek", "maciej", "foo"))
      )
      q.sql() mustBe "select 1 from users where (id, name, last, middle, alias) in (({id}, {name}, {last}, {middle}, {alias}))"
      q.interpolate() mustBe "select 1 from users where (id, name, last, middle, alias) in (('a', 'mike', 'bryzek', 'maciej', 'foo'))"
    }

    "same column name" in {
      val q =
        Query("select 1 from users").in5(
          ("id", "name", "name", "name", "name"),
          Seq(("a", "mike", "bryzek", "c", "foo"))
        )
      q.sql() mustBe "select 1 from users where (id, name, name, name, name) in (({id}, {name}, {name_2}, {name_3}, {name_4}))"
      q.interpolate() mustBe "select 1 from users where (id, name, name, name, name) in (('a', 'mike', 'bryzek', 'c', 'foo'))"
    }
  }

  "optionalIn5" must {
    "defined" in {
      val q = Query("select 1 from users").optionalIn5(
        ("id", "name", "last", "middle", "alias"),
        Some(Seq(("a", "mike", "bryzek", "maciej", "foo")))
      )
      q.sql() mustBe "select 1 from users where (id, name, last, middle, alias) in (({id}, {name}, {last}, {middle}, {alias}))"
      q.interpolate() mustBe "select 1 from users where (id, name, last, middle, alias) in (('a', 'mike', 'bryzek', 'maciej', 'foo'))"
    }

    "empty" in {
      val q = Query("select 1 from users").optionalIn5(("id", "name", "last", "middle", "alias"), None)
      q.sql() mustBe "select 1 from users"
      q.interpolate() mustBe "select 1 from users"
    }
  }

  "in query" must {
    "simple" in {
      val q = Query("select 1 from users").in("id", Query("select 1"))
      q.sql() mustBe "select 1 from users where id in (select 1)"
      q.interpolate() mustBe "select 1 from users where id in (select 1)"
    }

    "nested bind variable" in {
      val q = Query("select 1 from users").in("id", Query("select id from tmp").equals("tmp_id", "foo"))
      q.sql() mustBe "select 1 from users where id in (select id from tmp where tmp_id = {tmp_id})"
      q.interpolate() mustBe "select 1 from users where id in (select id from tmp where tmp_id = 'foo')"
    }

    "subquery has same bind variable as parent" in {
      val q = Query("select 1 from users").equals("id", "a").in("id", Query("select id from tmp").equals("id", "foo"))
      q.sql() mustBe "select 1 from users where id = {id} and id in (select id from tmp where id = {id_2})"
      q.interpolate() mustBe "select 1 from users where id = 'a' and id in (select id from tmp where id = 'foo')"
    }

    "subquery has statically declared bind variable" in {
      val q = Query("select 1 from users")
        .equals("id", "a")
        .in("id", Query("select id from tmp where id = {id}").bind("id", "foo"))
      q.sql() mustBe "select 1 from users where id = {id} and id in (select id from tmp where id = {id_2})"
      q.interpolate() mustBe "select 1 from users where id = 'a' and id in (select id from tmp where id = 'foo')"
    }
  }

  "greaterThan" in {
    val q = Query("select 1 from users").greaterThan("id", "1")
    q.sql() mustBe "select 1 from users where id > {id}"
    q.interpolate() mustBe "select 1 from users where id > '1'"
  }

  "greaterThanOrEquals" in {
    val q = Query("select 1 from users").greaterThanOrEquals("id", "1")
    q.sql() mustBe "select 1 from users where id >= {id}"
    q.interpolate() mustBe "select 1 from users where id >= '1'"
  }

  "lessThan" in {
    val q = Query("select 1 from users").lessThan("id", "1")
    q.sql() mustBe "select 1 from users where id < {id}"
    q.interpolate() mustBe "select 1 from users where id < '1'"
  }

  "lessThanOrEquals" in {
    val q = Query("select 1 from users").lessThanOrEquals("id", "1")
    q.sql() mustBe "select 1 from users where id <= {id}"
    q.interpolate() mustBe "select 1 from users where id <= '1'"
  }

  "limit" in {
    val q = Query("select 1 from users").limit(1)
    q.sql() mustBe "select 1 from users limit 1"
    q.interpolate() mustBe "select 1 from users limit 1"
  }

  "optionalLimit" must {
    "defined" in {
      val q = Query("select 1 from users").optionalLimit(Some(1))
      q.sql() mustBe "select 1 from users limit 1"
      q.interpolate() mustBe "select 1 from users limit 1"
    }

    "empty" in {
      val q = Query("select 1 from users").optionalLimit(None)
      q.sql() mustBe "select 1 from users"
      q.interpolate() mustBe "select 1 from users"
    }
  }

  "offset" in {
    val q = Query("select 1 from users").offset(1)
    q.sql() mustBe "select 1 from users offset 1"
    q.interpolate() mustBe "select 1 from users offset 1"
  }

  "optionalOffset" must {
    "defined" in {
      val q = Query("select 1 from users").optionalOffset(Some(1))
      q.sql() mustBe "select 1 from users offset 1"
      q.interpolate() mustBe "select 1 from users offset 1"
    }

    "empty" in {
      val q = Query("select 1 from users").optionalOffset(None)
      q.sql() mustBe "select 1 from users"
      q.interpolate() mustBe "select 1 from users"
    }
  }

  "orderBy" in {
    val q = Query("select 1 from users").orderBy("id")
    q.sql() mustBe "select 1 from users order by id"
    q.interpolate() mustBe "select 1 from users order by id"
  }

  "optionalOrderBy" must {
    "defined" in {
      val q = Query("select 1 from users").orderBy(Some("id"))
      q.sql() mustBe "select 1 from users order by id"
      q.interpolate() mustBe "select 1 from users order by id"
    }

    "empty" in {
      val q = Query("select 1 from users").orderBy(None)
      q.sql() mustBe "select 1 from users"
      q.interpolate() mustBe "select 1 from users"
    }
  }

  "groupBy" in {
    val q = Query("select 1 from users").groupBy("id")
    q.sql() mustBe "select 1 from users group by id"
    q.interpolate() mustBe "select 1 from users group by id"
  }

  "optionalgroupBy" must {
    "defined" in {
      val q = Query("select 1 from users").groupBy(Some("id"))
      q.sql() mustBe "select 1 from users group by id"
      q.interpolate() mustBe "select 1 from users group by id"
    }

    "empty" in {
      val q = Query("select 1 from users").groupBy(None)
      q.sql() mustBe "select 1 from users"
      q.interpolate() mustBe "select 1 from users"
    }
  }

  "bind variable with qualified name" in {
    val q = Query("select 1 from users").equals("users.id", 1)
    q.sql() mustBe "select 1 from users where users.id = {users.id}::integer"
    q.interpolate() mustBe "select 1 from users where users.id = 1"
  }

  "overlapping bind variables" in {
    val q = Query("select 1 from users").bind("id", 1).equals("id", 2)
    q.sql() mustBe "select 1 from users where id = {id_2}::integer"
    q.interpolate() mustBe "select 1 from users where id = 2"
  }

  "Query can be reused" in {
    val q = Query("select 1 from users where id = {id}")
    q.bind("id", 1).interpolate() mustBe "select 1 from users where id = 1"
    q.bind("id", 2).interpolate() mustBe "select 1 from users where id = 2"
  }

  "sanitizes" in {
    Try {
      Query("select 1 from users").equals("drop table users", "1")
    } match {
      case Success(_) => sys.error("Expected error")
      case Failure(ex) => ex.getMessage.contains("contains unsafe characters") mustBe true
    }
  }

  "bind" must {
    "preserve variables in base query" in {
      val q = Query("select 1 from users where last4={last4}").bind("last4", "1234")
      q.sql() mustBe "select 1 from users where last4={last4}"
      q.interpolate() mustBe "select 1 from users where last4='1234'"
    }

    "support cast" in {
      val q = Query("select 1 from users").equals("data::text", "{}")
      q.sql() mustBe "select 1 from users where data::text = {data}"
      q.interpolate() mustBe "select 1 from users where data::text = '{}'"
    }

    "support json" in {
      val q = Query("select 1 from users").equals("data->>('name')", "mike")
      q.sql() mustBe "select 1 from users where data->>('name') = {data_name}"
      q.interpolate() mustBe "select 1 from users where data->>('name') = 'mike'"
    }
  }
}
