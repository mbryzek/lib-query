# lib-query

Wrapper around anorm to make it simpler to build and reuse custom SQL queries, while making
it trivial to ensure we always use Bind variables.

Dependencies are built using https://jitpack.io

## Using it

Every new version is published to the Maven repository at https://maven.bryzek.com. Add it to your
build's resolvers beside Maven Central, which keeps serving every version published before the
move:

```scala
resolvers += "Bryzek Maven repository" at "https://maven.bryzek.com"
libraryDependencies += "com.bryzek" %% "lib-query" % "<version>"
```

A version the fleet's own builds have not used for 90 days is removed from the Maven repository. A
library's newest version always stays, and nothing on Maven Central is ever removed. A build pinning
a removed version fails to resolve and moves to a newer one; a version number is never reused.

## Typed queries

`SqlColumn`, `SqlTable` and `SqlExpr` (in `com.bryzek.util`) name what a query needs from a column
or a table; play-core's generated `Column` and `Table` implement them. Every column-taking method of
`Query` has a `SqlColumn` overload that renders the column qualified (`i.status`) and binds under its
bare name (`{status}`, `{status_2}` when taken), beside the `String` method it mirrors:

```scala
Query.select(r.IssueId, Sql.count(r.Id))(r)
  .join(i, i.Id -> r.IssueId)
  .equals(i.Status, "open")
  .notExists(Query.select(o.Id)(o).sameAs(o.IssueId, r.IssueId).isNull(o.EndedAt))
  .groupBy(r.IssueId)
  .havingGreaterThan(Sql.count(r.Id), 1)
  .orderBy(OrderBy(r.IssueId.desc.nullsLast))
```

`Sql.count()`, `count`, `countDistinct`, `sum`, `max` and `min` build aggregates named
`count_id`, `max_ended_at` and so on; `.as("n")` renames one.
