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
