#!/usr/bin/env bash
#
# lib-query's build (ISS-2173). READ devops/templates/ci/build.sh AND
# devops/docs/ci.md FIRST: landing this file IS the enrolment and
# `set -euo pipefail` is not style. This is a FAST repository: the fleet never
# builds it, and the session runs this script on the pull request's head at
# `dev agent pr ready`, which records the result and merges on green.
#
# WHAT A GREEN HERE VOUCHES FOR: this library's own sources compile under
# `-Werror` with the full `-Wunused` set and its own ScalaTest suite passes, on
# the exact commit about to be squashed.
#
# WHAT IT DOES NOT VOUCH FOR, and this is the important half: THAT ANY CONSUMER
# STILL COMPILES. Nothing in the fleet builds `platform` or `acumen` against a
# lib PR's tree. A consumer resolves a RELEASED version out of Maven Central, and
# no released version exists until the library's rollout
# (`dev rollout --app lib-query`) tags main and a runner publishes it, so a merge
# here publishes nothing. So a PR that changes a rendered SQL fragment is green
# here and can still break a consumer at the moment somebody bumps the pin. That
# break surfaces in the CONSUMER's own version-bump PR, verified by the
# consumer's own build, which is where the rollout and `Agent_library_pin`
# already put it. Enrolling this repo
# narrows nothing about that and is not meant to.
#
# NO DATABASE BLOCK, AND THAT IS A CLAIM WORTH CHECKING RATHER THAN A DEFAULT,
# because this is the one library that depends on `anorm-postgres`. It is a
# query BUILDER: every test in `src/test` asserts on the SQL string and the bind
# parameters a `Query` renders, and not one of them opens a connection. Copying
# the database block from devops/templates/ci/build-scala.sh would start a
# container, hold a host port from the range `dev db session` allocates out of,
# and depend on the DigitalOcean registry credential — three ways to go red that
# have nothing to do with the code under test, and the session-database port
# range is the single biggest producer of false red in the fleet (ISS-2178). If a
# test here ever does reach a real Postgres, this file has to grow the block back;
# every runner already carries docker, the registry credential and a database.
#
# FORMATTING IS REPORTED, NEVER ENFORCED (ISS-19427). A build fails only for a
# functional problem, and an unformatted file behaves exactly as its formatted
# copy does. `scalafmtOnCompile := true` formats the sources, and
# `dev ci step check-format` (the devops release's format check, which runs
# `dev fmt report`: the native scalafmt CLI over the sources AND `build.sbt`,
# which nothing else formats) prints every unformatted file and exits 0. The
# hygiene sweep runs it over `main` and fixes what it finds.
#
# `-Xmx4G` in `.jvmopts` — MEASURED, not copied (ISS-1171's method, run
# 2026-08-11). Cold checkout, `SBT_OPTS="-Xmx4G -Xss4M"`, process-tree RSS
# sampled with `/bin/ps -axo pid=,ppid=,rss=`: peak 1.3 GB, exit 0, 127 tests
# passed. 4G is `Agent::Heap::MIN_GB` — the least any runner hands a build, so
# nothing below it is worth measuring. The file is here rather than omitted
# because an omission is indistinguishable from nobody having looked.
# `Test / fork` is false in this build, so `SBT_OPTS` is the whole ceiling and
# there is no forked test JVM outside it to pin.
#
# TEST HEALTH READS THE NEXT TWO LINES (ISS-19887, design DES-18577 "Every repository
# with CI"). The first is where sbt leaves a JUnit report per suite, which is what the
# fleet writes the test ledger from: `TEST-<suite>.xml` is the build's run of a suite,
# `FIRST-<suite>.xml` the first run of one the build reran. The second says this build
# runs with the lists platform holds: whatever starts it fetches them first and exports
# the file as `CI_TEST_LISTS`. The runner half is devops templates/sbt-test-health/ci/test-health.sh,
# sourced from the devops release below and never copied, and lib-build-sbt's CiTestListsPlugin and
# CiTestRerunPlugin (project/plugins.sbt).
#
# ci-test-reports: target/test-reports/*.xml
# ci-test-lists: required
set -euo pipefail

echo "building ${CI_REPO:-?} @ ${CI_SHA:-?} (${CI_EVENT:-?}, clean=${CI_CLEAN_BUILD:-?})"

# ci-advisory: scripts/check-format
dev ci step check-format || echo "check-format: could not measure this tree — advisory, nothing fails on it"

# THE SUITE, WITH TEST HEALTH (ISS-19887). `ci_test_health_sbt` prints the `ci-lists:` line for
# the lists `CI_TEST_LISTS` names (exiting 75 on a file that is not one, or on `CI_SLOW_SPECS=1`
# with none), and `ciTest` runs every spec the lists leave in -- the slow ones alone under
# `CI_SLOW_SPECS=1` -- then reruns each spec that failed once, printing `ci-flake:` per spec that
# passed the second time and `ci-rerun: done`. A spec that fails twice fails the build.
ci_th=$(dev ci template sbt-test-health/ci/test-health.sh) || exit 75
source "$ci_th"
ci_test_health_sbt
sbt -Dsbt.log.noformat=true "${ci_sbt_test_commands[@]}"
