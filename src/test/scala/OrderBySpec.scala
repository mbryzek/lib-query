package io.flow.postgresql

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}
import helpers.BaseSpec

class OrderBySpec extends BaseSpec {

  "parse" must {
    "empty" in {
      OrderBy.parse("")
    }
  }

}
