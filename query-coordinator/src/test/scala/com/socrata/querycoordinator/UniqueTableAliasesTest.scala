package com.socrata.querycoordinator

import com.socrata.soql.parsing.Parser


class UniqueTableAliasesTest extends TestBase {

  test("table aliases renamed so that analyze do not collide") {
    val q = """
            SELECT aa |>
            SELECT @b.ee, max(aa) as max_aa, max(@b.dd) as max_dd JOIN @bbbb-bbbb as b ON aa=@b.aa group by @b.ee |>
            SELECT @b.aa, max_aa, @b.ff JOIN @cccc-cccc as b ON aa=@b.aa ORDER BY @b.ff"""
    val parsed0 = new Parser().selectStatement(q)


    val expected = Seq(
      "SELECT `aa`",
      "SELECT @b.`ee`, max(`aa`) AS max_aa, max(@b.`dd`) AS max_dd JOIN @bbbb-bbbb AS b ON `aa` = @b.`aa` GROUP BY @b.`ee`",
      "SELECT @b__2.`aa`, `max_aa`, @b__2.`ff` JOIN @cccc-cccc AS b__2 ON `aa` = @b__2.`aa` ORDER BY @b__2.`ff` ASC NULL LAST")

    val parsed = UniqueTableAliases(parsed0)

    expected.zip(parsed.map(_.toString).seq).foreach {
      case (expectedSql, actualSql) =>
        actualSql shouldBe(expectedSql)
    }
  }
}
