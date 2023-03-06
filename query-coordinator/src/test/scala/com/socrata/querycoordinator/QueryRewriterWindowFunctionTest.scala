package com.socrata.querycoordinator

import com.socrata.querycoordinator.QueryRewritingTestUtility._
import com.socrata.soql.types.{SoQLDate, SoQLNumber, SoQLText}
import org.scalatest.FunSuite

class QueryRewriterWindowFunctionTest extends FunSuite {

  test("Window function should rewrite when there are no clauses") {
    // Assumes a default setup, ie.. SoQLAnalyzer/AbstractParser.Parameters/Parser/QueryRewriter
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "depname_column" -> (SoQLText.t, "depname"),
          "empno_column" -> (SoQLNumber.t, "empno"),
          "salary_column" -> (SoQLNumber.t, "salary")
        )
      ),
      Map(
        "one" -> "SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) as avgSalary"
      ),
      "SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) as avgSalary",
      "select c1 as depname, c2 as empno, c3 as salary, c4 as avgSalary",
      Some("one")
    )
  }

  test("Window function should rewrite when where clauses match") {
    // Assumes a default setup, ie.. SoQLAnalyzer/AbstractParser.Parameters/Parser/QueryRewriter
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "depname_column" -> (SoQLText.t, "depname"),
          "empno_column" -> (SoQLNumber.t, "empno"),
          "salary_column" -> (SoQLNumber.t, "salary")
        )
      ),
      Map(
        "one" -> "SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) as avgSalary where salary>10"
      ),
      "SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) as avgSalary where salary>10",
      "select c1 as depname, c2 as empno, c3 as salary, c4 as avgSalary",
      Some("one")
    )
  }

  test("Mismatch of query where clause with window function should not rewrite") {
    // Assumes a default setup, ie.. SoQLAnalyzer/AbstractParser.Parameters/Parser/QueryRewriter
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "depname_column" -> (SoQLText.t, "depname"),
          "empno_column" -> (SoQLNumber.t, "empno"),
          "salary_column" -> (SoQLNumber.t, "salary")
        )
      ),
      Map(
        "one" -> "SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) as avgSalary"
      ),
      "SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) as avgSalary where salary>10",
      "SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) as avgSalary where salary>10",
      None
    )
  }

  test("Mismatch of rollup where clause with window function should not rewrite") {
    // Assumes a default setup, ie.. SoQLAnalyzer/AbstractParser.Parameters/Parser/QueryRewriter
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "depname_column" -> (SoQLText.t, "depname"),
          "empno_column" -> (SoQLNumber.t, "empno"),
          "salary_column" -> (SoQLNumber.t, "salary")
        )
      ),
      Map(
        "one" -> "SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) as avgSalary where salary>10"
      ),
      "SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) as avgSalary",
      "SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) as avgSalary",
      None
    )
  }


  test("Window function should rewrite when groupby clauses match") {
    // Assumes a default setup, ie.. SoQLAnalyzer/AbstractParser.Parameters/Parser/QueryRewriter
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "depname_column" -> (SoQLText.t, "depname"),
          "empno_column" -> (SoQLNumber.t, "empno"),
          "salary_column" -> (SoQLNumber.t, "salary")
        )
      ),
      Map(
        "one" -> "SELECT depname, empno, sum(salary) as salarySum, avg(salarySum) OVER (PARTITION BY depname) as avgSalary group by depname,empno"
      ),
      "SELECT depname, empno, sum(salary) as salarySum, avg(salarySum) OVER (PARTITION BY depname) as avgSalary group by depname,empno",
      "select c1 as depname, c2 as empno, c3 as salarySum, c4 as avgSalary",
      Some("one")
    )
  }

  test("Mismatch of groupby with window function should not rewrite") {
    // Assumes a default setup, ie.. SoQLAnalyzer/AbstractParser.Parameters/Parser/QueryRewriter
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "depname_column" -> (SoQLText.t, "depname"),
          "empno_column" -> (SoQLNumber.t, "empno"),
          "salary_column" -> (SoQLNumber.t, "salary")
        )
      ),
      Map(
        "one" -> "SELECT depname, empno, sum(salary) as salarySum, avg(salarySum) OVER (PARTITION BY depname) as avgSalary group by depname,empno"
      ),
      "SELECT depname, sum(salary) as salarySum, avg(salarySum) OVER (PARTITION BY depname) as avgSalary group by depname",
      "SELECT depname, sum(salary) as salarySum, avg(salarySum) OVER (PARTITION BY depname) as avgSalary group by depname",
      None
    )
  }


  test("Window function should rewrite when having clauses match") {
    // Assumes a default setup, ie.. SoQLAnalyzer/AbstractParser.Parameters/Parser/QueryRewriter
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "depname_column" -> (SoQLText.t, "depname"),
          "empno_column" -> (SoQLNumber.t, "empno"),
          "salary_column" -> (SoQLNumber.t, "salary")
        )
      ),
      Map(
        "one" -> "SELECT depname, empno, sum(salary) as salarySum, avg(salarySum) OVER (PARTITION BY depname) as avgSalary group by depname,empno having salarySum>10"
      ),
      "SELECT depname, empno, sum(salary) as salarySum, avg(salarySum) OVER (PARTITION BY depname) as avgSalary group by depname,empno having salarySum>10",
      "select c1 as depname, c2 as empno, c3 as salarySum, c4 as avgSalary",
      Some("one")
    )
  }

  test("Mismatch having clause with window function should not rewrite") {
    // Assumes a default setup, ie.. SoQLAnalyzer/AbstractParser.Parameters/Parser/QueryRewriter
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "depname_column" -> (SoQLText.t, "depname"),
          "empno_column" -> (SoQLNumber.t, "empno"),
          "salary_column" -> (SoQLNumber.t, "salary")
        )
      ),
      Map(
        "one" -> "SELECT depname, empno, sum(salary) as salarySum, avg(salarySum) OVER (PARTITION BY depname) as avgSalary group by depname,empno having salarySum>10"
      ),
      "SELECT depname, empno, sum(salary) as salarySum, avg(salarySum) OVER (PARTITION BY depname) as avgSalary group by depname,empno having salarySum>5",
      "SELECT depname, empno, sum(salary) as salarySum, avg(salarySum) OVER (PARTITION BY depname) as avgSalary group by depname,empno having salarySum>5",
      None
    )
  }


  test("Window function should rewrite when there are no clauses and its not top level") {
    // Assumes a default setup, ie.. SoQLAnalyzer/AbstractParser.Parameters/Parser/QueryRewriter
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "depname_column" -> (SoQLText.t, "depname"),
          "empno_column" -> (SoQLNumber.t, "empno"),
          "salary_column" -> (SoQLNumber.t, "salary")
        )
      ),
      Map(
        "one" -> "SELECT depname, empno, salary, (1 + avg(salary) OVER (PARTITION BY depname)) as avgSalary"
      ),
      "SELECT depname, empno, salary, (1 + avg(salary) OVER (PARTITION BY depname)) as avgSalary",
      "select c1 as depname, c2 as empno, c3 as salary, c4 as avgSalary",
      Some("one")
    )
  }

  test("Rewrite a query with a window function when no rollups have a window function, should not rewrite") {
    // Assumes a default setup, ie.. SoQLAnalyzer/AbstractParser.Parameters/Parser/QueryRewriter
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "depname_column" -> (SoQLText.t, "depname"),
          "empno_column" -> (SoQLNumber.t, "empno"),
          "salary_column" -> (SoQLNumber.t, "salary")
        )
      ),
      Map(
        "one" -> "SELECT depname, empno, sum(salary) as salarySum group by depname,empno"
      ),
      "SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) as avgSalary",
      "SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) as avgSalary",
      None
    )
  }

}
