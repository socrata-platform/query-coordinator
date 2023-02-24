package com.socrata.querycoordinator

class TestQueryRewriterDateTruncViaFilter extends TestQueryRewriterDateTrunc {
  override val rewrittenQueryRymd = """
    SELECT c2 as ward, coalesce(sum(c3)
    FILTER (WHERE c1 BETWEEN date_trunc_ymd('2011-02-01') AND date_trunc_ymd('2012-05-02')), 0) as count
    GROUP BY c2"""

  override val rewrittenQueryRym = """
    SELECT c2 as ward, coalesce(sum(c3)
    FILTER (WHERE c1 BETWEEN date_trunc_ym('2011-02-01') AND date_trunc_ym('2012-05-02')), 0) as count
     GROUP BY c2"""

  override val rewrittenQueryNotBetweenRym = """
    SELECT c2 as ward, coalesce(sum(c3)
    FILTER (WHERE c1 NOT BETWEEN date_trunc_ym('2011-02-01') AND date_trunc_ym('2012-05-02')), 0) as count
     GROUP BY c2"""

  override val rewrittenQueryRy = """
    SELECT c2 as ward, coalesce(sum(c3)
    FILTER (WHERE c1 BETWEEN date_trunc_y('2011-02-01') AND date_trunc_y('2012-05-02')), 0) as count
     GROUP BY c2"""

  override val qDateTruncOnLiterals = """
    SELECT ward, count(*)
    FILTER (WHERE crime_date BETWEEN '2004-01-01' AND '2014-01-01') as count
     GROUP BY ward"""

  override val qDifferingAggregations = """
    SELECT ward, count(*)
    FILTER (WHERE crime_date BETWEEN date_trunc_y('2004-01-01') AND date_trunc_ym('2014-01-01')) as count
     GROUP BY ward"""

  override val qDateTruncCannotBeMapped = """
    SELECT ward, count(*)
    FILTER (WHERE crime_date BETWEEN date_trunc_y(some_date) AND date_trunc_y('2014-01-01')) as count
     GROUP BY ward"""

  override val qBetweenDateTruncYmd = """
    SELECT ward, count(*)
    FILTER (WHERE crime_date BETWEEN date_trunc_ymd('2011-02-01') AND date_trunc_ymd('2012-05-02')) AS count
     GROUP BY ward"""

  override val qBetweenDateTruncYm = """
     SELECT ward, count(*)
     FILTER (WHERE crime_date BETWEEN date_trunc_ym('2011-02-01') AND date_trunc_ym('2012-05-02')) AS count
      GROUP BY ward"""

  override val qBetweenDateTruncY = """
    SELECT ward, count(*)
    FILTER (WHERE crime_date BETWEEN date_trunc_y('2011-02-01') AND date_trunc_y('2012-05-02')) AS count
     GROUP BY ward"""

  override val qNotBetweenDateTruncYm = """
    SELECT ward, count(*)
    FILTER (WHERE crime_date NOT BETWEEN date_trunc_ym('2011-02-01') AND date_trunc_ym('2012-05-02')) AS count
     GROUP BY ward"""
}
