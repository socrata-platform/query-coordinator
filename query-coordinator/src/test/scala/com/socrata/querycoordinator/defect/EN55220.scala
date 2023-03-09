package com.socrata.querycoordinator.defect

import com.socrata.querycoordinator.QueryRewritingTestUtility.AssertRewriteDefault
import com.socrata.soql.types.{SoQLBoolean, SoQLFloatingTimestamp, SoQLNumber, SoQLText}
import org.scalatest.FunSuite

//https://socrata.atlassian.net/browse/EN-55220
class EN55220 extends FunSuite{
  test("3 year filing query rewrites to rollup") {
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "rowkey_column" -> (SoQLNumber.t, "rowkey"),
          "efspname_column" -> (SoQLText.t, "efspname"),
          "county_column" -> (SoQLText.t, "county"),
          "office_column" -> (SoQLText.t, "office"),
          "orgchartname_column" -> (SoQLText.t, "orgchartname"),
          "nodeid_column" -> (SoQLNumber.t, "nodeid"),
          "casedataid_column" -> (SoQLNumber.t, "casedataid"),
          "casenumber_column" -> (SoQLText.t, "casenumber"),
          "envelopeid_column" -> (SoQLNumber.t, "envelopeid"),
          "isinitial_column" -> (SoQLBoolean.t, "isinitial"),
          "filingid_column" -> (SoQLNumber.t, "filingid"),
          "filingtype_column" -> (SoQLText.t, "filingtype"),
          "submitted_column" -> (SoQLFloatingTimestamp.t, "submitted"),
          "submittedday_column" -> (SoQLText.t, "submittedday"),
          "filingstate_column" -> (SoQLText.t, "filingstate"),
          "casecategorycode_column" -> (SoQLText.t, "casecategorycode"),
          "casetypecode_column" -> (SoQLText.t, "casetypecode"),
          "filingcode_column" -> (SoQLText.t, "filingcode"),
          "isindividualfirm_column" -> (SoQLBoolean.t, "isindividualfirm"),
          "username_column" -> (SoQLText.t, "username"),
          "firmid_column" -> (SoQLNumber.t, "firmid"),
          "firmname_column" -> (SoQLText.t, "firmname"),
          "rejectionreason_column" -> (SoQLText.t, "rejectionreason"),
          "numberofdocuments_column" -> (SoQLNumber.t, "numberofdocuments"),
          "underreview_column" -> (SoQLFloatingTimestamp.t, "underreview"),
          "reviewed_column" -> (SoQLFloatingTimestamp.t, "reviewed"),
          "primaryamount_column" -> (SoQLNumber.t, "primaryamount"),
          "secondaryamount_column" -> (SoQLNumber.t, "secondaryamount"),
          "systempaymentaccounttype_column" -> (SoQLText.t, "systempaymentaccounttype"),
          "clerkname_column" -> (SoQLText.t, "clerkname"),
          "adjustedsubmitdate_column" -> (SoQLFloatingTimestamp.t, "adjustedsubmitdate"),
          "clerkresponsetime_column" -> (SoQLNumber.t, "clerkresponsetime"),
          "insertedby_column" -> (SoQLText.t, "insertedby"),
          "district_column" -> (SoQLText.t, "district"),
          "circuit_column" -> (SoQLText.t, "circuit"),

        )
      ),
      Map(
        "one" -> "select casetypecode,orgchartname,office,county,rejectionreason,casecategorycode,efspname,filingstate,lower ( filingstate ) like '%rejected%',filingstate = 'RejectedByCourt' and isindividualfirm = false,filingstate = 'RejectedByCourt' and isindividualfirm = true,filingstate = 'RejectedByCourt',isindividualfirm = true,isindividualfirm = false,date_trunc_ymd(submitted),date_trunc_ym(submitted),date_trunc_y(submitted),date_extract_woy(submitted),min(submitted),max(submitted),count(*),count(filingid),(sum(case(filingstate = 'RejectedByCourt',1,true,0))/ nullif(case(sum(case(filingstate in ('AcceptedByCourt','RejectedByCourt'),1,true,0))=0,1,true,sum(case(filingstate in ('AcceptedByCourt','RejectedByCourt'),1,true,0))),0)*100),(SUM(CASE(isindividualfirm=true AND filingstate='RejectedByCourt',1,TRUE,0))/ nullif(SUM(CASE(filingstate='AcceptedByCourt' OR filingstate='RejectedByCourt',1,TRUE,0)),0)*100),(sum(case(isindividualfirm = false,1,true,0))/ nullif(count(filingid),0)*100),(sum(case(isindividualfirm = true,1,true,0))/ nullif(count(filingid),0)*100) group by casetypecode,orgchartname,office,county,rejectionreason,casecategorycode,efspname,filingstate,lower ( filingstate ) like '%rejected%',filingstate = 'RejectedByCourt' and isindividualfirm = false,filingstate = 'RejectedByCourt' and isindividualfirm = true,filingstate = 'RejectedByCourt',isindividualfirm = true,isindividualfirm = false,date_trunc_ymd(submitted),date_trunc_ym(submitted),date_trunc_y(submitted),date_extract_woy(submitted)"
      ),
      "SELECT date_trunc_y(submitted) as period__alias, (SUM(CASE(isindividualfirm=true AND filingstate='RejectedByCourt',1,TRUE,0))/ nullif(SUM(CASE(filingstate='AcceptedByCourt' OR filingstate='RejectedByCourt',1,TRUE,0)),0)*100) as value__alias WHERE (submitted >= '2020-01-27') AND (submitted < '2022-08-09') GROUP BY period__alias ORDER BY period__alias ASC LIMIT 10000",
      "SELECT c17 as period__alias,(((sum((case WHEN (c13 and c12) THEN 1 WHEN true THEN 0 end))) / (nullif((sum((case WHEN ((c8 = 'AcceptedByCourt') or c12) THEN 1 WHEN true THEN 0 end))),0))) * 100) as value__alias WHERE ((c15 >= '2020-01-27') and (c15 < '2022-08-09')) GROUP BY c17 ORDER BY c17 nulls last LIMIT 10000",
      Some("one")
    )
  }
}
