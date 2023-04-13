package com.socrata.querycoordinator.defect

import com.socrata.querycoordinator.QueryRewritingTestUtility.AssertRewriteDefault
import com.socrata.soql.types.{SoQLBoolean, SoQLDate, SoQLFloatingTimestamp, SoQLNumber, SoQLText}
import org.scalatest.FunSuite

//https://socrata.atlassian.net/browse/EN-58705

//  Rollup query cannot be merged into a leaf
//  Query to rollup exact match not implemented
//
class EN58705 extends FunSuite{
  ignore("example one") {
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "rowkey_column" -> (SoQLNumber.t, "rowkey"),
          "efspname_column" -> (SoQLText.t, "efspname"),
          "county_column" -> (SoQLText.t, "county"),
          "office_column" -> (SoQLText.t, "office"),
          "orgchartname_column" -> (SoQLText.t, "orgchartname"),
          "nodeid_column" -> (SoQLNumber.t, "nodeid"),
          "casedataid_column" -> (SoQLText.t, "casedataid"),
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
          "isindividualfirm_column" -> (SoQLText.t, "isindividualfirm"),
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
          "circuit_column" -> (SoQLText.t, "circuit")
        )
      ),
      Map(
        "one" -> "SELECT casenumber, max(submitted) as submitted_ GROUP BY casenumber |> SELECT date_trunc_ymd(submitted_) AS period__alias_2, count(`casenumber`) AS `value__alias_2` GROUP BY `submitted_`"
      ),
      "SELECT casenumber, max(submitted) as submitted_ GROUP BY casenumber |> SELECT date_trunc_ymd(submitted_) AS period__alias_2, count(casenumber) AS value__alias_2 GROUP BY submitted_",
      "SELECT c1 as period__alias_2,(coalesce(c2,0)) as value__alias_2",
      Some("one")
    )
  }


 ignore("example two") {
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "ftrans_id_column" -> (SoQLNumber.t, "ftrans_id"),
          "order_id_column" -> (SoQLNumber.t, "order_id"),
          "timestamp_column" -> (SoQLFloatingTimestamp.t, "timestamp"),
          "implement_type_column" -> (SoQLText.t, "implement_type"),
          "implement_name_column" -> (SoQLText.t, "implement_name"),
          "transaction_type_column" -> (SoQLText.t, "transaction_type"),
          "account_number_column" -> (SoQLText.t, "account_number"),
          "merchant_column" -> (SoQLText.t, "merchant"),
          "service_column" -> (SoQLText.t, "service"),
          "transaction_amount_column" -> (SoQLNumber.t, "transaction_amount"),
          "failure_message_column" -> (SoQLText.t, "failure_message"),
          "processor_column" -> (SoQLText.t, "processor"),
          "bin_number_column" -> (SoQLNumber.t, "bin_number"),
          "account_code_column" -> (SoQLText.t, "account_code"),
          "email_address_column" -> (SoQLText.t, "email_address"),
          "customer_address1_column" -> (SoQLText.t, "customer_address1"),
          "government_level_column" -> (SoQLText.t, "government_level"),
          "agency_category_column" -> (SoQLText.t, "agency_category"),
          "customer_state_column" -> (SoQLText.t, "customer_state"),
          "billing_state_column" -> (SoQLText.t, "billing_state"),
          "ip_address_column" -> (SoQLText.t, "ip_address"),
          "local_ref_column" -> (SoQLText.t, "local_ref"),
          "service_code_column" -> (SoQLNumber.t, "service_code"),
          "settle_date_column" -> (SoQLFloatingTimestamp.t, "settle_date"),
          "processing_cost_column" -> (SoQLNumber.t, "processing_cost"),
          "customer_address2_column" -> (SoQLText.t, "customer_address2"),
          "customer_city_column" -> (SoQLText.t, "customer_city"),
          "customer_zip_column" -> (SoQLText.t, "customer_zip"),
          "billing_address1_column" -> (SoQLText.t, "billing_address1"),
          "billing_address2_column" -> (SoQLText.t, "billing_address2"),
          "billing_city_column" -> (SoQLText.t, "billing_city"),
          "billing_zip_column" -> (SoQLText.t, "billing_zip"),
          "customer_country_column" -> (SoQLText.t, "customer_country"),
          "billing_country_column" -> (SoQLText.t, "billing_country")
        )
      ),
      Map(
        "one" -> "SELECT :*,* WHERE transaction_type = 'PAYMENT' |> select service,merchant,customer_state,agency_category,date_trunc_ymd(timestamp),date_trunc_ym(timestamp),date_trunc_y(timestamp),date_extract_woy(timestamp),min(timestamp),max(timestamp),count(*),count(ftrans_id) group by service,merchant,customer_state,agency_category,date_trunc_ymd(timestamp),date_trunc_ym(timestamp),date_trunc_y(timestamp),date_extract_woy(timestamp)"
      ),
      "select :*, * where (service in ('TXDPS Breath Test','TXDPS Crime Recs - Public','TXDPS Crime Recs - Secure','TXDPS Driver License','TXDPS Driver License Wallet','TXDPS Driver Record','TXDPS General Store','TXDPS Inspector App','TXDPS LTC','TXDPS Metal Reg Prog','TXDPS Quarter Award','TXDPS Reinstatement','TXDPS Special Badge','TXDPS Station App','TXDPS Station Renew','TXDPS UEP','TXDPS VIC Lic Renew','DPS Contractor DR','DPS Driver Records','TX DPS Priv Sec Bulk','TX DPS Priv Sec Bus','TX DPS Priv Sec Ind','Tx DPS-DL Office','Capitol Access Pass','DRIVER RCRD MNTRING')) |> SELECT * WHERE transaction_type = 'PAYMENT' |> SELECT count(ftrans_id) as total__alias_2,count(ftrans_id) as secondary_view_21654093466970_01646435825036__alias_2 WHERE (timestamp >= '2022-03-03') AND (timestamp < '2023-01-20')",
      "SELECT (coalesce((sum(c12)), 0)) as total__alias_2, (coalesce((sum(c12)), 0)) as secondary_view_21654093466970_01646435825036__alias_2 WHERE (((c1 in ('TXDPS Breath Test','TXDPS Crime Recs - Public','TXDPS Crime Recs - Secure','TXDPS Driver License','TXDPS Driver License Wallet','TXDPS Driver Record','TXDPS General Store','TXDPS Inspector App','TXDPS LTC','TXDPS Metal Reg Prog','TXDPS Quarter Award','TXDPS Reinstatement','TXDPS Special Badge','TXDPS Station App','TXDPS Station Renew','TXDPS UEP','TXDPS VIC Lic Renew','DPS Contractor DR','DPS Driver Records','TX DPS Priv Sec Bulk','TX DPS Priv Sec Bus','TX DPS Priv Sec Ind','Tx DPS-DL Office','Capitol Access Pass','DRIVER RCRD MNTRING')) and true) and ((c5 >= '2022-03-03') and (c5 < '2023-01-20')))",
      Some("one")
    )
  }
}
