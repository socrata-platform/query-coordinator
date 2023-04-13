package com.socrata.querycoordinator.defect

import com.socrata.querycoordinator.QueryRewritingTestUtility.AssertRewriteDefault
import com.socrata.soql.types.{SoQLBoolean, SoQLFloatingTimestamp, SoQLNumber, SoQLText}
import org.scalatest.FunSuite

//https://socrata.atlassian.net/browse/EN-58749
//
//  ToDo: invalid test (missing schema)
//
class EN58749 extends FunSuite{

  ignore("example one") {
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "object_id_column" -> (SoQLText.t, "object_id"),
          "client_code_column" -> (SoQLText.t, "client_code"),
          "quick_ref_id_column" -> (SoQLText.t, "quick_ref_id"),
          "tax_year_column" -> (SoQLText.t, "tax_year"),
          "owner_name_column" -> (SoQLText.t, "owner_name"),
          "appr_land_column" -> (SoQLNumber.t, "appr_land"),
          "appr_building_column" -> (SoQLNumber.t, "appr_building"),
          "appr_total_column" -> (SoQLNumber.t, "appr_total"),
          "cost_value_column" -> (SoQLNumber.t, "cost_value"),
          "total_estimated_market_value_column" -> (SoQLNumber.t, "total_estimated_market_value"),
          "lbcs_func_code_column" -> (SoQLText.t, "lbcs_func_code"),
          "lbcs_func_description_column" -> (SoQLText.t, "lbcs_func_description"),
          "class_code_column" -> (SoQLText.t, "class_code"),
          "class_description_column" -> (SoQLText.t, "class_description"),
          "total_acres_column" -> (SoQLNumber.t, "total_acres"),
          "total_sqft_column" -> (SoQLNumber.t, "total_sqft"),
          "building_style_column" -> (SoQLText.t, "building_style"),
          "residential_year_built_column" -> (SoQLNumber.t, "residential_year_built"),
          "style_column" -> (SoQLText.t, "style"),
          "basement_description_column" -> (SoQLText.t, "basement_description"),
          "sfla_column" -> (SoQLNumber.t, "sfla"),
          "bedrooms_column" -> (SoQLNumber.t, "bedrooms"),
          "land_use_code_column" -> (SoQLText.t, "land_use_code"),
          "land_use_description_column" -> (SoQLText.t, "land_use_description"),
          "situs_address_column" -> (SoQLText.t, "situs_address"),
          "street_column" -> (SoQLText.t, "street"),
          "city_column" -> (SoQLText.t, "city"),
          "state_column" -> (SoQLText.t, "state"),
          "zip_column" -> (SoQLText.t, "zip"),
          "tax_district_description_column" -> (SoQLText.t, "tax_district_description"),
          "neighborhood_description_column" -> (SoQLText.t, "neighborhood_description"),
          "neighborhood_code_column" -> (SoQLText.t, "neighborhood_code"),
          "property_type_description_column" -> (SoQLText.t, "property_type_description"),
          "base_property_type_column" -> (SoQLText.t, "base_property_type"),
          "school_taxing_unit_codes_column" -> (SoQLText.t, "school_taxing_unit_codes"),
          "property_number_column" -> (SoQLText.t, "property_number"),
          "site_name_column" -> (SoQLText.t, "site_name"),
          "aguseland_value_column" -> (SoQLNumber.t, "aguseland_value"),
          "income_value_column" -> (SoQLNumber.t, "income_value"),
          "value_method_key_column" -> (SoQLText.t, "value_method_key"),
          "quality_code_column" -> (SoQLText.t, "quality_code"),
          "quality_description_column" -> (SoQLText.t, "quality_description"),
          "cdu_description_column" -> (SoQLText.t, "cdu_description"),
          "cdu_code_column" -> (SoQLText.t, "cdu_code"),
          "physical_condition_code_column" -> (SoQLText.t, "physical_condition_code"),
          "physical_condition_description_column" -> (SoQLText.t, "physical_condition_description"),
          "sale_date_column" -> (SoQLFloatingTimestamp.t, "sale_date"),
          "sale_price_column" -> (SoQLNumber.t, "sale_price"),
          "adj_sale_price_column" -> (SoQLNumber.t, "adj_sale_price"),
          "sale_type_code_column" -> (SoQLText.t, "sale_type_code"),
          "sale_validity_code_column" -> (SoQLText.t, "sale_validity_code"),


        )
      ),
      Map(
        "one" -> "SELECT `object_id` AS `object_id`, `client_code` AS `client_code`, `quick_ref_id` AS `quick_ref_id`, `tax_year` AS `tax_year`, `owner_name` AS `owner_name`, `appr_land` AS `appr_land`, `appr_building` AS `appr_building`, `appr_total` AS `appr_total`, `cost_value` AS `cost_value`, `total_estimated_market_value` AS `total_estimated_market_value`, `lbcs_func_code` AS `lbcs_func_code`, `lbcs_func_description` AS `lbcs_func_description`, `class_code` AS `class_code`, `class_description` AS `class_description`, `total_acres` AS `total_acres`, `total_sqft` AS `total_sqft`, `building_style` AS `building_style`, `residential_year_built` AS `residential_year_built`, `style` AS `style`, `basement_description` AS `basement_description`, `sfla` AS `sfla`, `bedrooms` AS `bedrooms`, `land_use_code` AS `land_use_code`, `land_use_description` AS `land_use_description`, `situs_address` AS `situs_address`, `street` AS `street`, `city` AS `city`, `state` AS `state`, `zip` AS `zip`, `tax_district_description` AS `tax_district_description`, `neighborhood_description` AS `neighborhood_description`, `neighborhood_code` AS `neighborhood_code`, `property_type_description` AS `property_type_description`, `base_property_type` AS `base_property_type`, `school_taxing_unit_codes` AS `school_taxing_unit_codes`, `property_number` AS `property_number`, `site_name` AS `site_name`, `aguseland_value` AS `aguseland_value`, `income_value` AS `income_value`, `value_method_key` AS `value_method_key`, `quality_code` AS `quality_code`, `quality_description` AS `quality_description`, `cdu_code` AS `cdu_code`, `cdu_description` AS `cdu_description`, `physical_condition_code` AS `physical_condition_code`, `physical_condition_description` AS `physical_condition_description` UNION ALL SELECT `object_id` AS `object_id`, `client_code` AS `client_code`, `quick_ref_id` AS `quick_ref_id`, `tax_year` AS `tax_year`, `owner_name` AS `owner_name`, `appr_land` AS `appr_land`, `appr_building` AS `appr_building`, `appr_total` AS `appr_total`, `cost_value` AS `cost_value`, `total_estimated_market_value` AS `total_estimated_market_value`, `lbcs_func_code` AS `lbcs_func_code`, `lbcs_func_description` AS `lbcs_func_description`, `class_code` AS `class_code`, `class_description` AS `class_description`, `total_acres` AS `total_acres`, `total_sqft` AS `total_sqft`, `building_style` AS `building_style`, `residential_year_built` AS `residential_year_built`, `style` AS `style`, `basement_description` AS `basement_description`, `sfla` AS `sfla`, `bedrooms` AS `bedrooms`, `land_use_code` AS `land_use_code`, `land_use_description` AS `land_use_description`, `situs_address` AS `situs_address`, `street` AS `street`, `city` AS `city`, `state` AS `state`, `zip` AS `zip`, `tax_district_description` AS `tax_district_description`, `neighborhood_description` AS `neighborhood_description`, `neighborhood_code` AS `neighborhood_code`, `property_type_description` AS `property_type_description`, `base_property_type` AS `base_property_type`, `school_taxing_unit_codes` AS `school_taxing_unit_codes`, `property_number` AS `property_number`, `site_name` AS `site_name`, `aguseland_value` AS `aguseland_value`, `income_value` AS `income_value`, `value_method_key` AS `value_method_key`, `quality_code` AS `quality_code`, `quality_description` AS `quality_description`, `cdu_code` AS `cdu_code`, `cdu_description` AS `cdu_description`, `physical_condition_code` AS `physical_condition_code`, `physical_condition_description` AS `physical_condition_description` FROM @r5im-axsb UNION ALL SELECT `object_id` AS `object_id`, `client_code` AS `client_code`, `quick_ref_id` AS `quick_ref_id`, `tax_year` AS `tax_year`, `owner_name` AS `owner_name`, `appr_land` AS `appr_land`, `appr_building` AS `appr_building`, `appr_total` AS `appr_total`, `cost_value` AS `cost_value`, `total_estimated_market_value` AS `total_estimated_market_value`, `lbcs_func_code` AS `lbcs_func_code`, `lbcs_func_description` AS `lbcs_func_description`, `class_code` AS `class_code`, `class_description` AS `class_description`, `total_acres` AS `total_acres`, `total_sqft` AS `total_sqft`, `building_style` AS `building_style`, `residential_year_built` AS `residential_year_built`, `style` AS `style`, `basement_description` AS `basement_description`, `sfla` AS `sfla`, `bedrooms` AS `bedrooms`, `land_use_code` AS `land_use_code`, `land_use_description` AS `land_use_description`, `situs_address` AS `situs_address`, `street` AS `street`, `city` AS `city`, `state` AS `state`, `zip` AS `zip`, `tax_district_description` AS `tax_district_description`, `neighborhood_description` AS `neighborhood_description`, `neighborhood_code` AS `neighborhood_code`, `property_type_description` AS `property_type_description`, `base_property_type` AS `base_property_type`, `school_taxing_unit_codes` AS `school_taxing_unit_codes`, `property_number` AS `property_number`, `site_name` AS `site_name`, `aguseland_value` AS `aguseland_value`, `income_value` AS `income_value`, `value_method_key` AS `value_method_key`, `quality_code` AS `quality_code`, `quality_description` AS `quality_description`, `cdu_code` AS `cdu_code`, `cdu_description` AS `cdu_description`, `physical_condition_code` AS `physical_condition_code`, `physical_condition_description` AS `physical_condition_description` FROM @p8ex-zwi5 UNION ALL SELECT `object_id` AS `object_id`, `client_code` AS `client_code`, `quick_ref_id` AS `quick_ref_id`, `tax_year` AS `tax_year`, `owner_name` AS `owner_name`, `appr_land` AS `appr_land`, `appr_building` AS `appr_building`, `appr_total` AS `appr_total`, `cost_value` AS `cost_value`, `total_estimated_market_value` AS `total_estimated_market_value`, `lbcs_func_code` AS `lbcs_func_code`, `lbcs_func_description` AS `lbcs_func_description`, `class_code` AS `class_code`, `class_description` AS `class_description`, `total_acres` AS `total_acres`, `total_sqft` AS `total_sqft`, `building_style` AS `building_style`, `residential_year_built` AS `residential_year_built`, `style` AS `style`, `basement_description` AS `basement_description`, `sfla` AS `sfla`, `bedrooms` AS `bedrooms`, `land_use_code` AS `land_use_code`, `land_use_description` AS `land_use_description`, `situs_address` AS `situs_address`, `street` AS `street`, `city` AS `city`, `state` AS `state`, `zip` AS `zip`, `tax_district_description` AS `tax_district_description`, `neighborhood_description` AS `neighborhood_description`, `neighborhood_code` AS `neighborhood_code`, `property_type_description` AS `property_type_description`, `base_property_type` AS `base_property_type`, `school_taxing_unit_codes` AS `school_taxing_unit_codes`, `property_number` AS `property_number`, `site_name` AS `site_name`, `aguseland_value` AS `aguseland_value`, `income_value` AS `income_value`, `value_method_key` AS `value_method_key`, `quality_code` AS `quality_code`, `quality_description` AS `quality_description`, `cdu_code` AS `cdu_code`, `cdu_description` AS `cdu_description`, `physical_condition_code` AS `physical_condition_code`, `physical_condition_description` AS `physical_condition_description` FROM @gtgw-eqkn UNION ALL SELECT `object_id` AS `object_id`, `client_code` AS `client_code`, `quick_ref_id` AS `quick_ref_id`, `tax_year` AS `tax_year`, `owner_name` AS `owner_name`, `appr_land` AS `appr_land`, `appr_building` AS `appr_building`, `appr_total` AS `appr_total`, `cost_value` AS `cost_value`, `total_estimated_market_value` AS `total_estimated_market_value`, `lbcs_func_code` AS `lbcs_func_code`, `lbcs_func_description` AS `lbcs_func_description`, `class_code` AS `class_code`, `class_description` AS `class_description`, `total_acres` AS `total_acres`, `total_sqft` AS `total_sqft`, `building_style` AS `building_style`, `residential_year_built` AS `residential_year_built`, `style` AS `style`, `basement_description` AS `basement_description`, `sfla` AS `sfla`, `bedrooms` AS `bedrooms`, `land_use_code` AS `land_use_code`, `land_use_description` AS `land_use_description`, `situs_address` AS `situs_address`, `street` AS `street`, `city` AS `city`, `state` AS `state`, `zip` AS `zip`, `tax_district_description` AS `tax_district_description`, `neighborhood_description` AS `neighborhood_description`, `neighborhood_code` AS `neighborhood_code`, `property_type_description` AS `property_type_description`, `base_property_type` AS `base_property_type`, `school_taxing_unit_codes` AS `school_taxing_unit_codes`, `property_number` AS `property_number`, `site_name` AS `site_name`, `aguseland_value` AS `aguseland_value`, `income_value` AS `income_value`, `value_method_key` AS `value_method_key`, `quality_code` AS `quality_code`, `quality_description` AS `quality_description`, `cdu_code` AS `cdu_code`, `cdu_description` AS `cdu_description`, `physical_condition_code` AS `physical_condition_code`, `physical_condition_description` AS `physical_condition_description` FROM @3jmu-qt3s"
      ),
      "SELECT `object_id` AS `object_id`, `client_code` AS `client_code`, `quick_ref_id` AS `quick_ref_id`, `tax_year` AS `tax_year`, `owner_name` AS `owner_name`, `appr_land` AS `appr_land`, `appr_building` AS `appr_building`, `appr_total` AS `appr_total`, `cost_value` AS `cost_value`, `total_estimated_market_value` AS `total_estimated_market_value`, `lbcs_func_code` AS `lbcs_func_code`, `lbcs_func_description` AS `lbcs_func_description`, `class_code` AS `class_code`, `class_description` AS `class_description`, `total_acres` AS `total_acres`, `total_sqft` AS `total_sqft`, `building_style` AS `building_style`, `residential_year_built` AS `residential_year_built`, `style` AS `style`, `basement_description` AS `basement_description`, `sfla` AS `sfla`, `bedrooms` AS `bedrooms`, `land_use_code` AS `land_use_code`, `land_use_description` AS `land_use_description`, `situs_address` AS `situs_address`, `street` AS `street`, `city` AS `city`, `state` AS `state`, `zip` AS `zip`, `tax_district_description` AS `tax_district_description`, `neighborhood_description` AS `neighborhood_description`, `neighborhood_code` AS `neighborhood_code`, `property_type_description` AS `property_type_description`, `base_property_type` AS `base_property_type`, `school_taxing_unit_codes` AS `school_taxing_unit_codes`, `property_number` AS `property_number`, `site_name` AS `site_name`, `aguseland_value` AS `aguseland_value`, `income_value` AS `income_value`, `value_method_key` AS `value_method_key`, `quality_code` AS `quality_code`, `quality_description` AS `quality_description`, `cdu_code` AS `cdu_code`, `cdu_description` AS `cdu_description`, `physical_condition_code` AS `physical_condition_code`, `physical_condition_description` AS `physical_condition_description` UNION ALL SELECT `object_id` AS `object_id`, `client_code` AS `client_code`, `quick_ref_id` AS `quick_ref_id`, `tax_year` AS `tax_year`, `owner_name` AS `owner_name`, `appr_land` AS `appr_land`, `appr_building` AS `appr_building`, `appr_total` AS `appr_total`, `cost_value` AS `cost_value`, `total_estimated_market_value` AS `total_estimated_market_value`, `lbcs_func_code` AS `lbcs_func_code`, `lbcs_func_description` AS `lbcs_func_description`, `class_code` AS `class_code`, `class_description` AS `class_description`, `total_acres` AS `total_acres`, `total_sqft` AS `total_sqft`, `building_style` AS `building_style`, `residential_year_built` AS `residential_year_built`, `style` AS `style`, `basement_description` AS `basement_description`, `sfla` AS `sfla`, `bedrooms` AS `bedrooms`, `land_use_code` AS `land_use_code`, `land_use_description` AS `land_use_description`, `situs_address` AS `situs_address`, `street` AS `street`, `city` AS `city`, `state` AS `state`, `zip` AS `zip`, `tax_district_description` AS `tax_district_description`, `neighborhood_description` AS `neighborhood_description`, `neighborhood_code` AS `neighborhood_code`, `property_type_description` AS `property_type_description`, `base_property_type` AS `base_property_type`, `school_taxing_unit_codes` AS `school_taxing_unit_codes`, `property_number` AS `property_number`, `site_name` AS `site_name`, `aguseland_value` AS `aguseland_value`, `income_value` AS `income_value`, `value_method_key` AS `value_method_key`, `quality_code` AS `quality_code`, `quality_description` AS `quality_description`, `cdu_code` AS `cdu_code`, `cdu_description` AS `cdu_description`, `physical_condition_code` AS `physical_condition_code`, `physical_condition_description` AS `physical_condition_description` FROM @r5im-axsb UNION ALL SELECT `object_id` AS `object_id`, `client_code` AS `client_code`, `quick_ref_id` AS `quick_ref_id`, `tax_year` AS `tax_year`, `owner_name` AS `owner_name`, `appr_land` AS `appr_land`, `appr_building` AS `appr_building`, `appr_total` AS `appr_total`, `cost_value` AS `cost_value`, `total_estimated_market_value` AS `total_estimated_market_value`, `lbcs_func_code` AS `lbcs_func_code`, `lbcs_func_description` AS `lbcs_func_description`, `class_code` AS `class_code`, `class_description` AS `class_description`, `total_acres` AS `total_acres`, `total_sqft` AS `total_sqft`, `building_style` AS `building_style`, `residential_year_built` AS `residential_year_built`, `style` AS `style`, `basement_description` AS `basement_description`, `sfla` AS `sfla`, `bedrooms` AS `bedrooms`, `land_use_code` AS `land_use_code`, `land_use_description` AS `land_use_description`, `situs_address` AS `situs_address`, `street` AS `street`, `city` AS `city`, `state` AS `state`, `zip` AS `zip`, `tax_district_description` AS `tax_district_description`, `neighborhood_description` AS `neighborhood_description`, `neighborhood_code` AS `neighborhood_code`, `property_type_description` AS `property_type_description`, `base_property_type` AS `base_property_type`, `school_taxing_unit_codes` AS `school_taxing_unit_codes`, `property_number` AS `property_number`, `site_name` AS `site_name`, `aguseland_value` AS `aguseland_value`, `income_value` AS `income_value`, `value_method_key` AS `value_method_key`, `quality_code` AS `quality_code`, `quality_description` AS `quality_description`, `cdu_code` AS `cdu_code`, `cdu_description` AS `cdu_description`, `physical_condition_code` AS `physical_condition_code`, `physical_condition_description` AS `physical_condition_description` FROM @p8ex-zwi5 UNION ALL SELECT `object_id` AS `object_id`, `client_code` AS `client_code`, `quick_ref_id` AS `quick_ref_id`, `tax_year` AS `tax_year`, `owner_name` AS `owner_name`, `appr_land` AS `appr_land`, `appr_building` AS `appr_building`, `appr_total` AS `appr_total`, `cost_value` AS `cost_value`, `total_estimated_market_value` AS `total_estimated_market_value`, `lbcs_func_code` AS `lbcs_func_code`, `lbcs_func_description` AS `lbcs_func_description`, `class_code` AS `class_code`, `class_description` AS `class_description`, `total_acres` AS `total_acres`, `total_sqft` AS `total_sqft`, `building_style` AS `building_style`, `residential_year_built` AS `residential_year_built`, `style` AS `style`, `basement_description` AS `basement_description`, `sfla` AS `sfla`, `bedrooms` AS `bedrooms`, `land_use_code` AS `land_use_code`, `land_use_description` AS `land_use_description`, `situs_address` AS `situs_address`, `street` AS `street`, `city` AS `city`, `state` AS `state`, `zip` AS `zip`, `tax_district_description` AS `tax_district_description`, `neighborhood_description` AS `neighborhood_description`, `neighborhood_code` AS `neighborhood_code`, `property_type_description` AS `property_type_description`, `base_property_type` AS `base_property_type`, `school_taxing_unit_codes` AS `school_taxing_unit_codes`, `property_number` AS `property_number`, `site_name` AS `site_name`, `aguseland_value` AS `aguseland_value`, `income_value` AS `income_value`, `value_method_key` AS `value_method_key`, `quality_code` AS `quality_code`, `quality_description` AS `quality_description`, `cdu_code` AS `cdu_code`, `cdu_description` AS `cdu_description`, `physical_condition_code` AS `physical_condition_code`, `physical_condition_description` AS `physical_condition_description` FROM @gtgw-eqkn UNION ALL SELECT `object_id` AS `object_id`, `client_code` AS `client_code`, `quick_ref_id` AS `quick_ref_id`, `tax_year` AS `tax_year`, `owner_name` AS `owner_name`, `appr_land` AS `appr_land`, `appr_building` AS `appr_building`, `appr_total` AS `appr_total`, `cost_value` AS `cost_value`, `total_estimated_market_value` AS `total_estimated_market_value`, `lbcs_func_code` AS `lbcs_func_code`, `lbcs_func_description` AS `lbcs_func_description`, `class_code` AS `class_code`, `class_description` AS `class_description`, `total_acres` AS `total_acres`, `total_sqft` AS `total_sqft`, `building_style` AS `building_style`, `residential_year_built` AS `residential_year_built`, `style` AS `style`, `basement_description` AS `basement_description`, `sfla` AS `sfla`, `bedrooms` AS `bedrooms`, `land_use_code` AS `land_use_code`, `land_use_description` AS `land_use_description`, `situs_address` AS `situs_address`, `street` AS `street`, `city` AS `city`, `state` AS `state`, `zip` AS `zip`, `tax_district_description` AS `tax_district_description`, `neighborhood_description` AS `neighborhood_description`, `neighborhood_code` AS `neighborhood_code`, `property_type_description` AS `property_type_description`, `base_property_type` AS `base_property_type`, `school_taxing_unit_codes` AS `school_taxing_unit_codes`, `property_number` AS `property_number`, `site_name` AS `site_name`, `aguseland_value` AS `aguseland_value`, `income_value` AS `income_value`, `value_method_key` AS `value_method_key`, `quality_code` AS `quality_code`, `quality_description` AS `quality_description`, `cdu_code` AS `cdu_code`, `cdu_description` AS `cdu_description`, `physical_condition_code` AS `physical_condition_code`, `physical_condition_description` AS `physical_condition_description` FROM @3jmu-qt3s",
      "c1 as object_id,c2 as client_code,c3 as quick_ref_id,c4 as tax_year,c5 as owner_name,c6 as appr_land,c7 as appr_building,c8 as appr_total,c9 as cost_value,c10 as total_estimated_market_value,c11 as lbcs_func_code,c12 as lbcs_func_description,c13 as class_code,c14 as class_description,c15 as total_acres,c16 as total_sqft,c17 as building_style,c18 as residential_year_built,c19 as style,c20 as basement_description,c21 as sfla,c22 as bedrooms,c23 as land_use_code,c24 as land_use_description,c25 as situs_address,c26 as street,c27 as city,c28 as state,c29 as tax_district_description,c30 as neighborhood_description,c31 as neighborhood_code,c32 as property_type_description,c33 as base_property_type,c34 as school_taxing_unit_codes,c35 as property_number,c36 as site_name,c37 as aguseland_value,c38 as income_value,c39 as value_method_key,c40 as quality_code,c41 as quality_description,c42 as cdu_code,c43 as cdu_description,c44 as physical_condition_code,c45 as physical_condition_description",
      Some("one")
    )
  }
}
