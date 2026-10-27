/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.tpcds.gen;

import java.util.Locale;

/** Schema and random-stream metadata for the 24 TPC-DS base tables. */
public enum TpcdsTable {
  CALL_CENTER(
      1,
      100,
      0x0BL,
      true,
      true,
      3,
      2,
      new long[] {3, 12, 15, 18, 21, 24, 27, 30, 30},
      new int[] {
        0, 15, 10, 1, 4, 10, 0, 2, 1, 1, 1, 2, 1, 50, 50, 2, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 15, 1, 1, 2
      },
      "cc_call_center_sk:bigint:0:key;"
          + "cc_call_center_id:char(16):1:varchar;"
          + "cc_rec_start_date:date:2:date;"
          + "cc_rec_end_date:date:3:date;"
          + "cc_closed_date_sk:integer:4:key;"
          + "cc_open_date_sk:integer:5:key;"
          + "cc_name:varchar(50):6:varchar;"
          + "cc_class:varchar(50):7:varchar;"
          + "cc_employees:integer:8:integer;"
          + "cc_sq_ft:integer:9:integer;"
          + "cc_hours:char(20):10:varchar;"
          + "cc_manager:varchar(40):11:varchar;"
          + "cc_mkt_id:integer:12:integer;"
          + "cc_mkt_class:char(50):13:varchar;"
          + "cc_mkt_desc:varchar(100):14:varchar;"
          + "cc_market_manager:varchar(40):15:varchar;"
          + "cc_division:integer:16:integer;"
          + "cc_division_name:varchar(50):17:varchar;"
          + "cc_company:integer:18:integer;"
          + "cc_company_name:char(50):19:varchar;"
          + "cc_street_number:char(10):30:integer;"
          + "cc_street_name:varchar(60):30:varchar;"
          + "cc_street_type:char(15):30:varchar;"
          + "cc_suite_number:char(10):30:varchar;"
          + "cc_city:varchar(60):30:varchar;"
          + "cc_county:varchar(30):30:varchar;"
          + "cc_state:char(2):30:varchar;"
          + "cc_zip:char(10):30:varchar;"
          + "cc_country:varchar(20):30:varchar;"
          + "cc_gmt_offset:decimal(5,2):30:integer;"
          + "cc_tax_percentage:decimal(5,2):31:decimal"),
  CATALOG_PAGE(
      35,
      200,
      0x03L,
      false,
      false,
      1,
      1,
      new long[] {11718, 12000, 20400, 26000, 30000, 36000, 40000, 46000, 50000},
      new int[] {1, 1, 1, 1, 1, 1, 1, 1, 100, 1, 2},
      "cp_catalog_page_sk:bigint:0:key;"
          + "cp_catalog_page_id:char(16):1:varchar;"
          + "cp_start_date_sk:integer:2:key;"
          + "cp_end_date_sk:integer:3:key;"
          + "cp_department:varchar(50):5:varchar;"
          + "cp_catalog_number:integer:6:integer;"
          + "cp_catalog_page_number:integer:7:integer;"
          + "cp_description:varchar(100):8:varchar;"
          + "cp_type:varchar(100):9:varchar"),
  CATALOG_RETURNS(
      46,
      400,
      0x10007L,
      false,
      false,
      2,
      10000,
      new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0},
      new int[] {
        28, 28, 14, 14, 14, 14, 14, 28, 14, 14, 14, 0, 14, 14, 14, 14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 28, 70
      },
      "cr_returned_date_sk:bigint:0:key;"
          + "cr_returned_time_sk:bigint:1:key;"
          + "cr_item_sk:bigint:2:key;"
          + "cr_refunded_customer_sk:bigint:3:key;"
          + "cr_refunded_cdemo_sk:bigint:4:key;"
          + "cr_refunded_hdemo_sk:bigint:5:key;"
          + "cr_refunded_addr_sk:bigint:6:key;"
          + "cr_returning_customer_sk:bigint:7:key;"
          + "cr_returning_cdemo_sk:bigint:8:key;"
          + "cr_returning_hdemo_sk:bigint:9:key;"
          + "cr_returning_addr_sk:bigint:10:key;"
          + "cr_call_center_sk:bigint:11:key;"
          + "cr_catalog_page_sk:bigint:12:key;"
          + "cr_ship_mode_sk:bigint:13:key;"
          + "cr_warehouse_sk:bigint:14:key;"
          + "cr_reason_sk:bigint:15:key;"
          + "cr_order_number:bigint:16:key;"
          + "cr_return_quantity:integer:17:integer;"
          + "cr_return_amount:decimal(7,2):18:decimal;"
          + "cr_return_tax:decimal(7,2):19:decimal;"
          + "cr_return_amt_inc_tax:decimal(7,2):20:decimal;"
          + "cr_fee:decimal(7,2):21:decimal;"
          + "cr_return_ship_cost:decimal(7,2):22:decimal;"
          + "cr_refunded_cash:decimal(7,2):23:decimal;"
          + "cr_reversed_charge:decimal(7,2):24:decimal;"
          + "cr_store_credit:decimal(7,2):25:decimal;"
          + "cr_net_loss:decimal(7,2):26:decimal"),
  CATALOG_SALES(
      75,
      100,
      0x28000L,
      false,
      false,
      2,
      10000,
      new long[] {16, 160, 1600, 4800, 16000, 48000, 160000, 480000, 1600000},
      new int[] {
        1, 2, 14, 1, 1, 1, 1, 2, 1, 1, 1, 1, 42, 14, 14, 1, 14, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 112, 0, 28, 14, 0
      },
      "cs_sold_date_sk:bigint:0:key;"
          + "cs_sold_time_sk:bigint:1:key;"
          + "cs_ship_date_sk:bigint:2:key;"
          + "cs_bill_customer_sk:bigint:3:key;"
          + "cs_bill_cdemo_sk:bigint:4:key;"
          + "cs_bill_hdemo_sk:bigint:5:key;"
          + "cs_bill_addr_sk:bigint:6:key;"
          + "cs_ship_customer_sk:bigint:7:key;"
          + "cs_ship_cdemo_sk:bigint:8:key;"
          + "cs_ship_hdemo_sk:bigint:9:key;"
          + "cs_ship_addr_sk:bigint:10:key;"
          + "cs_call_center_sk:bigint:11:key;"
          + "cs_catalog_page_sk:bigint:12:key;"
          + "cs_ship_mode_sk:bigint:13:key;"
          + "cs_warehouse_sk:bigint:14:key;"
          + "cs_item_sk:bigint:15:key;"
          + "cs_promo_sk:bigint:16:key;"
          + "cs_order_number:bigint:17:key;"
          + "cs_quantity:integer:18:integer;"
          + "cs_wholesale_cost:decimal(7,2):19:decimal;"
          + "cs_list_price:decimal(7,2):20:decimal;"
          + "cs_sales_price:decimal(7,2):21:decimal;"
          + "cs_ext_discount_amt:decimal(7,2):24:decimal;"
          + "cs_ext_sales_price:decimal(7,2):23:decimal;"
          + "cs_ext_wholesale_cost:decimal(7,2):25:decimal;"
          + "cs_ext_list_price:decimal(7,2):26:decimal;"
          + "cs_ext_tax:decimal(7,2):27:decimal;"
          + "cs_coupon_amt:decimal(7,2):22:decimal;"
          + "cs_ext_ship_cost:decimal(7,2):28:decimal;"
          + "cs_net_paid:decimal(7,2):29:decimal;"
          + "cs_net_paid_inc_tax:decimal(7,2):30:decimal;"
          + "cs_net_paid_inc_ship:decimal(7,2):31:decimal;"
          + "cs_net_paid_inc_ship_tax:decimal(7,2):32:decimal;"
          + "cs_net_profit:decimal(7,2):33:decimal"),
  CUSTOMER(
      114,
      700,
      0x13L,
      false,
      false,
      3,
      1000,
      new long[] {100, 500, 2000, 5000, 12000, 30000, 65000, 80000, 100000},
      new int[] {1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 2, 1, 0, 0, 1, 1, 23, 1, 2},
      "c_customer_sk:bigint:0:key;"
          + "c_customer_id:char(16):1:varchar;"
          + "c_current_cdemo_sk:bigint:2:key;"
          + "c_current_hdemo_sk:bigint:3:key;"
          + "c_current_addr_sk:bigint:4:key;"
          + "c_first_shipto_date_sk:bigint:5:integer;"
          + "c_first_sales_date_sk:bigint:6:integer;"
          + "c_salutation:char(10):7:varchar;"
          + "c_first_name:char(20):8:varchar;"
          + "c_last_name:char(30):9:varchar;"
          + "c_preferred_cust_flag:char(1):10:boolean;"
          + "c_birth_day:integer:11:integer;"
          + "c_birth_month:integer:12:integer;"
          + "c_birth_year:integer:13:integer;"
          + "c_birth_country:varchar(20):14:varchar;"
          + "c_login:char(13):15:varchar;"
          + "c_email_address:char(50):16:varchar;"
          + "c_last_review_date_sk:bigint:17:integer"),
  CUSTOMER_ADDRESS(
      133,
      600,
      0x03L,
      false,
      false,
      3,
      1000,
      new long[] {50, 250, 1000, 2500, 6000, 15000, 32500, 40000, 50000},
      new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 7, 1},
      "ca_address_sk:bigint:0:key;"
          + "ca_address_id:char(16):1:varchar;"
          + "ca_street_number:char(10):2:integer;"
          + "ca_street_name:varchar(60):3:varchar;"
          + "ca_street_type:char(15):4:varchar;"
          + "ca_suite_number:char(10):5:varchar;"
          + "ca_city:varchar(60):6:varchar;"
          + "ca_county:varchar(30):7:varchar;"
          + "ca_state:char(2):8:varchar;"
          + "ca_zip:char(10):9:varchar;"
          + "ca_country:varchar(20):10:varchar;"
          + "ca_gmt_offset:decimal(5,2):11:integer;"
          + "ca_location_type:char(20):12:varchar"),
  CUSTOMER_DEMOGRAPHICS(
      149,
      0,
      0x1L,
      false,
      false,
      1,
      100,
      new long[] {19208, 19208, 19208, 19208, 19208, 19208, 19208, 19208, 19208},
      new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 2},
      "cd_demo_sk:bigint:0:key;"
          + "cd_gender:char(1):1:varchar;"
          + "cd_marital_status:char(1):2:varchar;"
          + "cd_education_status:char(20):3:varchar;"
          + "cd_purchase_estimate:integer:4:integer;"
          + "cd_credit_rating:char(10):5:varchar;"
          + "cd_dep_count:integer:6:integer;"
          + "cd_dep_employed_count:integer:7:integer;"
          + "cd_dep_college_count:integer:8:integer"),
  DATE_DIM(
      159,
      0,
      0x03L,
      false,
      false,
      1,
      1,
      new long[] {73049, 73049, 73049, 73049, 73049, 73049, 73049, 73049, 73049},
      new int[] {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2
      },
      "d_date_sk:bigint:0:key;"
          + "d_date_id:char(16):1:varchar;"
          + "d_date:date:0:date;"
          + "d_month_seq:integer:3:integer;"
          + "d_week_seq:integer:4:integer;"
          + "d_quarter_seq:integer:5:integer;"
          + "d_year:integer:6:integer;"
          + "d_dow:integer:7:integer;"
          + "d_moy:integer:8:integer;"
          + "d_dom:integer:9:integer;"
          + "d_qoy:integer:10:integer;"
          + "d_fy_year:integer:11:integer;"
          + "d_fy_quarter_seq:integer:12:integer;"
          + "d_fy_week_seq:integer:13:integer;"
          + "d_day_name:char(9):14:varchar;"
          + "d_quarter_name:char(6):15:varchar;"
          + "d_holiday:char(1):16:boolean;"
          + "d_weekend:char(1):17:boolean;"
          + "d_following_holiday:char(1):18:boolean;"
          + "d_first_dom:integer:19:integer;"
          + "d_last_dom:integer:20:integer;"
          + "d_same_day_ly:integer:21:integer;"
          + "d_same_day_lq:integer:22:integer;"
          + "d_current_day:char(1):23:boolean;"
          + "d_current_week:char(1):24:boolean;"
          + "d_current_month:char(1):25:boolean;"
          + "d_current_quarter:char(1):26:boolean;"
          + "d_current_year:char(1):27:boolean"),
  HOUSEHOLD_DEMOGRAPHICS(
      188,
      0,
      0x01L,
      false,
      false,
      1,
      1,
      new long[] {7200, 7200, 7200, 7200, 7200, 7200, 7200, 7200, 7200},
      new int[] {1, 1, 1, 1, 1, 2},
      "hd_demo_sk:bigint:0:key;"
          + "hd_income_band_sk:bigint:1:key;"
          + "hd_buy_potential:char(15):2:varchar;"
          + "hd_dep_count:integer:3:integer;"
          + "hd_vehicle_count:integer:4:integer"),
  INCOME_BAND(
      194,
      0,
      0x1L,
      false,
      false,
      1,
      1,
      new long[] {20, 20, 20, 20, 20, 20, 20, 20, 20},
      new int[] {1, 1, 1, 2},
      "ib_income_band_sk:bigint:0:integer;"
          + "ib_lower_bound:integer:1:integer;"
          + "ib_upper_bound:integer:2:integer"),
  INVENTORY(
      198,
      1000,
      0x07L,
      false,
      false,
      -1,
      1,
      new long[] {-1, -1, -1, -1, -1, -1, -1, -1, -1},
      new int[] {1, 1, 1, 1, 2},
      "inv_date_sk:bigint:0:key;"
          + "inv_item_sk:bigint:1:key;"
          + "inv_warehouse_sk:bigint:2:key;"
          + "inv_quantity_on_hand:integer:3:integer"),
  ITEM(
      203,
      50,
      0x0BL,
      true,
      false,
      3,
      2000,
      new long[] {9, 51, 102, 132, 150, 180, 201, 231, 251},
      new int[] {1, 1, 1, 2, 200, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 50, 1, 1, 1, 2, 1, 2, 1, 2},
      "i_item_sk:bigint:0:key;"
          + "i_item_id:char(16):1:varchar;"
          + "i_rec_start_date:date:2:date;"
          + "i_rec_end_date:date:3:date;"
          + "i_item_desc:varchar(200):4:varchar;"
          + "i_current_price:decimal(7,2):5:decimal;"
          + "i_wholesale_cost:decimal(7,2):6:decimal;"
          + "i_brand_id:integer:7:key;"
          + "i_brand:char(50):8:varchar;"
          + "i_class_id:integer:9:key;"
          + "i_class:char(50):10:varchar;"
          + "i_category_id:integer:11:key;"
          + "i_category:char(50):12:varchar;"
          + "i_manufact_id:integer:13:key;"
          + "i_manufact:char(50):14:varchar;"
          + "i_size:char(20):15:varchar;"
          + "i_formulation:char(20):16:varchar;"
          + "i_color:char(20):17:varchar;"
          + "i_units:char(10):18:varchar;"
          + "i_container:char(10):19:varchar;"
          + "i_manager_id:integer:20:key;"
          + "i_product_name:char(50):21:varchar"),
  PROMOTION(
      228,
      200,
      0x03L,
      false,
      false,
      3,
      1,
      new long[] {300, 500, 1000, 1300, 1500, 1800, 2000, 2300, 2500},
      new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 100, 1, 1, 2},
      "p_promo_sk:bigint:0:key;"
          + "p_promo_id:char(16):1:varchar;"
          + "p_start_date_sk:bigint:2:key;"
          + "p_end_date_sk:bigint:3:key;"
          + "p_item_sk:bigint:4:key;"
          + "p_cost:decimal(15,2):5:decimal;"
          + "p_response_target:integer:6:integer;"
          + "p_promo_name:char(50):7:varchar;"
          + "p_channel_dmail:char(1):8:boolean;"
          + "p_channel_email:char(1):9:boolean;"
          + "p_channel_catalog:char(1):10:boolean;"
          + "p_channel_tv:char(1):11:boolean;"
          + "p_channel_radio:char(1):12:boolean;"
          + "p_channel_press:char(1):13:boolean;"
          + "p_channel_event:char(1):14:boolean;"
          + "p_channel_demo:char(1):15:boolean;"
          + "p_channel_details:varchar(100):16:varchar;"
          + "p_purpose:char(15):17:varchar;"
          + "p_discount_active:char(1):18:boolean"),
  REASON(
      248,
      0,
      0x03L,
      false,
      false,
      3,
      1,
      new long[] {35, 45, 55, 60, 65, 67, 70, 72, 75},
      new int[] {1, 1, 1, 2},
      "r_reason_sk:bigint:0:key;"
          + "r_reason_id:char(16):1:varchar;"
          + "r_reason_desc:char(100):2:varchar"),
  SHIP_MODE(
      252,
      0,
      0x03L,
      false,
      false,
      1,
      1,
      new long[] {20, 20, 20, 20, 20, 20, 20, 20, 20},
      new int[] {1, 1, 1, 1, 21, 1, 2},
      "sm_ship_mode_sk:bigint:0:key;"
          + "sm_ship_mode_id:char(16):1:varchar;"
          + "sm_type:char(30):2:varchar;"
          + "sm_code:char(10):3:varchar;"
          + "sm_carrier:char(20):5:varchar;"
          + "sm_contract:char(20):4:varchar"),
  STORE(
      259,
      100,
      0xBL,
      true,
      true,
      3,
      2,
      new long[] {6, 51, 201, 402, 501, 675, 750, 852, 951},
      new int[] {
        1, 1, 1, 2, 2, 0, 1, 1, 1, 2, 1, 1, 1, 100, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2,
        1, 1, 7
      },
      "s_store_sk:bigint:0:key;"
          + "s_store_id:char(16):1:varchar;"
          + "s_rec_start_date:date:2:date;"
          + "s_rec_end_date:date:3:date;"
          + "s_closed_date_sk:bigint:4:key;"
          + "s_store_name:varchar(50):5:varchar;"
          + "s_number_employees:integer:6:integer;"
          + "s_floor_space:integer:7:integer;"
          + "s_hours:char(20):8:varchar;"
          + "s_manager:varchar(40):9:varchar;"
          + "s_market_id:integer:10:integer;"
          + "s_geography_class:varchar(100):12:varchar;"
          + "s_market_desc:varchar(100):13:varchar;"
          + "s_market_manager:varchar(40):14:varchar;"
          + "s_division_id:integer:15:key;"
          + "s_division_name:varchar(50):16:varchar;"
          + "s_company_id:integer:17:key;"
          + "s_company_name:varchar(50):18:varchar;"
          + "s_street_number:varchar(10):19:integer;"
          + "s_street_name:varchar(60):20:varchar;"
          + "s_street_type:char(15):21:varchar;"
          + "s_suite_number:char(10):22:varchar;"
          + "s_city:varchar(60):23:varchar;"
          + "s_county:varchar(30):24:varchar;"
          + "s_state:char(2):25:varchar;"
          + "s_zip:char(10):26:varchar;"
          + "s_country:varchar(20):27:varchar;"
          + "s_gmt_offset:decimal(5,2):28:integer;"
          + "s_tax_percentage:decimal(5,2):11:decimal"),
  STORE_RETURNS(
      292,
      700,
      0x204L,
      false,
      false,
      -1,
      1,
      new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0},
      new int[] {32, 32, 16, 16, 16, 16, 16, 16, 16, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 80, 32},
      "sr_returned_date_sk:bigint:0:key;"
          + "sr_return_time_sk:bigint:1:key;"
          + "sr_item_sk:bigint:2:key;"
          + "sr_customer_sk:bigint:3:key;"
          + "sr_cdemo_sk:bigint:4:key;"
          + "sr_hdemo_sk:bigint:5:key;"
          + "sr_addr_sk:bigint:6:key;"
          + "sr_store_sk:bigint:7:key;"
          + "sr_reason_sk:bigint:8:key;"
          + "sr_ticket_number:bigint:9:key;"
          + "sr_return_quantity:integer:10:integer;"
          + "sr_return_amt:decimal(7,2):11:decimal;"
          + "sr_return_tax:decimal(7,2):12:decimal;"
          + "sr_return_amt_inc_tax:decimal(7,2):13:decimal;"
          + "sr_fee:decimal(7,2):14:decimal;"
          + "sr_return_ship_cost:decimal(7,2):15:decimal;"
          + "sr_refunded_cash:decimal(7,2):16:decimal;"
          + "sr_reversed_charge:decimal(7,2):17:decimal;"
          + "sr_store_credit:decimal(7,2):18:decimal;"
          + "sr_net_loss:decimal(7,2):19:decimal"),
  STORE_SALES(
      314,
      900,
      0x204L,
      false,
      false,
      2,
      10000,
      new long[] {24, 240, 2400, 7200, 24000, 72000, 240000, 720000, 2400000},
      new int[] {2, 2, 1, 1, 1, 1, 1, 1, 16, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 128, 32, 0},
      "ss_sold_date_sk:bigint:0:key;"
          + "ss_sold_time_sk:bigint:1:key;"
          + "ss_item_sk:bigint:2:key;"
          + "ss_customer_sk:bigint:3:key;"
          + "ss_cdemo_sk:bigint:4:key;"
          + "ss_hdemo_sk:bigint:5:key;"
          + "ss_addr_sk:bigint:6:key;"
          + "ss_store_sk:bigint:7:key;"
          + "ss_promo_sk:bigint:8:key;"
          + "ss_ticket_number:bigint:9:key;"
          + "ss_quantity:integer:10:integer;"
          + "ss_wholesale_cost:decimal(7,2):11:decimal;"
          + "ss_list_price:decimal(7,2):12:decimal;"
          + "ss_sales_price:decimal(7,2):13:decimal;"
          + "ss_ext_discount_amt:decimal(7,2):14:decimal;"
          + "ss_ext_sales_price:decimal(7,2):15:decimal;"
          + "ss_ext_wholesale_cost:decimal(7,2):16:decimal;"
          + "ss_ext_list_price:decimal(7,2):17:decimal;"
          + "ss_ext_tax:decimal(7,2):18:decimal;"
          + "ss_coupon_amt:decimal(7,2):14:decimal;"
          + "ss_net_paid:decimal(7,2):19:decimal;"
          + "ss_net_paid_inc_tax:decimal(7,2):20:decimal;"
          + "ss_net_profit:decimal(7,2):21:decimal"),
  TIME_DIM(
      340,
      0,
      0x03L,
      false,
      false,
      1,
      1,
      new long[] {86400, 86400, 86400, 86400, 86400, 86400, 86400, 86400, 86400},
      new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2},
      "t_time_sk:bigint:0:key;"
          + "t_time_id:char(16):1:varchar;"
          + "t_time:integer:2:integer;"
          + "t_hour:integer:3:integer;"
          + "t_minute:integer:4:integer;"
          + "t_second:integer:5:integer;"
          + "t_am_pm:char(2):6:varchar;"
          + "t_shift:char(20):7:varchar;"
          + "t_sub_shift:char(20):8:varchar;"
          + "t_meal_time:char(20):9:varchar"),
  WAREHOUSE(
      351,
      200,
      0x03L,
      false,
      true,
      3,
      1,
      new long[] {5, 10, 15, 17, 20, 22, 25, 27, 30},
      new int[] {1, 1, 80, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 7},
      "w_warehouse_sk:bigint:0:key;"
          + "w_warehouse_id:char(16):1:varchar;"
          + "w_warehouse_name:varchar(20):2:varchar;"
          + "w_warehouse_sq_ft:integer:3:integer;"
          + "w_street_number:char(10):4:integer;"
          + "w_street_name:varchar(60):5:varchar;"
          + "w_street_type:char(15):6:varchar;"
          + "w_suite_number:char(10):7:varchar;"
          + "w_city:varchar(60):8:varchar;"
          + "w_county:varchar(30):9:varchar;"
          + "w_state:char(2):10:varchar;"
          + "w_zip:char(10):11:varchar;"
          + "w_country:varchar(20):12:varchar;"
          + "w_gmt_offset:decimal(5,2):13:integer"),
  WEB_PAGE(
      367,
      250,
      0x0BL,
      true,
      false,
      3,
      2,
      new long[] {30, 100, 1020, 1302, 1500, 1800, 2001, 2301, 2502},
      new int[] {1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1},
      "wp_web_page_sk:bigint:0:key;"
          + "wp_web_page_id:char(16):1:varchar;"
          + "wp_rec_start_date:date:2:date;"
          + "wp_rec_end_date:date:3:date;"
          + "wp_creation_date_sk:bigint:4:key;"
          + "wp_access_date_sk:bigint:5:key;"
          + "wp_autogen_flag:char(1):6:boolean;"
          + "wp_customer_sk:bigint:7:key;"
          + "wp_url:varchar(100):8:varchar;"
          + "wp_type:char(50):9:varchar;"
          + "wp_char_count:integer:10:integer;"
          + "wp_link_count:integer:11:integer;"
          + "wp_image_count:integer:12:integer;"
          + "wp_max_ad_count:integer:13:integer"),
  WEB_RETURNS(
      383,
      900,
      0x2004L,
      false,
      false,
      2,
      10000,
      new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0},
      new int[] {
        32, 32, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 80, 32
      },
      "wr_returned_date_sk:bigint:0:key;"
          + "wr_returned_time_sk:bigint:1:key;"
          + "wr_item_sk:bigint:2:key;"
          + "wr_refunded_customer_sk:bigint:3:key;"
          + "wr_refunded_cdemo_sk:bigint:4:key;"
          + "wr_refunded_hdemo_sk:bigint:5:key;"
          + "wr_refunded_addr_sk:bigint:6:key;"
          + "wr_returning_customer_sk:bigint:7:key;"
          + "wr_returning_cdemo_sk:bigint:8:key;"
          + "wr_returning_hdemo_sk:bigint:9:key;"
          + "wr_returning_addr_sk:bigint:10:key;"
          + "wr_web_page_sk:bigint:11:key;"
          + "wr_reason_sk:bigint:12:key;"
          + "wr_order_number:bigint:13:key;"
          + "wr_return_quantity:integer:14:integer;"
          + "wr_return_amt:decimal(7,2):15:decimal;"
          + "wr_return_tax:decimal(7,2):16:decimal;"
          + "wr_return_amt_inc_tax:decimal(7,2):17:decimal;"
          + "wr_fee:decimal(7,2):18:decimal;"
          + "wr_return_ship_cost:decimal(7,2):19:decimal;"
          + "wr_refunded_cash:decimal(7,2):20:decimal;"
          + "wr_reversed_charge:decimal(7,2):21:decimal;"
          + "wr_account_credit:decimal(7,2):22:decimal;"
          + "wr_net_loss:decimal(7,2):23:decimal"),
  WEB_SALES(
      409,
      5,
      0x20008L,
      false,
      false,
      2,
      1000,
      new long[] {60, 600, 6000, 18000, 60000, 180000, 600000, 1800000, 6000000},
      new int[] {
        2, 2, 16, 1, 1, 1, 1, 1, 2, 2, 1, 1, 16, 16, 16, 16, 16, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 128, 32, 16, 0
      },
      "ws_sold_date_sk:bigint:0:key;"
          + "ws_sold_time_sk:bigint:1:key;"
          + "ws_ship_date_sk:bigint:2:key;"
          + "ws_item_sk:bigint:3:key;"
          + "ws_bill_customer_sk:bigint:4:key;"
          + "ws_bill_cdemo_sk:bigint:5:key;"
          + "ws_bill_hdemo_sk:bigint:6:key;"
          + "ws_bill_addr_sk:bigint:7:key;"
          + "ws_ship_customer_sk:bigint:8:key;"
          + "ws_ship_cdemo_sk:bigint:9:key;"
          + "ws_ship_hdemo_sk:bigint:10:key;"
          + "ws_ship_addr_sk:bigint:11:key;"
          + "ws_web_page_sk:bigint:12:key;"
          + "ws_web_site_sk:bigint:13:key;"
          + "ws_ship_mode_sk:bigint:14:key;"
          + "ws_warehouse_sk:bigint:15:key;"
          + "ws_promo_sk:bigint:16:key;"
          + "ws_order_number:bigint:17:key;"
          + "ws_quantity:integer:18:integer;"
          + "ws_wholesale_cost:decimal(7,2):19:decimal;"
          + "ws_list_price:decimal(7,2):20:decimal;"
          + "ws_sales_price:decimal(7,2):21:decimal;"
          + "ws_ext_discount_amt:decimal(7,2):22:decimal;"
          + "ws_ext_sales_price:decimal(7,2):23:decimal;"
          + "ws_ext_wholesale_cost:decimal(7,2):24:decimal;"
          + "ws_ext_list_price:decimal(7,2):25:decimal;"
          + "ws_ext_tax:decimal(7,2):26:decimal;"
          + "ws_coupon_amt:decimal(7,2):27:decimal;"
          + "ws_ext_ship_cost:decimal(7,2):28:decimal;"
          + "ws_net_paid:decimal(7,2):29:decimal;"
          + "ws_net_paid_inc_tax:decimal(7,2):30:decimal;"
          + "ws_net_paid_inc_ship:decimal(7,2):31:decimal;"
          + "ws_net_paid_inc_ship_tax:decimal(7,2):32:decimal;"
          + "ws_net_profit:decimal(7,2):33:decimal"),
  WEB_SITE(
      447,
      100,
      0x0BL,
      true,
      true,
      3,
      2,
      new long[] {15, 21, 12, 21, 27, 33, 39, 42, 48},
      new int[] {
        1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 20, 100, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 7, 70
      },
      "web_site_sk:bigint:0:key;"
          + "web_site_id:char(16):1:varchar;"
          + "web_rec_start_date:date:2:date;"
          + "web_rec_end_date:date:3:date;"
          + "web_name:varchar(50):4:varchar;"
          + "web_open_date_sk:bigint:5:key;"
          + "web_close_date_sk:bigint:6:key;"
          + "web_class:varchar(50):7:varchar;"
          + "web_manager:varchar(40):8:varchar;"
          + "web_mkt_id:integer:9:integer;"
          + "web_mkt_class:varchar(50):10:varchar;"
          + "web_mkt_desc:varchar(100):11:varchar;"
          + "web_market_manager:varchar(40):12:varchar;"
          + "web_company_id:integer:13:integer;"
          + "web_company_name:char(50):14:varchar;"
          + "web_street_number:char(10):15:integer;"
          + "web_street_name:varchar(60):16:varchar;"
          + "web_street_type:char(15):17:varchar;"
          + "web_suite_number:char(10):18:varchar;"
          + "web_city:varchar(60):19:varchar;"
          + "web_county:varchar(30):20:varchar;"
          + "web_state:char(2):21:varchar;"
          + "web_zip:char(10):22:varchar;"
          + "web_country:varchar(20):23:varchar;"
          + "web_gmt_offset:decimal(5,2):24:integer;"
          + "web_tax_percentage:decimal(5,2):25:decimal");
  final int firstColumn;
  final int nullThreshold;
  final long requiredBits;
  final boolean history;
  private final boolean small;
  final int scalingModel;
  final long multiplier;
  final long[] sizes;
  final int[] budgets;
  private final Column[] columns;
  private final long[] outputNullMasks = new long[Long.SIZE];
  final long sentinelColumns;

  TpcdsTable(
      int firstColumn,
      int nullThreshold,
      long requiredBits,
      boolean history,
      boolean small,
      int scalingModel,
      long multiplier,
      long[] sizes,
      int[] budgets,
      String schema) {
    this.firstColumn = firstColumn;
    this.nullThreshold = nullThreshold;
    this.requiredBits = requiredBits;
    this.history = history;
    this.small = small;
    this.scalingModel = scalingModel;
    this.multiplier = multiplier;
    this.sizes = sizes;
    this.budgets = budgets;
    String[] fields = schema.split(";");
    columns = new Column[fields.length];
    long sentinels = 0;
    for (int i = 0; i < fields.length; i++) {
      String[] parts = fields[i].split(":");
      columns[i] =
          new Column(parts[0], parts[1], Integer.parseInt(parts[2]), parts[3], requiredBits);
      outputNullMasks[columns[i].nullBit] |= 1L << i;
      if (parts[3].equals("key") || parts[3].equals("date")) sentinels |= 1L << i;
    }
    sentinelColumns = sentinels;
  }

  long outputNulls(long bitmap) {
    long result = 0;
    for (; bitmap != 0; bitmap &= bitmap - 1) {
      result |= outputNullMasks[Long.numberOfTrailingZeros(bitmap)];
    }
    return result;
  }

  public String getName() {
    return name().toLowerCase(Locale.ROOT);
  }

  public static TpcdsTable getTable(String name) {
    return valueOf(name.toUpperCase(Locale.ROOT));
  }

  public Column[] getColumns() {
    return columns.clone();
  }

  public Column column(int ordinal) {
    return columns[ordinal];
  }

  public int columnCount() {
    return columns.length;
  }

  public boolean isSmall() {
    return small;
  }

  public TpcdsTable parent() {
    switch (this) {
      case CATALOG_RETURNS:
        return CATALOG_SALES;
      case STORE_RETURNS:
        return STORE_SALES;
      case WEB_RETURNS:
        return WEB_SALES;
      default:
        return this;
    }
  }

  public static final class Column {
    private final String name;
    private final String type;
    final int nullBit;
    final String printKind;
    private final boolean nullable;

    Column(String name, String type, int nullBit, String printKind, long requiredBits) {
      this.printKind = printKind;
      this.name = name;
      this.type = type;
      this.nullBit = nullBit;
      nullable = (requiredBits & (1L << nullBit)) == 0 || name.endsWith("rec_end_date");
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public boolean isNullable() {
      return nullable;
    }
  }
}
