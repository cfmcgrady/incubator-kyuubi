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

/** Column stream identifiers in the fixed reference format. */
final class StreamIds {
  static final int CC_OPEN_DATE_ID = 6;
  static final int CC_CLASS = 8;
  static final int CC_EMPLOYEES = 9;
  static final int CC_SQ_FT = 10;
  static final int CC_HOURS = 11;
  static final int CC_MANAGER = 12;
  static final int CC_MARKET_ID = 13;
  static final int CC_MARKET_CLASS = 14;
  static final int CC_MARKET_DESC = 15;
  static final int CC_MARKET_MANAGER = 16;
  static final int CC_COMPANY = 19;
  static final int CC_ADDRESS = 31;
  static final int CC_TAX_PERCENTAGE = 32;
  static final int CC_SCD = 33;
  static final int CC_NULLS = 34;
  static final int CP_DESCRIPTION = 43;
  static final int CP_NULLS = 45;
  static final int CR_RETURNED_DATE_SK = 46;
  static final int CR_RETURNED_TIME_SK = 47;
  static final int CR_RETURNING_CUSTOMER_SK = 53;
  static final int CR_SHIP_MODE_SK = 59;
  static final int CR_WAREHOUSE_SK = 60;
  static final int CR_REASON_SK = 61;
  static final int CR_NULLS = 73;
  static final int CR_PRICING = 74;
  static final int CS_SOLD_TIME_SK = 76;
  static final int CS_SHIP_DATE_SK = 77;
  static final int CS_BILL_CUSTOMER_SK = 78;
  static final int CS_SHIP_CUSTOMER_SK = 82;
  static final int CS_CALL_CENTER_SK = 86;
  static final int CS_CATALOG_PAGE_SK = 87;
  static final int CS_SHIP_MODE_SK = 88;
  static final int CS_WAREHOUSE_SK = 89;
  static final int CS_SOLD_ITEM_SK = 90;
  static final int CS_PROMO_SK = 91;
  static final int CS_ORDER_NUMBER = 92;
  static final int CS_PRICING = 109;
  static final int CS_PERMUTE = 110;
  static final int CS_NULLS = 111;
  static final int CR_IS_RETURNED = 112;
  static final int C_CURRENT_CDEMO_SK = 116;
  static final int C_CURRENT_HDEMO_SK = 117;
  static final int C_CURRENT_ADDR_SK = 118;
  static final int C_FIRST_SALES_DATE_ID = 120;
  static final int C_SALUTATION = 121;
  static final int C_FIRST_NAME = 122;
  static final int C_LAST_NAME = 123;
  static final int C_PREFERRED_CUST_FLAG = 124;
  static final int C_BIRTH_DAY = 125;
  static final int C_BIRTH_COUNTRY = 128;
  static final int C_EMAIL_ADDRESS = 130;
  static final int C_LAST_REVIEW_DATE = 131;
  static final int C_NULLS = 132;
  static final int CA_LOCATION_TYPE = 145;
  static final int CA_NULLS = 146;
  static final int CA_ADDRESS = 147;
  static final int CD_NULLS = 158;
  static final int D_NULLS = 187;
  static final int HD_NULLS = 193;
  static final int IB_NULLS = 197;
  static final int INV_QUANTITY_ON_HAND = 201;
  static final int INV_NULLS = 202;
  static final int I_ITEM_DESC = 207;
  static final int I_CURRENT_PRICE = 208;
  static final int I_WHOLESALE_COST = 209;
  static final int I_CLASS = 213;
  static final int I_CATEGORY = 215;
  static final int I_MANUFACT_ID = 216;
  static final int I_SIZE = 218;
  static final int I_FORMULATION = 219;
  static final int I_COLOR = 220;
  static final int I_UNITS = 221;
  static final int I_MANAGER_ID = 223;
  static final int I_NULLS = 225;
  static final int I_SCD = 226;
  static final int I_PROMO_SK = 227;
  static final int P_START_DATE_ID = 230;
  static final int P_END_DATE_ID = 231;
  static final int P_ITEM_SK = 232;
  static final int P_CHANNEL_DMAIL = 236;
  static final int P_CHANNEL_DETAILS = 244;
  static final int P_PURPOSE = 245;
  static final int P_NULLS = 247;
  static final int R_NULLS = 251;
  static final int SM_CONTRACT = 256;
  static final int SM_NULLS = 258;
  static final int W_STORE_CLOSED_DATE_ID = 263;
  static final int W_STORE_EMPLOYEES = 265;
  static final int W_STORE_FLOOR_SPACE = 266;
  static final int W_STORE_HOURS = 267;
  static final int W_STORE_MANAGER = 268;
  static final int W_STORE_MARKET_ID = 269;
  static final int W_STORE_TAX_PERCENTAGE = 270;
  static final int W_STORE_GEOGRAPHY_CLASS = 271;
  static final int W_STORE_MARKET_DESC = 272;
  static final int W_STORE_MARKET_MANAGER = 273;
  static final int W_STORE_DIVISION_NAME = 275;
  static final int W_STORE_COMPANY_NAME = 277;
  static final int W_STORE_NULLS = 288;
  static final int W_STORE_TYPE = 289;
  static final int W_STORE_SCD = 290;
  static final int W_STORE_ADDRESS = 291;
  static final int T_NULLS = 350;
  static final int W_WAREHOUSE_NAME = 353;
  static final int W_WAREHOUSE_SQ_FT = 354;
  static final int W_NULLS = 365;
  static final int W_WAREHOUSE_ADDRESS = 366;
  static final int WP_CREATION_DATE_SK = 371;
  static final int WP_ACCESS_DATE_SK = 372;
  static final int WP_AUTOGEN_FLAG = 373;
  static final int WP_CUSTOMER_SK = 374;
  static final int WP_TYPE = 376;
  static final int WP_CHAR_COUNT = 377;
  static final int WP_LINK_COUNT = 378;
  static final int WP_IMAGE_COUNT = 379;
  static final int WP_MAX_AD_COUNT = 380;
  static final int WP_NULLS = 381;
  static final int WP_SCD = 382;
  static final int WR_RETURNED_DATE_SK = 383;
  static final int WR_RETURNED_TIME_SK = 384;
  static final int WR_REFUNDED_CUSTOMER_SK = 386;
  static final int WR_RETURNING_CUSTOMER_SK = 390;
  static final int WR_REASON_SK = 395;
  static final int WR_PRICING = 407;
  static final int WR_NULLS = 408;
  static final int WS_SOLD_DATE_SK = 409;
  static final int WS_SOLD_TIME_SK = 410;
  static final int WS_SHIP_DATE_SK = 411;
  static final int WS_ITEM_SK = 412;
  static final int WS_BILL_CUSTOMER_SK = 413;
  static final int WS_SHIP_CUSTOMER_SK = 417;
  static final int WS_WEB_PAGE_SK = 421;
  static final int WS_WEB_SITE_SK = 422;
  static final int WS_SHIP_MODE_SK = 423;
  static final int WS_WAREHOUSE_SK = 424;
  static final int WS_PROMO_SK = 425;
  static final int WS_ORDER_NUMBER = 426;
  static final int WS_PRICING = 443;
  static final int WS_NULLS = 444;
  static final int WR_IS_RETURNED = 445;
  static final int WS_PERMUTATION = 446;
  static final int WEB_OPEN_DATE = 452;
  static final int WEB_CLOSE_DATE = 453;
  static final int WEB_MANAGER = 455;
  static final int WEB_MARKET_ID = 456;
  static final int WEB_MARKET_CLASS = 457;
  static final int WEB_MARKET_DESC = 458;
  static final int WEB_MARKET_MANAGER = 459;
  static final int WEB_COMPANY_ID = 460;
  static final int WEB_TAX_PERCENTAGE = 472;
  static final int WEB_NULLS = 473;
  static final int WEB_ADDRESS = 474;
  static final int WEB_SCD = 475;

  private StreamIds() {}
}
