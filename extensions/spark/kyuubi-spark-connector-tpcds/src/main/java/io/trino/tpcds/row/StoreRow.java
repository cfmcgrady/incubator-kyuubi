/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.tpcds.row;

import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_ADDRESS_CITY;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_ADDRESS_COUNTRY;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_ADDRESS_COUNTY;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_ADDRESS_GMT_OFFSET;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_ADDRESS_STATE;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_ADDRESS_STREET_NAME1;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_ADDRESS_STREET_NUM;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_ADDRESS_STREET_TYPE;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_ADDRESS_SUITE_NUM;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_ADDRESS_ZIP;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_CLOSED_DATE_ID;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_COMPANY_ID;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_COMPANY_NAME;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_DIVISION_ID;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_DIVISION_NAME;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_EMPLOYEES;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_FLOOR_SPACE;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_GEOGRAPHY_CLASS;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_HOURS;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_ID;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_MANAGER;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_MARKET_DESC;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_MARKET_ID;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_MARKET_MANAGER;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_NAME;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_REC_END_DATE_ID;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_REC_START_DATE_ID;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_SK;
import static io.trino.tpcds.generator.StoreGeneratorColumn.W_STORE_TAX_PERCENTAGE;
import static java.lang.String.format;

import io.trino.tpcds.type.Address;
import io.trino.tpcds.type.Decimal;
import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

public class StoreRow extends KyuubiTPCDSTableRowWithNulls {

  private final long storeSk;
  private final String storeId;
  private final long recStartDateId;
  private final long recEndDateId;
  private final long closedDateId;
  private final String storeName;
  private final int employees;
  private final int floorSpace;
  private final String hours;
  private final String storeManager;
  private final int marketId;
  private final Decimal dTaxPercentage;
  private final String geographyClass;
  private final String marketDesc;
  private final String marketManager;
  private final long divisionId;
  private final String divisionName;
  private final long companyId;
  private final String companyName;
  private final Address address;

  public StoreRow(
      long nullBitMap,
      long storeSk,
      String storeId,
      long recStartDateId,
      long recEndDateId,
      long closedDateId,
      String storeName,
      int employees,
      int floorSpace,
      String hours,
      String storeManager,
      int marketId,
      Decimal dTaxPercentage,
      String geographyClass,
      String marketDesc,
      String marketManager,
      long divisionId,
      String divisionName,
      long companyId,
      String companyName,
      Address address) {
    super(nullBitMap, W_STORE_SK);
    this.storeSk = storeSk;
    this.storeId = storeId;
    this.recStartDateId = recStartDateId;
    this.recEndDateId = recEndDateId;
    this.closedDateId = closedDateId;
    this.storeName = storeName;
    this.employees = employees;
    this.floorSpace = floorSpace;
    this.hours = hours;
    this.storeManager = storeManager;
    this.marketId = marketId;
    this.dTaxPercentage = dTaxPercentage;
    this.geographyClass = geographyClass;
    this.marketDesc = marketDesc;
    this.marketManager = marketManager;
    this.divisionId = divisionId;
    this.divisionName = divisionName;
    this.companyId = companyId;
    this.companyName = companyName;
    this.address = address;
  }

  public long getStoreSk() {
    return storeSk;
  }

  public String getStoreId() {
    return storeId;
  }

  public long getRecStartDateId() {
    return recStartDateId;
  }

  public long getRecEndDateId() {
    return recEndDateId;
  }

  public String getGeographyClass() {
    return geographyClass;
  }

  public long getDivisionId() {
    return divisionId;
  }

  public String getDivisionName() {
    return divisionName;
  }

  public long getCompanyId() {
    return companyId;
  }

  public String getCompanyName() {
    return companyName;
  }

  public long getClosedDateId() {
    return closedDateId;
  }

  public String getStoreName() {
    return storeName;
  }

  public int getEmployees() {
    return employees;
  }

  public int getFloorSpace() {
    return floorSpace;
  }

  public String getHours() {
    return hours;
  }

  public String getStoreManager() {
    return storeManager;
  }

  public int getMarketId() {
    return marketId;
  }

  public Decimal getdTaxPercentage() {
    return dTaxPercentage;
  }

  public String getMarketDesc() {
    return marketDesc;
  }

  public String getMarketManager() {
    return marketManager;
  }

  public Address getAddress() {
    return address;
  }

  @Override
  public Object[] values() {
    return new Object[] {
      getOrNullForKey(storeSk, W_STORE_SK),
      getOrNull(storeId, W_STORE_ID),
      getDateOrNullFromJulianDays(recStartDateId, W_STORE_REC_START_DATE_ID),
      getDateOrNullFromJulianDays(recEndDateId, W_STORE_REC_END_DATE_ID),
      getOrNullForKey(closedDateId, W_STORE_CLOSED_DATE_ID),
      getOrNull(storeName, W_STORE_NAME),
      getOrNull(employees, W_STORE_EMPLOYEES),
      getOrNull(floorSpace, W_STORE_FLOOR_SPACE),
      getOrNull(hours, W_STORE_HOURS),
      getOrNull(storeManager, W_STORE_MANAGER),
      getOrNull(marketId, W_STORE_MARKET_ID),
      getOrNull(geographyClass, W_STORE_GEOGRAPHY_CLASS),
      getOrNull(marketDesc, W_STORE_MARKET_DESC),
      getOrNull(marketManager, W_STORE_MARKET_MANAGER),
      getOrNullForKey(divisionId, W_STORE_DIVISION_ID),
      getOrNull(divisionName, W_STORE_DIVISION_NAME),
      getOrNullForKey(companyId, W_STORE_COMPANY_ID),
      getOrNull(companyName, W_STORE_COMPANY_NAME),
      getOrNull(address.getStreetNumber(), W_STORE_ADDRESS_STREET_NUM),
      getOrNull(address.getStreetName(), W_STORE_ADDRESS_STREET_NAME1),
      getOrNull(address.getStreetType(), W_STORE_ADDRESS_STREET_TYPE),
      getOrNull(address.getSuiteNumber(), W_STORE_ADDRESS_SUITE_NUM),
      getOrNull(address.getCity(), W_STORE_ADDRESS_CITY),
      getOrNull(address.getCounty(), W_STORE_ADDRESS_COUNTY),
      getOrNull(address.getState(), W_STORE_ADDRESS_STATE),
      getOrNull(format("%05d", address.getZip()), W_STORE_ADDRESS_ZIP),
      getOrNull(address.getCountry(), W_STORE_ADDRESS_COUNTRY),
      getOrNull(address.getGmtOffset(), W_STORE_ADDRESS_GMT_OFFSET),
      getOrNull(dTaxPercentage, W_STORE_TAX_PERCENTAGE)
    };
  }
}
