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

package io.trino.tpcds.row

import io.trino.tpcds.generator.StoreGeneratorColumn._
import io.trino.tpcds.`type`.Address
import io.trino.tpcds.`type`.Decimal
import java.util.{List => JList}

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow

class StoreRow(
                nullBitMap: Long,
                private val storeSk: Long,
                private val storeId: String,
                private val recStartDateId: Long,
                private val recEndDateId: Long,
                private val closedDateId: Long,
                private val storeName: String,
                private val employees: Int,
                private val floorSpace: Int,
                private val hours: String,
                private val storeManager: String,
                private val marketId: Int,
                private val dTaxPercentage: Decimal,
                private val geographyClass: String,
                private val marketDesc: String,
                private val marketManager: String,
                private val divisionId: Long,
                private val divisionName: String,
                private val companyId: Long,
                private val companyName: String,
                private val address: Address,
  ) extends TableRowWithNulls(nullBitMap, W_STORE_SK)
  with KyuubiTableRowWithNulls {

    override val nullBitMapInternal = nullBitMap
    override val firstColumnInternal = W_STORE_SK

  def getStoreSk: Long = storeSk
  def getStoreId: String = storeId
  def getRecStartDateId: Long = recStartDateId
  def getRecEndDateId: Long = recEndDateId
  def getClosedDateId: Long = closedDateId
  def getStoreName: String = storeName
  def getEmployees: Int = employees
  def getFloorSpace: Int = floorSpace
  def getHours: String = hours
  def getStoreManager: String = storeManager
  def getMarketId: Int = marketId
  def getDTaxPercentage: Decimal = dTaxPercentage
  def getGeographyClass: String = geographyClass
  def getMarketDesc: String = marketDesc
  def getMarketManager: String = marketManager
  def getDivisionId: Long = divisionId
  def getDivisionName: String = divisionName
  def getCompanyId: Long = companyId
  def getCompanyName: String = companyName
  def getAddress: Address = address

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
    getLongOrNull(storeSk, W_STORE_SK),
    getStringOrNullInternal(storeId, W_STORE_ID),
    getDateOrNullFromJulianDays(recStartDateId, W_STORE_REC_START_DATE_ID),
    getDateOrNullFromJulianDays(recEndDateId, W_STORE_REC_END_DATE_ID),
    getLongOrNull(closedDateId, W_STORE_CLOSED_DATE_ID),
    getStringOrNullInternal(storeName, W_STORE_NAME),
    getIntOrNull(employees, W_STORE_EMPLOYEES),
    getIntOrNull(floorSpace, W_STORE_FLOOR_SPACE),
    getStringOrNullInternal(hours, W_STORE_HOURS),
    getStringOrNullInternal(storeManager, W_STORE_MANAGER),
    getIntOrNull(marketId, W_STORE_MARKET_ID),
    getStringOrNullInternal(geographyClass, W_STORE_GEOGRAPHY_CLASS),
    getStringOrNullInternal(marketDesc, W_STORE_MARKET_DESC),
    getStringOrNullInternal(marketManager, W_STORE_MARKET_MANAGER),
    getIntOrNull(divisionId, W_STORE_DIVISION_ID),
    getStringOrNullInternal(divisionName, W_STORE_DIVISION_NAME),
    getIntOrNull(companyId, W_STORE_COMPANY_ID),
    getStringOrNullInternal(companyName, W_STORE_COMPANY_NAME),
    getStringOrNullInternal(address.getStreetNumber.toString, W_STORE_ADDRESS_STREET_NUM),
    getStringOrNullInternal(address.getStreetName, W_STORE_ADDRESS_STREET_NAME1),
    getStringOrNullInternal(address.getStreetType, W_STORE_ADDRESS_STREET_TYPE),
    getStringOrNullInternal(address.getSuiteNumber, W_STORE_ADDRESS_SUITE_NUM),
    getStringOrNullInternal(address.getCity, W_STORE_ADDRESS_CITY),
    getStringOrNullInternal(address.getCounty, W_STORE_ADDRESS_COUNTY),
    getStringOrNullInternal(address.getState, W_STORE_ADDRESS_STATE),
    getStringOrNullInternal(f"${address.getZip}%05d", W_STORE_ADDRESS_ZIP),
    getStringOrNullInternal(address.getCountry, W_STORE_ADDRESS_COUNTRY),
    getDecimalOrNull(address.getGmtOffset, W_STORE_ADDRESS_GMT_OFFSET, 5, 2),
    getDecimalOrNull(dTaxPercentage, W_STORE_TAX_PERCENTAGE, 5, 2)))
}
