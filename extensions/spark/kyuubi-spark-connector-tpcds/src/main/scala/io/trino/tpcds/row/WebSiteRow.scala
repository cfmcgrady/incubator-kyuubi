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

import io.trino.tpcds.generator.WebSiteGeneratorColumn._
import io.trino.tpcds.`type`.{Address, Decimal}

import java.util.{List => JList}

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow

class WebSiteRow(
                  nullBitMap: Long,
                  private val webSiteSk: Long,
                  private val webSiteId: String,
                  private val webRecStartDateId: Long,
                  private val webRecEndDateId: Long,
                  private val webName: String,
                  private val webOpenDate: Long,
                  private val webCloseDate: Long,
                  private val webClass: String,
                  private val webManager: String,
                  private val webMarketId: Int,
                  private val webMarketClass: String,
                  private val webMarketDesc: String,
                  private val webMarketManager: String,
                  private val webCompanyId: Int,
                  private val webCompanyName: String,
                  private val webAddress: Address,
                  private val webTaxPercentage: Decimal
  ) extends TableRowWithNulls(nullBitMap, WEB_SITE_SK)
  with KyuubiTableRowWithNulls {

    override val nullBitMapInternal = nullBitMap
    override val firstColumnInternal = WEB_SITE_SK

  def getWebName: String = webName
  def getWebOpenDate: Long = webOpenDate
  def getWebCloseDate: Long = webCloseDate
  def getWebClass: String = webClass
  def getWebManager: String = webManager
  def getWebMarketId: Int = webMarketId
  def getWebMarketClass: String = webMarketClass
  def getWebMarketDesc: String = webMarketDesc
  def getWebMarketManager: String = webMarketManager
  def getWebCompanyId: Int = webCompanyId
  def getWebCompanyName: String = webCompanyName
  def getWebAddress: Address = webAddress
  def getWebTaxPercentage: Decimal = webTaxPercentage
  def getWebSiteSk: Long = webSiteSk
  def getWebSiteId: String = webSiteId
  def getWebRecStartDateId: Long = webRecStartDateId
  def getWebRecEndDateId: Long = webRecEndDateId

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
    getLongOrNull(webSiteSk, WEB_SITE_SK),
    getStringOrNullInternal(webSiteId, WEB_SITE_ID),
    getDateOrNullFromJulianDays(webRecStartDateId, WEB_REC_START_DATE_ID),
    getDateOrNullFromJulianDays(webRecEndDateId, WEB_REC_END_DATE_ID),
    getStringOrNullInternal(webName, WEB_NAME),
    getLongOrNull(webOpenDate, WEB_OPEN_DATE),
    getLongOrNull(webCloseDate, WEB_CLOSE_DATE),
    getStringOrNullInternal(webClass, WEB_CLASS),
    getStringOrNullInternal(webManager, WEB_MANAGER),
    getIntOrNull(webMarketId, WEB_MARKET_ID),
    getStringOrNullInternal(webMarketClass, WEB_MARKET_CLASS),
    getStringOrNullInternal(webMarketDesc, WEB_MARKET_DESC),
    getStringOrNullInternal(webMarketManager, WEB_MARKET_MANAGER),
    getIntOrNull(webCompanyId, WEB_COMPANY_ID),
    getStringOrNullInternal(webCompanyName, WEB_COMPANY_NAME),
    getStringOrNullInternal(webAddress.getStreetNumber.toString, WEB_ADDRESS_STREET_NUM),
    getStringOrNullInternal(webAddress.getStreetName(), WEB_ADDRESS_STREET_NAME1),
    getStringOrNullInternal(webAddress.getStreetType(), WEB_ADDRESS_STREET_TYPE),
    getStringOrNullInternal(webAddress.getSuiteNumber(), WEB_ADDRESS_SUITE_NUM),
    getStringOrNullInternal(webAddress.getCity(), WEB_ADDRESS_CITY),
    getStringOrNullInternal(webAddress.getCounty(), WEB_ADDRESS_COUNTY),
    getStringOrNullInternal(webAddress.getState(), WEB_ADDRESS_STATE),
    getStringOrNullInternal(f"${webAddress.getZip}%05d", WEB_ADDRESS_ZIP),
    getStringOrNullInternal(webAddress.getCountry(), WEB_ADDRESS_COUNTRY),
    getDecimalOrNull(webAddress.getGmtOffset(), WEB_ADDRESS_GMT_OFFSET, 5, 2),
    getDecimalOrNull(webTaxPercentage, WEB_TAX_PERCENTAGE, 5, 2)))
}
