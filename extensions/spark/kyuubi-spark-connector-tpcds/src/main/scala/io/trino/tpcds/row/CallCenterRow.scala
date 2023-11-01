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

import java.util.{List => JList}

import io.trino.tpcds.`type`.{Address, Decimal}
import io.trino.tpcds.generator.CallCenterGeneratorColumn._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class CallCenterRow(
    private val ccCallCenterSk: Long,
    private val ccCallCenterId: String,
    private val ccRecStartDateId: Long,
    private val ccRecEndDateId: Long,
    private val ccClosedDateId: Long,
    private val ccOpenDateId: Long,
    private val ccName: String,
    private val ccClass: String,
    private val ccEmployees: Int,
    private val ccSqFt: Int,
    private val ccHours: String,
    private val ccManager: String,
    private val ccMarketId: Int,
    private val ccMarketClass: String,
    private val ccMarketDesc: String,
    private val ccMarketManager: String,
    private val ccDivisionId: Int,
    private val ccDivisionName: String,
    private val ccCompany: Int,
    private val ccCompanyName: String,
    private val ccAddress: Address,
    private val ccTaxPercentage: Decimal,
    nullBitMap: Long) extends TableRowWithNulls(nullBitMap, CC_CALL_CENTER_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = CC_CALL_CENTER_SK

  def getCcCallCenterSk: Long = ccCallCenterSk
  def getCcCallCenterId: String = ccCallCenterId
  def getCcRecStartDateId: Long = ccRecStartDateId
  def getCcRecEndDateId: Long = ccRecEndDateId
  def getCcClosedDateId: Long = ccClosedDateId
  def getCcOpenDateId: Long = ccOpenDateId
  def getCcName: String = ccName
  def getCcClass: String = ccClass
  def getCcEmployees: Int = ccEmployees
  def getCcSqFt: Int = ccSqFt
  def getCcHours: String = ccHours
  def getCcManager: String = ccManager
  def getCcMarketId: Int = ccMarketId
  def getCcMarketClass: String = ccMarketClass
  def getCcMarketDesc: String = ccMarketDesc
  def getCcMarketManager: String = ccMarketManager
  def getCcDivisionId: Int = ccDivisionId
  def getCcDivisionName: String = ccDivisionName
  def getCcCompany: Int = ccCompany
  def getCcCompanyName: String = ccCompanyName
  def getCcAddress: Address = ccAddress
  def getCcTaxPercentage: Decimal = ccTaxPercentage

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(ccCallCenterSk, CC_CALL_CENTER_SK),
        getStringOrNullInternal(ccCallCenterId, CC_CALL_CENTER_ID),
        getDateOrNullFromJulianDays(ccRecStartDateId, CC_REC_START_DATE_ID),
        getDateOrNullFromJulianDays(ccRecEndDateId, CC_REC_END_DATE_ID),
        getIntOrNullForKey(ccClosedDateId, CC_CLOSED_DATE_ID),
        getIntOrNullForKey(ccOpenDateId, CC_OPEN_DATE_ID),
        getStringOrNullInternal(ccName, CC_NAME),
        getStringOrNullInternal(ccClass, CC_CLASS),
        getIntOrNull(ccEmployees, CC_EMPLOYEES),
        getIntOrNull(ccSqFt, CC_SQ_FT),
        getStringOrNullInternal(ccHours, CC_HOURS),
        getStringOrNullInternal(ccManager, CC_MANAGER),
        getIntOrNull(ccMarketId, CC_MARKET_ID),
        getStringOrNullInternal(ccMarketClass, CC_MARKET_CLASS),
        getStringOrNullInternal(ccMarketDesc, CC_MARKET_DESC),
        getStringOrNullInternal(ccMarketManager, CC_MARKET_MANAGER),
        getIntOrNull(ccDivisionId, CC_DIVISION),
        getStringOrNullInternal(ccDivisionName, CC_DIVISION_NAME),
        getIntOrNull(ccCompany, CC_COMPANY),
        getStringOrNullInternal(ccCompanyName, CC_COMPANY_NAME),
        getStringOrNullInternal(ccAddress.getStreetNumber.toString, CC_STREET_NUMBER),
        getStringOrNullInternal(ccAddress.getStreetName, CC_STREET_NAME),
        getStringOrNullInternal(ccAddress.getStreetType, CC_STREET_TYPE),
        getStringOrNullInternal(ccAddress.getSuiteNumber, CC_SUITE_NUMBER),
        getStringOrNullInternal(ccAddress.getCity, CC_CITY),
        getStringOrNullInternal(ccAddress.getCounty, CC_ADDRESS),
        getStringOrNullInternal(ccAddress.getState, CC_STATE),
        getStringOrNullInternal(f"${ccAddress.getZip}%05d", CC_ZIP),
        getStringOrNullInternal(ccAddress.getCountry, CC_COUNTRY),
        getDecimalOrNull(ccAddress.getGmtOffset, CC_GMT_OFFSET, 5, 2),
        getDecimalOrNull(ccTaxPercentage, CC_TAX_PERCENTAGE, 5, 2)))
}

object CallCenterRow {
  class Builder {
    private var ccCallCenterSk = 0L
    private var ccCallCenterId: String = null
    private var ccRecStartDateId = 0L
    private var ccRecEndDateId = 0L
    private var ccClosedDateId = 0L
    private var ccOpenDateId = 0L
    private var ccName: String = null
    private var ccClass: String = null
    private var ccEmployees = 0
    private var ccSqFt = 0
    private var ccHours: String = null
    private var ccManager: String = null
    private var ccMarketId = 0
    private var ccMarketClass: String = null
    private var ccMarketDesc: String = null
    private var ccMarketManager: String = null
    private var ccDivisionId = 0
    private var ccDivisionName: String = null
    private var ccCompany = 0
    private var ccCompanyName: String = null
    private var ccAddress: Address = null
    private var ccTaxPercentage: Decimal = null
    private var nullBitMap = 0L

    def setCcCallCenterSk(ccCallCenterSk: Long): Builder = {
      this.ccCallCenterSk = ccCallCenterSk
      this
    }

    def setCcCallCenterId(ccCallCenterId: String): Builder = {
      this.ccCallCenterId = ccCallCenterId
      this
    }

    def setCcRecStartDateId(ccRecStartDateId: Long): Builder = {
      this.ccRecStartDateId = ccRecStartDateId
      this
    }

    def setCcRecEndDateId(ccRecEndDateId: Long): Builder = {
      this.ccRecEndDateId = ccRecEndDateId
      this
    }

    def setCcClosedDateId(ccClosedDateId: Long): Builder = {
      this.ccClosedDateId = ccClosedDateId
      this
    }

    def setCcOpenDateId(ccOpenDateId: Long): Builder = {
      this.ccOpenDateId = ccOpenDateId
      this
    }

    def setCcName(ccName: String): Builder = {
      this.ccName = ccName
      this
    }

    def setCcClass(ccClass: String): Builder = {
      this.ccClass = ccClass
      this
    }

    def setCcEmployees(ccEmployees: Int): Builder = {
      this.ccEmployees = ccEmployees
      this
    }

    def setCcSqFt(ccSqFt: Int): Builder = {
      this.ccSqFt = ccSqFt
      this
    }

    def setCcHours(ccHours: String): Builder = {
      this.ccHours = ccHours
      this
    }

    def setCcManager(ccManager: String): Builder = {
      this.ccManager = ccManager
      this
    }

    def setCcMarketId(ccMarketId: Int): Builder = {
      this.ccMarketId = ccMarketId
      this
    }

    def setCcMarketClass(ccMarketClass: String): Builder = {
      this.ccMarketClass = ccMarketClass
      this
    }

    def setCcMarketDesc(ccMarketDesc: String): Builder = {
      this.ccMarketDesc = ccMarketDesc
      this
    }

    def setCcMarketManager(ccMarketManager: String): Builder = {
      this.ccMarketManager = ccMarketManager
      this
    }

    def setCcDivisionId(ccDivisionId: Int): Builder = {
      this.ccDivisionId = ccDivisionId
      this
    }

    def setCcDivisionName(ccDivisionName: String): Builder = {
      this.ccDivisionName = ccDivisionName
      this
    }

    def setCcCompany(ccCompany: Int): Builder = {
      this.ccCompany = ccCompany
      this
    }

    def setCcCompanyName(ccCompanyName: String): Builder = {
      this.ccCompanyName = ccCompanyName
      this
    }

    def setCcAddress(ccAddress: Address): Builder = {
      this.ccAddress = ccAddress
      this
    }

    def setCcTaxPercentage(ccTaxPercentage: Decimal): Builder = {
      this.ccTaxPercentage = ccTaxPercentage
      this
    }

    def setNullBitMap(nullBitMap: Long): Unit = {
      this.nullBitMap = nullBitMap
    }

    def build(): CallCenterRow = {
      new CallCenterRow(
        ccCallCenterSk,
        ccCallCenterId,
        ccRecStartDateId,
        ccRecEndDateId,
        ccClosedDateId,
        ccOpenDateId,
        ccName,
        ccClass,
        ccEmployees,
        ccSqFt,
        ccHours,
        ccManager,
        ccMarketId,
        ccMarketClass,
        ccMarketDesc,
        ccMarketManager,
        ccDivisionId,
        ccDivisionName,
        ccCompany,
        ccCompanyName,
        ccAddress,
        ccTaxPercentage,
        nullBitMap)
    }
  }
}
