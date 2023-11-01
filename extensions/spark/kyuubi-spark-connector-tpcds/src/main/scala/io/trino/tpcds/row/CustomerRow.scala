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

import io.trino.tpcds.generator.CustomerGeneratorColumn._

import java.util.{List => JList}

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow

class CustomerRow(
    private val cCustomerSk: Long,
    private val cCustomerId: String,
    private val cCurrentCdemoSk: Long,
    private val cCurrentHdemoSk: Long,
    private val cCurrentAddrSk: Long,
    private val cFirstShiptoDateId: Int,
    private val cFirstSalesDateId: Int,
    private val cSalutation: String,
    private val cFirstName: String,
    private val cLastName: String,
    private val cPreferredCustFlag: Boolean,
    private val cBirthDay: Int,
    private val cBirthMonth: Int,
    private val cBirthYear: Int,
    private val cBirthCountry: String,
    private val cEmailAddress: String,
    private val cLastReviewDate: Int,
    nullBitMap: Long) extends TableRowWithNulls(nullBitMap, C_CUSTOMER_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = C_CUSTOMER_SK

  val cLogin: String = null; // never gets set to anything

  def getCCustomerSk: Long = cCustomerSk
  def getCCustomerId: String = cCustomerId
  def getCCurrentCdemoSk: Long = cCurrentCdemoSk
  def getCCurrentHdemoSk: Long = cCurrentHdemoSk
  def getCCurrentAddrSk: Long = cCurrentAddrSk
  def getCFirstShiptoDateId: Int = cFirstShiptoDateId
  def getCFirstSalesDateId: Int = cFirstSalesDateId
  def getCSalutation: String = cSalutation
  def getCFirstName: String = cFirstName
  def getCLastName: String = cLastName
  def isCPreferredCustFlag: Boolean = cPreferredCustFlag
  def getCBirthDay: Int = cBirthDay
  def getCBirthMonth: Int = cBirthMonth
  def getCBirthYear: Int = cBirthYear
  def getCBirthCountry: String = cBirthCountry
  def getCLogin: String = cLogin
  def getCEmailAddress: String = cEmailAddress
  def getCLastReviewDate: Int = cLastReviewDate

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(cCustomerSk, C_CUSTOMER_SK),
        getStringOrNullInternal(cCustomerId, C_CUSTOMER_ID),
        getLongOrNull(cCurrentCdemoSk, C_CURRENT_CDEMO_SK),
        getLongOrNull(cCurrentHdemoSk, C_CURRENT_HDEMO_SK),
        getLongOrNull(cCurrentAddrSk, C_CURRENT_ADDR_SK),
        getLongOrNull(cFirstShiptoDateId, C_FIRST_SHIPTO_DATE_ID),
        getLongOrNull(cFirstSalesDateId, C_FIRST_SALES_DATE_ID),
        getStringOrNullInternal(cSalutation, C_SALUTATION),
        getStringOrNullInternal(cFirstName, C_FIRST_NAME),
        getStringOrNullInternal(cLastName, C_LAST_NAME),
        getStringOrNullInternal(cPreferredCustFlag, C_PREFERRED_CUST_FLAG),
        getIntOrNull(cBirthDay, C_BIRTH_DAY),
        getIntOrNull(cBirthMonth, C_BIRTH_MONTH),
        getIntOrNull(cBirthYear, C_BIRTH_YEAR),
        getStringOrNullInternal(cBirthCountry, C_BIRTH_COUNTRY),
        cLogin,
        getStringOrNullInternal(cEmailAddress, C_EMAIL_ADDRESS),
        getLongOrNull(cLastReviewDate, C_LAST_REVIEW_DATE)))
}
