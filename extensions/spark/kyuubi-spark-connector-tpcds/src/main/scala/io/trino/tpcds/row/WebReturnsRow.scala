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

import io.trino.tpcds.`type`.Pricing
import io.trino.tpcds.generator.WebReturnsGeneratorColumn._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class WebReturnsRow(
    nullBitMap: Long,
    private val wrReturnedDateSk: Long,
    private val wrReturnedTimeSk: Long,
    private val wrItemSk: Long,
    private val wrRefundedCustomerSk: Long,
    private val wrRefundedCdemoSk: Long,
    private val wrRefundedHdemoSk: Long,
    private val wrRefundedAddrSk: Long,
    private val wrReturningCustomerSk: Long,
    private val wrReturningCdemoSk: Long,
    private val wrReturningHdemoSk: Long,
    private val wrReturningAddrSk: Long,
    private val wrWebPageSk: Long,
    private val wrReasonSk: Long,
    private val wrOrderNumber: Long,
    private val wrPricing: Pricing) extends TableRowWithNulls(nullBitMap, WR_RETURNED_DATE_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = WR_RETURNED_DATE_SK

  def getWrReturnedDateSk: Long = wrReturnedDateSk
  def getWrReturnedTimeSk: Long = wrReturnedTimeSk
  def getWrItemSk: Long = wrItemSk
  def getWrRefundedCustomerSk: Long = wrRefundedCustomerSk
  def getWrRefundedCdemoSk: Long = wrRefundedCdemoSk
  def getWrRefundedHdemoSk: Long = wrRefundedHdemoSk
  def getWrRefundedAddrSk: Long = wrRefundedAddrSk
  def getWrReturningCustomerSk: Long = wrReturningCustomerSk
  def getWrReturningCdemoSk: Long = wrReturningCdemoSk
  def getWrReturningHdemoSk: Long = wrReturningHdemoSk
  def getWrReturningAddrSk: Long = wrReturningAddrSk
  def getWrWebPageSk: Long = wrWebPageSk
  def getWrReasonSk: Long = wrReasonSk
  def getWrOrderNumber: Long = wrOrderNumber
  def getWrPricing: Pricing = wrPricing

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(wrReturnedDateSk, WR_RETURNED_DATE_SK),
        getLongOrNull(wrReturnedTimeSk, WR_RETURNED_TIME_SK),
        getLongOrNull(wrItemSk, WR_ITEM_SK),
        getLongOrNull(wrRefundedCustomerSk, WR_REFUNDED_CUSTOMER_SK),
        getLongOrNull(wrRefundedCdemoSk, WR_REFUNDED_CDEMO_SK),
        getLongOrNull(wrRefundedHdemoSk, WR_REFUNDED_HDEMO_SK),
        getLongOrNull(wrRefundedAddrSk, WR_REFUNDED_ADDR_SK),
        getLongOrNull(wrReturningCustomerSk, WR_RETURNING_CUSTOMER_SK),
        getLongOrNull(wrReturningCdemoSk, WR_RETURNING_CDEMO_SK),
        getLongOrNull(wrReturningHdemoSk, WR_RETURNING_HDEMO_SK),
        getLongOrNull(wrReturningAddrSk, WR_RETURNING_ADDR_SK),
        getLongOrNull(wrWebPageSk, WR_WEB_PAGE_SK),
        getLongOrNull(wrReasonSk, WR_REASON_SK),
        getLongOrNull(wrOrderNumber, WR_ORDER_NUMBER),
        getIntOrNull(wrPricing.getQuantity(), WR_PRICING_QUANTITY),
        getDecimalOrNull(wrPricing.getNetPaid, WR_PRICING_NET_PAID, 7, 2),
        getDecimalOrNull(wrPricing.getExtTax, WR_PRICING_EXT_TAX, 7, 2),
        getDecimalOrNull(wrPricing.getNetPaidIncludingTax, WR_PRICING_NET_PAID_INC_TAX, 7, 2),
        getDecimalOrNull(wrPricing.getFee, WR_PRICING_FEE, 7, 2),
        getDecimalOrNull(wrPricing.getExtShipCost, WR_PRICING_EXT_SHIP_COST, 7, 2),
        getDecimalOrNull(wrPricing.getRefundedCash, WR_PRICING_REFUNDED_CASH, 7, 2),
        getDecimalOrNull(wrPricing.getReversedCharge, WR_PRICING_REVERSED_CHARGE, 7, 2),
        getDecimalOrNull(wrPricing.getStoreCredit, WR_PRICING_STORE_CREDIT, 7, 2),
        getDecimalOrNull(wrPricing.getNetLoss, WR_PRICING_NET_LOSS, 7, 2)))
}
