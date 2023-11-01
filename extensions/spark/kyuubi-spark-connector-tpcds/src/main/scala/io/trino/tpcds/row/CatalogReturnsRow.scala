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

import io.trino.tpcds.generator.CatalogReturnsGeneratorColumn._
import io.trino.tpcds.`type`.Pricing
import java.util.{List => JList}

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow

class CatalogReturnsRow(
    private val crReturnedDateSk: Long,
    private val crReturnedTimeSk: Long,
    private val crItemSk: Long,
    private val crRefundedCustomerSk: Long,
    private val crRefundedCdemoSk: Long,
    private val crRefundedHdemoSk: Long,
    private val crRefundedAddrSk: Long,
    private val crReturningCustomerSk: Long,
    private val crReturningCdemoSk: Long,
    private val crReturningHdemoSk: Long,
    private val crReturningAddrSk: Long,
    private val crCallCenterSk: Long,
    private val crCatalogPageSk: Long,
    private val crShipModeSk: Long,
    private val crWarehouseSk: Long,
    private val crReasonSk: Long,
    private val crOrderNumber: Long,
    private val crPricing: Pricing,
    nullBitMap: Long) extends TableRowWithNulls(nullBitMap, CR_RETURNED_DATE_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = CR_RETURNED_DATE_SK

  def getCrReturnedDateSk: Long = crReturnedDateSk
  def getCrReturnedTimeSk: Long = crReturnedTimeSk
  def getCrItemSk: Long = crItemSk
  def getCrRefundedCustomerSk: Long = crRefundedCustomerSk
  def getCrRefundedCdemoSk: Long = crRefundedCdemoSk
  def getCrRefundedHdemoSk: Long = crRefundedHdemoSk
  def getCrRefundedAddrSk: Long = crRefundedAddrSk
  def getCrReturningCustomerSk: Long = crReturningCustomerSk
  def getCrReturningCdemoSk: Long = crReturningCdemoSk
  def getCrReturningHdemoSk: Long = crReturningHdemoSk
  def getCrReturningAddrSk: Long = crReturningAddrSk
  def getCrCallCenterSk: Long = crCallCenterSk
  def getCrCatalogPageSk: Long = crCatalogPageSk
  def getCrShipModeSk: Long = crShipModeSk
  def getCrWarehouseSk: Long = crWarehouseSk
  def getCrReasonSk: Long = crReasonSk
  def getCrOrderNumber: Long = crOrderNumber
  def getCrPricing: Pricing = crPricing

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(crReturnedDateSk, CR_RETURNED_DATE_SK),
        getLongOrNull(crReturnedTimeSk, CR_RETURNED_TIME_SK),
        getLongOrNull(crItemSk, CR_ITEM_SK),
        getLongOrNull(crRefundedCustomerSk, CR_REFUNDED_CUSTOMER_SK),
        getLongOrNull(crRefundedCdemoSk, CR_REFUNDED_CDEMO_SK),
        getLongOrNull(crRefundedHdemoSk, CR_REFUNDED_HDEMO_SK),
        getLongOrNull(crRefundedAddrSk, CR_REFUNDED_ADDR_SK),
        getLongOrNull(crReturningCustomerSk, CR_RETURNING_CUSTOMER_SK),
        getLongOrNull(crReturningCdemoSk, CR_RETURNING_CDEMO_SK),
        getLongOrNull(crReturningHdemoSk, CR_RETURNING_HDEMO_SK),
        getLongOrNull(crReturningAddrSk, CR_RETURNING_ADDR_SK),
        getLongOrNull(crCallCenterSk, CR_CALL_CENTER_SK),
        getLongOrNull(crCatalogPageSk, CR_CATALOG_PAGE_SK),
        getLongOrNull(crShipModeSk, CR_SHIP_MODE_SK),
        getLongOrNull(crWarehouseSk, CR_WAREHOUSE_SK),
        getLongOrNull(crReasonSk, CR_REASON_SK),
        getLongOrNull(crOrderNumber, CR_ORDER_NUMBER),
        getIntOrNull(crPricing.getQuantity, CR_PRICING_QUANTITY),
        getDecimalOrNull(crPricing.getNetPaid, CR_PRICING_NET_PAID, 7, 2),
        getDecimalOrNull(crPricing.getExtTax, CR_PRICING_EXT_TAX, 7, 2),
        getDecimalOrNull(crPricing.getNetPaidIncludingTax, CR_PRICING_NET_PAID_INC_TAX, 7, 2),
        getDecimalOrNull(crPricing.getFee, CR_PRICING_FEE, 7, 2),
        getDecimalOrNull(crPricing.getExtShipCost, CR_PRICING_EXT_SHIP_COST, 7, 2),
        getDecimalOrNull(crPricing.getRefundedCash, CR_PRICING_REFUNDED_CASH, 7, 2),
        getDecimalOrNull(crPricing.getReversedCharge, CR_PRICING_REVERSED_CHARGE, 7, 2),
        getDecimalOrNull(crPricing.getStoreCredit, CR_PRICING_STORE_CREDIT, 7, 2),
        getDecimalOrNull(crPricing.getNetLoss, CR_PRICING_NET_LOSS, 7, 2)))
}
