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
import io.trino.tpcds.generator.StoreReturnsGeneratorColumn._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class StoreReturnsRow(
    nullBitMap: Long,
    private val srReturnedDateSk: Long,
    private val srReturnedTimeSk: Long,
    private val srItemSk: Long,
    private val srCustomerSk: Long,
    private val srCdemoSk: Long,
    private val srHdemoSk: Long,
    private val srAddrSk: Long,
    private val srStoreSk: Long,
    private val srReasonSk: Long,
    private val srTicketNumber: Long,
    private val srPricing: Pricing) extends TableRowWithNulls(nullBitMap, SR_RETURNED_DATE_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = SR_RETURNED_DATE_SK

  def getSrReturnedDateSk: Long = srReturnedDateSk
  def getSrReturnedTimeSk: Long = srReturnedTimeSk
  def getSrItemSk: Long = srItemSk
  def getSrCustomerSk: Long = srCustomerSk
  def getSrCdemoSk: Long = srCdemoSk
  def getSrHdemoSk: Long = srHdemoSk
  def getSrAddrSk: Long = srAddrSk
  def getSrStoreSk: Long = srStoreSk
  def getSrReasonSk: Long = srReasonSk
  def getSrTicketNumber: Long = srTicketNumber
  def getSrPricing: Pricing = srPricing

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(srReturnedDateSk, SR_RETURNED_DATE_SK),
        getLongOrNull(srReturnedTimeSk, SR_RETURNED_TIME_SK),
        getLongOrNull(srItemSk, SR_ITEM_SK),
        getLongOrNull(srCustomerSk, SR_CUSTOMER_SK),
        getLongOrNull(srCdemoSk, SR_CDEMO_SK),
        getLongOrNull(srHdemoSk, SR_HDEMO_SK),
        getLongOrNull(srAddrSk, SR_ADDR_SK),
        getLongOrNull(srStoreSk, SR_STORE_SK),
        getLongOrNull(srReasonSk, SR_REASON_SK),
        getLongOrNull(srTicketNumber, SR_TICKET_NUMBER),
        getIntOrNull(srPricing.getQuantity, SR_PRICING_QUANTITY),
        getDecimalOrNull(srPricing.getNetPaid, SR_PRICING_NET_PAID, 7, 2),
        getDecimalOrNull(srPricing.getExtTax, SR_PRICING_EXT_TAX, 7, 2),
        getDecimalOrNull(srPricing.getNetPaidIncludingTax, SR_PRICING_NET_PAID_INC_TAX, 7, 2),
        getDecimalOrNull(srPricing.getFee, SR_PRICING_FEE, 7, 2),
        getDecimalOrNull(srPricing.getExtShipCost, SR_PRICING_EXT_SHIP_COST, 7, 2),
        getDecimalOrNull(srPricing.getRefundedCash, SR_PRICING_REFUNDED_CASH, 7, 2),
        getDecimalOrNull(srPricing.getReversedCharge, SR_PRICING_REVERSED_CHARGE, 7, 2),
        getDecimalOrNull(srPricing.getStoreCredit, SR_PRICING_STORE_CREDIT, 7, 2),
        getDecimalOrNull(srPricing.getNetLoss, SR_PRICING_NET_LOSS, 7, 2)))
}
