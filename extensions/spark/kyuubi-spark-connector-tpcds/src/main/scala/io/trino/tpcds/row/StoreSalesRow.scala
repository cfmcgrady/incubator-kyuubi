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

import io.trino.tpcds.generator.StoreSalesGeneratorColumn._
import io.trino.tpcds.`type`.Pricing
import java.util.{List => JList}

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow

class StoreSalesRow(
                     nullBitMap: Long,
                     private val ssSoldDateSk: Long,
                     private val ssSoldTimeSk: Long,
                     private val ssSoldItemSk: Long,
                     private val ssSoldCustomerSk: Long,
                     private val ssSoldCdemoSk: Long,
                     private val ssSoldHdemoSk: Long,
                     private val ssSoldAddrSk: Long,
                     private val ssSoldStoreSk: Long,
                     private val ssSoldPromoSk: Long,
                     private val ssTicketNumber: Long,
                     private val ssPricing: Pricing
  ) extends TableRowWithNulls(nullBitMap, SS_SOLD_DATE_SK)
  with KyuubiTableRowWithNulls {

    override val nullBitMapInternal = nullBitMap
    override val firstColumnInternal = SS_SOLD_DATE_SK

  def getSsSoldDateSk: Long = ssSoldDateSk
  def getSsSoldTimeSk: Long = ssSoldTimeSk
  def getSsSoldItemSk: Long = ssSoldItemSk
  def getSsSoldCustomerSk: Long = ssSoldCustomerSk
  def getSsSoldCdemoSk: Long = ssSoldCdemoSk
  def getSsSoldHdemoSk: Long = ssSoldHdemoSk
  def getSsSoldAddrSk: Long = ssSoldAddrSk
  def getSsSoldStoreSk: Long = ssSoldStoreSk
  def getSsSoldPromoSk: Long = ssSoldPromoSk
  def getSsTicketNumber: Long = ssTicketNumber
  def getSsPricing: Pricing = ssPricing

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
    getLongOrNull(ssSoldDateSk, SS_SOLD_DATE_SK),
    getLongOrNull(ssSoldTimeSk, SS_SOLD_TIME_SK),
    getLongOrNull(ssSoldItemSk, SS_SOLD_ITEM_SK),
    getLongOrNull(ssSoldCustomerSk, SS_SOLD_CUSTOMER_SK),
    getLongOrNull(ssSoldCdemoSk, SS_SOLD_CDEMO_SK),
    getLongOrNull(ssSoldHdemoSk, SS_SOLD_HDEMO_SK),
    getLongOrNull(ssSoldAddrSk, SS_SOLD_ADDR_SK),
    getLongOrNull(ssSoldStoreSk, SS_SOLD_STORE_SK),
    getLongOrNull(ssSoldPromoSk, SS_SOLD_PROMO_SK),
    getLongOrNull(ssTicketNumber, SS_TICKET_NUMBER),
    getIntOrNull(ssPricing.getQuantity, SS_PRICING_QUANTITY),
        getDecimalOrNull(ssPricing.getWholesaleCost, SS_PRICING_WHOLESALE_COST, 7, 2),
        getDecimalOrNull(ssPricing.getListPrice, SS_PRICING_LIST_PRICE, 7, 2),
        getDecimalOrNull(ssPricing.getSalesPrice, SS_PRICING_SALES_PRICE, 7, 2),
        getDecimalOrNull(ssPricing.getCouponAmount, SS_PRICING_COUPON_AMT, 7, 2),
        getDecimalOrNull(ssPricing.getExtSalesPrice, SS_PRICING_EXT_SALES_PRICE, 7, 2),
        getDecimalOrNull(ssPricing.getExtWholesaleCost, SS_PRICING_EXT_WHOLESALE_COST, 7, 2),
        getDecimalOrNull(ssPricing.getExtListPrice, SS_PRICING_EXT_LIST_PRICE, 7, 2),
        getDecimalOrNull(ssPricing.getExtTax, SS_PRICING_EXT_TAX, 7, 2),
        getDecimalOrNull(ssPricing.getCouponAmount, SS_PRICING_COUPON_AMT, 7, 2),
        getDecimalOrNull(ssPricing.getNetPaid, SS_PRICING_NET_PAID, 7, 2),
        getDecimalOrNull(ssPricing.getNetPaidIncludingTax, SS_PRICING_NET_PAID_INC_TAX, 7, 2),
        getDecimalOrNull(ssPricing.getNetProfit, SS_PRICING_NET_PROFIT, 7, 2)))
}
