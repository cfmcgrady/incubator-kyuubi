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
import io.trino.tpcds.generator.WebSalesGeneratorColumn._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class WebSalesRow(
    nullBitMap: Long,
    private val wsSoldDateSk: Long,
    private val wsSoldTimeSk: Long,
    private val wsShipDateSk: Long,
    private val wsItemSk: Long,
    private val wsBillCustomerSk: Long,
    private val wsBillCdemoSk: Long,
    private val wsBillHdemoSk: Long,
    private val wsBillAddrSk: Long,
    private val wsShipCustomerSk: Long,
    private val wsShipCdemoSk: Long,
    private val wsShipHdemoSk: Long,
    private val wsShipAddrSk: Long,
    private val wsWebPageSk: Long,
    private val wsWebSiteSk: Long,
    private val wsShipModeSk: Long,
    private val wsWarehouseSk: Long,
    private val wsPromoSk: Long,
    private val wsOrderNumber: Long,
    private val wsPricing: Pricing) extends TableRowWithNulls(nullBitMap, WS_SOLD_DATE_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = WS_SOLD_DATE_SK

  def getWsShipCdemoSk: Long = wsShipCdemoSk
  def getWsShipHdemoSk: Long = wsShipHdemoSk
  def getWsShipAddrSk: Long = wsShipAddrSk
  def getWsPricing: Pricing = wsPricing
  def getWsItemSk: Long = wsItemSk
  def getWsOrderNumber: Long = wsOrderNumber
  def getWsWebPageSk: Long = wsWebPageSk
  def getWsShipDateSk: Long = wsShipDateSk
  def getWsShipCustomerSk: Long = wsShipCustomerSk
  def getWsSoldDateSk: Long = wsSoldDateSk
  def getWsSoldTimeSk: Long = wsSoldTimeSk
  def getWsBillCustomerSk: Long = wsBillCustomerSk
  def getWsBillCdemoSk: Long = wsBillCdemoSk
  def getWsBillHdemoSk: Long = wsBillHdemoSk
  def getWsBillAddrSk: Long = wsBillAddrSk
  def getWsWebSiteSk: Long = wsWebSiteSk
  def getWsShipModeSk: Long = wsShipModeSk
  def getWsWarehouseSk: Long = wsWarehouseSk
  def getWsPromoSk: Long = wsPromoSk

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(wsSoldDateSk, WS_SOLD_DATE_SK),
        getLongOrNull(wsSoldTimeSk, WS_SOLD_TIME_SK),
        getLongOrNull(wsShipDateSk, WS_SHIP_DATE_SK),
        getLongOrNull(wsItemSk, WS_ITEM_SK),
        getLongOrNull(wsBillCustomerSk, WS_BILL_CUSTOMER_SK),
        getLongOrNull(wsBillCdemoSk, WS_BILL_CDEMO_SK),
        getLongOrNull(wsBillHdemoSk, WS_BILL_HDEMO_SK),
        getLongOrNull(wsBillAddrSk, WS_BILL_ADDR_SK),
        getLongOrNull(wsShipCustomerSk, WS_SHIP_CUSTOMER_SK),
        getLongOrNull(wsShipCdemoSk, WS_SHIP_CDEMO_SK),
        getLongOrNull(wsShipHdemoSk, WS_SHIP_HDEMO_SK),
        getLongOrNull(wsShipAddrSk, WS_SHIP_ADDR_SK),
        getLongOrNull(wsWebPageSk, WS_WEB_PAGE_SK),
        getLongOrNull(wsWebSiteSk, WS_WEB_SITE_SK),
        getLongOrNull(wsShipModeSk, WS_SHIP_MODE_SK),
        getLongOrNull(wsWarehouseSk, WS_WAREHOUSE_SK),
        getLongOrNull(wsPromoSk, WS_PROMO_SK),
        getLongOrNull(wsOrderNumber, WS_ORDER_NUMBER),
        getIntOrNull(wsPricing.getQuantity(), WS_PRICING_QUANTITY),
        getDecimalOrNull(wsPricing.getWholesaleCost, WS_PRICING_WHOLESALE_COST, 7, 2),
        getDecimalOrNull(wsPricing.getListPrice, WS_PRICING_LIST_PRICE, 7, 2),
        getDecimalOrNull(wsPricing.getSalesPrice, WS_PRICING_SALES_PRICE, 7, 2),
        getDecimalOrNull(wsPricing.getExtDiscountAmount, WS_PRICING_EXT_DISCOUNT_AMT, 7, 2),
        getDecimalOrNull(wsPricing.getExtSalesPrice, WS_PRICING_EXT_SALES_PRICE, 7, 2),
        getDecimalOrNull(wsPricing.getExtWholesaleCost, WS_PRICING_EXT_WHOLESALE_COST, 7, 2),
        getDecimalOrNull(wsPricing.getExtListPrice, WS_PRICING_EXT_LIST_PRICE, 7, 2),
        getDecimalOrNull(wsPricing.getExtTax, WS_PRICING_EXT_TAX, 7, 2),
        getDecimalOrNull(wsPricing.getCouponAmount, WS_PRICING_COUPON_AMT, 7, 2),
        getDecimalOrNull(wsPricing.getExtShipCost, WS_PRICING_EXT_SHIP_COST, 7, 2),
        getDecimalOrNull(wsPricing.getNetPaid, WS_PRICING_NET_PAID, 7, 2),
        getDecimalOrNull(wsPricing.getNetPaidIncludingTax, WS_PRICING_NET_PAID_INC_TAX, 7, 2),
        getDecimalOrNull(wsPricing.getNetPaidIncludingShipping, WS_PRICING_NET_PAID_INC_SHIP, 7, 2),
        getDecimalOrNull(
          wsPricing.getNetPaidIncludingShippingAndTax,
          WS_PRICING_NET_PAID_INC_SHIP_TAX,
          7,
          2),
        getDecimalOrNull(wsPricing.getNetProfit, WS_PRICING_NET_PROFIT, 7, 2)))
}
