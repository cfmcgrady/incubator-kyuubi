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

import io.trino.tpcds.`type`.{Decimal => TPCDSDecimal}
import io.trino.tpcds.`type`.Pricing
import io.trino.tpcds.Options
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_BILL_ADDR_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_BILL_CDEMO_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_BILL_CUSTOMER_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_BILL_HDEMO_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_CALL_CENTER_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_CATALOG_PAGE_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_ORDER_NUMBER
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_COUPON_AMT
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_EXT_DISCOUNT_AMOUNT
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_EXT_LIST_PRICE
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_EXT_SALES_PRICE
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_EXT_SHIP_COST
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_EXT_TAX
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_EXT_WHOLESALE_COST
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_LIST_PRICE
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_NET_PAID
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_NET_PAID_INC_SHIP
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_NET_PAID_INC_SHIP_TAX
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_NET_PAID_INC_TAX
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_NET_PROFIT
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_QUANTITY
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_SALES_PRICE
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_WHOLESALE_COST
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PROMO_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SHIP_ADDR_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SHIP_CDEMO_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SHIP_CUSTOMER_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SHIP_DATE_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SHIP_HDEMO_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SHIP_MODE_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SOLD_DATE_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SOLD_ITEM_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SOLD_TIME_SK
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_WAREHOUSE_SK
import io.trino.tpcds.generator.GeneratorColumn
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, RebaseDateTime}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

class CatalogSalesRow(
    private val csSoldDateSk: Long,
    private val csSoldTimeSk: Long,
    private val csShipDateSk: Long,
    private val csBillCustomerSk: Long,
    private val csBillCdemoSk: Long,
    private val csBillHdemoSk: Long,
    private val csBillAddrSk: Long,
    private val csShipCustomerSk: Long,
    private val csShipCdemoSk: Long,
    private val csShipHdemoSk: Long,
    private val csShipAddrSk: Long,
    private val csCallCenterSk: Long,
    private val csCatalogPageSk: Long,
    private val csShipModeSk: Long,
    private val csWarehouseSk: Long,
    private val csSoldItemSk: Long,
    private val csPromoSk: Long,
    private val csOrderNumber: Long,
    private val csPricing: Pricing,
    nullBitMap: Long) extends TableRowWithNulls(nullBitMap, CS_SOLD_DATE_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = CS_SOLD_DATE_SK

  def getCsPricing: Pricing = csPricing

  def getCsSoldItemSk: Long = csSoldItemSk

  def getCsCatalogPageSk: Long = csCatalogPageSk

  def getCsOrderNumber: Long = csOrderNumber

  def getCsBillCustomerSk: Long = csBillCustomerSk

  def getCsBillCdemoSk: Long = csBillCdemoSk

  def getCsBillHdemoSk: Long = csBillHdemoSk

  def getCsBillAddrSk: Long = csBillAddrSk

  def getCsCallCenterSk: Long = csCallCenterSk

  def getCsShipCustomerSk: Long = csShipCustomerSk

  def getCsShipCdemoSk: Long = csShipCdemoSk

  def getCsShipAddrSk: Long = csShipAddrSk

  def getCsShipDateSk: Long = csShipDateSk

  def getCsSoldDateSk: Long = csSoldDateSk

  def getCsSoldTimeSk: Long = csSoldTimeSk

  def getCsShipHdemoSk: Long = csShipHdemoSk

  def getCsShipModeSk: Long = csShipModeSk

  def getCsWarehouseSk: Long = csWarehouseSk

  def getCsPromoSk: Long = csPromoSk

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow = {
    new GenericInternalRow(
      Array(
        getLongOrNull(csSoldDateSk, CS_SOLD_DATE_SK),
        getLongOrNull(csSoldTimeSk, CS_SOLD_TIME_SK),
        getLongOrNull(csShipDateSk, CS_SHIP_DATE_SK),
        getLongOrNull(csBillCustomerSk, CS_BILL_CUSTOMER_SK),
        getLongOrNull(csBillCdemoSk, CS_BILL_CDEMO_SK),
        getLongOrNull(csBillHdemoSk, CS_BILL_HDEMO_SK),
        getLongOrNull(csBillAddrSk, CS_BILL_ADDR_SK),
        getLongOrNull(csShipCustomerSk, CS_SHIP_CUSTOMER_SK),
        getLongOrNull(csShipCdemoSk, CS_SHIP_CDEMO_SK),
        getLongOrNull(csShipHdemoSk, CS_SHIP_HDEMO_SK),
        getLongOrNull(csShipAddrSk, CS_SHIP_ADDR_SK),
        getLongOrNull(csCallCenterSk, CS_CALL_CENTER_SK),
        getLongOrNull(csCatalogPageSk, CS_CATALOG_PAGE_SK),
        getLongOrNull(csShipModeSk, CS_SHIP_MODE_SK),
        getLongOrNull(csWarehouseSk, CS_WAREHOUSE_SK),
        getLongOrNull(csSoldItemSk, CS_SOLD_ITEM_SK),
        getLongOrNull(csPromoSk, CS_PROMO_SK),
        getLongOrNull(csOrderNumber, CS_ORDER_NUMBER),
        getIntOrNull(csPricing.getQuantity(), CS_PRICING_QUANTITY),
        getDecimalOrNull(csPricing.getWholesaleCost, CS_PRICING_WHOLESALE_COST, 7, 2),
        getDecimalOrNull(csPricing.getListPrice, CS_PRICING_LIST_PRICE, 7, 2),
        getDecimalOrNull(csPricing.getSalesPrice, CS_PRICING_SALES_PRICE, 7, 2),
        getDecimalOrNull(csPricing.getExtDiscountAmount, CS_PRICING_EXT_DISCOUNT_AMOUNT, 7, 2),
        getDecimalOrNull(csPricing.getExtSalesPrice, CS_PRICING_EXT_SALES_PRICE, 7, 2),
        getDecimalOrNull(csPricing.getExtWholesaleCost, CS_PRICING_EXT_WHOLESALE_COST, 7, 2),
        getDecimalOrNull(csPricing.getExtListPrice, CS_PRICING_EXT_LIST_PRICE, 7, 2),
        getDecimalOrNull(csPricing.getExtTax, CS_PRICING_EXT_TAX, 7, 2),
        getDecimalOrNull(csPricing.getCouponAmount, CS_PRICING_COUPON_AMT, 7, 2),
        getDecimalOrNull(csPricing.getExtShipCost, CS_PRICING_EXT_SHIP_COST, 7, 2),
        getDecimalOrNull(csPricing.getNetPaid, CS_PRICING_NET_PAID, 7, 2),
        getDecimalOrNull(csPricing.getNetPaidIncludingTax, CS_PRICING_NET_PAID_INC_TAX, 7, 2),
        getDecimalOrNull(csPricing.getNetPaidIncludingShipping, CS_PRICING_NET_PAID_INC_SHIP, 7, 2),
        getDecimalOrNull(
          csPricing.getNetPaidIncludingShippingAndTax,
          CS_PRICING_NET_PAID_INC_SHIP_TAX,
          7,
          2),
        getDecimalOrNull(csPricing.getNetProfit, CS_PRICING_NET_PROFIT, 7, 2)))
  }
}

trait KyuubiTableRowWithNulls { self: TableRowWithNulls =>

  def isNullInternal(column: GeneratorColumn): Boolean = {
    val kBitMask = 1L << (column.getGlobalColumnNumber - firstColumnInternal.getGlobalColumnNumber)
    (nullBitMapInternal & kBitMask) != 0
  }

  def getLongOrNull(value: Long, column: GeneratorColumn): Any = {
    if (isNullInternal(column) || (value == -1L)) null else value
  }

  def getLongOrNull(value: Int, column: GeneratorColumn): Any = {
    if (isNullInternal(column) || (value == -1)) null else value.toLong
  }

  def getDecimalOrNull(
      value: TPCDSDecimal,
      column: GeneratorColumn,
      precision: Int,
      scale: Int): Decimal = {
    if (isNullInternal(column)) null else Decimal(value.getNumber, precision, scale)
  }

  def getDecimalOrNull(
      value: Int,
      column: GeneratorColumn,
      precision: Int,
      scale: Int): Decimal = {
    if (isNullInternal(column)) null
    else {
      val decimal = Decimal(value)
      decimal.changePrecision(precision, scale)
      decimal
    }
  }

  def getIntOrNull(value: Long, column: GeneratorColumn): Any = {
    if (isNullInternal(column) || (value == -1L)) null else value.toInt
  }

  def getIntOrNull(value: Int, column: GeneratorColumn): Any = {
    if (isNullInternal(column) || (value == -1)) null else value
  }

  def getStringOrNullInternal(value: String, column: GeneratorColumn): UTF8String = {
    if (isNullInternal(column) || value == null || value == Options.DEFAULT_NULL_STRING) {
      null
    } else UTF8String.fromString(value)
  }

  def getStringOrNullInternal(value: Boolean, column: GeneratorColumn): UTF8String = {
    if (isNullInternal(column)) {
      null
    } else {
      if (value) KyuubiTableRowWithNulls.TRUE_STRING else KyuubiTableRowWithNulls.FALSE_STRING
    }
  }

  def getDateOrNullFromJulianDays(value: Long, column: GeneratorColumn): Any = {
    if (isNullInternal(column) || value < 0) {
      null
    } else {
      RebaseDateTime.rebaseJulianToGregorianDays(value.toInt) - DateTimeUtils.JULIAN_DAY_OF_EPOCH
    }
  }

  def nullBitMapInternal: Long
  def firstColumnInternal: GeneratorColumn
  def internalRow: InternalRow
}

object KyuubiTableRowWithNulls {
  private val TRUE_STRING = UTF8String.fromString("Y")
  private val FALSE_STRING = UTF8String.fromString("N")
}
