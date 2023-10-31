package io.trino.tpcds.row;

import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_BILL_ADDR_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_BILL_CDEMO_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_BILL_CUSTOMER_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_BILL_HDEMO_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_ITEM_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_ORDER_NUMBER;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_COUPON_AMT;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_EXT_DISCOUNT_AMT;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_EXT_LIST_PRICE;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_EXT_SALES_PRICE;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_EXT_SHIP_COST;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_EXT_TAX;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_EXT_WHOLESALE_COST;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_LIST_PRICE;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_NET_PAID;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_NET_PAID_INC_SHIP;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_NET_PAID_INC_SHIP_TAX;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_NET_PAID_INC_TAX;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_NET_PROFIT;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_QUANTITY;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_SALES_PRICE;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PRICING_WHOLESALE_COST;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_PROMO_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_SHIP_ADDR_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_SHIP_CDEMO_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_SHIP_CUSTOMER_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_SHIP_DATE_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_SHIP_HDEMO_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_SHIP_MODE_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_SOLD_DATE_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_SOLD_TIME_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_WAREHOUSE_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_WEB_PAGE_SK;
import static io.trino.tpcds.generator.WebSalesGeneratorColumn.WS_WEB_SITE_SK;

import io.trino.tpcds.type.Pricing;
import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

public class WebSalesRow extends KyuubiTPCDSTableRowWithNulls {
  private final long wsSoldDateSk;
  private final long wsSoldTimeSk;
  private final long wsShipDateSk;
  private final long wsItemSk;
  private final long wsBillCustomerSk;
  private final long wsBillCdemoSk;
  private final long wsBillHdemoSk;
  private final long wsBillAddrSk;
  private final long wsShipCustomerSk;
  private final long wsShipCdemoSk;
  private final long wsShipHdemoSk;
  private final long wsShipAddrSk;
  private final long wsWebPageSk;
  private final long wsWebSiteSk;
  private final long wsShipModeSk;
  private final long wsWarehouseSk;
  private final long wsPromoSk;
  private final long wsOrderNumber;
  private final Pricing wsPricing;

  public WebSalesRow(
      long nullBitMap,
      long wsSoldDateSk,
      long wsSoldTimeSk,
      long wsShipDateSk,
      long wsItemSk,
      long wsBillCustomerSk,
      long wsBillCdemoSk,
      long wsBillHdemoSk,
      long wsBillAddrSk,
      long wsShipCustomerSk,
      long wsShipCdemoSk,
      long wsShipHdemoSk,
      long wsShipAddrSk,
      long wsWebPageSk,
      long wsWebSiteSk,
      long wsShipModeSk,
      long wsWarehouseSk,
      long wsPromoSk,
      long wsOrderNumber,
      Pricing wsPricing) {
    super(nullBitMap, WS_SOLD_DATE_SK);
    this.wsSoldDateSk = wsSoldDateSk;
    this.wsSoldTimeSk = wsSoldTimeSk;
    this.wsShipDateSk = wsShipDateSk;
    this.wsItemSk = wsItemSk;
    this.wsBillCustomerSk = wsBillCustomerSk;
    this.wsBillCdemoSk = wsBillCdemoSk;
    this.wsBillHdemoSk = wsBillHdemoSk;
    this.wsBillAddrSk = wsBillAddrSk;
    this.wsShipCustomerSk = wsShipCustomerSk;
    this.wsShipCdemoSk = wsShipCdemoSk;
    this.wsShipHdemoSk = wsShipHdemoSk;
    this.wsShipAddrSk = wsShipAddrSk;
    this.wsWebPageSk = wsWebPageSk;
    this.wsWebSiteSk = wsWebSiteSk;
    this.wsShipModeSk = wsShipModeSk;
    this.wsWarehouseSk = wsWarehouseSk;
    this.wsPromoSk = wsPromoSk;
    this.wsOrderNumber = wsOrderNumber;
    this.wsPricing = wsPricing;
  }

  public long getWsShipCdemoSk() {
    return wsShipCdemoSk;
  }

  public long getWsShipHdemoSk() {
    return wsShipHdemoSk;
  }

  public long getWsShipAddrSk() {
    return wsShipAddrSk;
  }

  public Pricing getWsPricing() {
    return wsPricing;
  }

  public long getWsItemSk() {
    return wsItemSk;
  }

  public long getWsOrderNumber() {
    return wsOrderNumber;
  }

  public long getWsWebPageSk() {
    return wsWebPageSk;
  }

  public long getWsShipDateSk() {
    return wsShipDateSk;
  }

  public long getWsShipCustomerSk() {
    return wsShipCustomerSk;
  }

  public long getWsSoldDateSk() {
    return wsSoldDateSk;
  }

  public long getWsSoldTimeSk() {
    return wsSoldTimeSk;
  }

  public long getWsBillCustomerSk() {
    return wsBillCustomerSk;
  }

  public long getWsBillCdemoSk() {
    return wsBillCdemoSk;
  }

  public long getWsBillHdemoSk() {
    return wsBillHdemoSk;
  }

  public long getWsBillAddrSk() {
    return wsBillAddrSk;
  }

  public long getWsWebSiteSk() {
    return wsWebSiteSk;
  }

  public long getWsShipModeSk() {
    return wsShipModeSk;
  }

  public long getWsWarehouseSk() {
    return wsWarehouseSk;
  }

  public long getWsPromoSk() {
    return wsPromoSk;
  }

  @Override
  public Object[] values() {
    return new Object[] {
      getOrNullForKey(wsSoldDateSk, WS_SOLD_DATE_SK),
      getOrNullForKey(wsSoldTimeSk, WS_SOLD_TIME_SK),
      getOrNullForKey(wsShipDateSk, WS_SHIP_DATE_SK),
      getOrNullForKey(wsItemSk, WS_ITEM_SK),
      getOrNullForKey(wsBillCustomerSk, WS_BILL_CUSTOMER_SK),
      getOrNullForKey(wsBillCdemoSk, WS_BILL_CDEMO_SK),
      getOrNullForKey(wsBillHdemoSk, WS_BILL_HDEMO_SK),
      getOrNullForKey(wsBillAddrSk, WS_BILL_ADDR_SK),
      getOrNullForKey(wsShipCustomerSk, WS_SHIP_CUSTOMER_SK),
      getOrNullForKey(wsShipCdemoSk, WS_SHIP_CDEMO_SK),
      getOrNullForKey(wsShipHdemoSk, WS_SHIP_HDEMO_SK),
      getOrNullForKey(wsShipAddrSk, WS_SHIP_ADDR_SK),
      getOrNullForKey(wsWebPageSk, WS_WEB_PAGE_SK),
      getOrNullForKey(wsWebSiteSk, WS_WEB_SITE_SK),
      getOrNullForKey(wsShipModeSk, WS_SHIP_MODE_SK),
      getOrNullForKey(wsWarehouseSk, WS_WAREHOUSE_SK),
      getOrNullForKey(wsPromoSk, WS_PROMO_SK),
      getOrNullForKey(wsOrderNumber, WS_ORDER_NUMBER),
      getOrNull(wsPricing.getQuantity(), WS_PRICING_QUANTITY),
      getOrNull(wsPricing.getWholesaleCost(), WS_PRICING_WHOLESALE_COST),
      getOrNull(wsPricing.getListPrice(), WS_PRICING_LIST_PRICE),
      getOrNull(wsPricing.getSalesPrice(), WS_PRICING_SALES_PRICE),
      getOrNull(wsPricing.getExtDiscountAmount(), WS_PRICING_EXT_DISCOUNT_AMT),
      getOrNull(wsPricing.getExtSalesPrice(), WS_PRICING_EXT_SALES_PRICE),
      getOrNull(wsPricing.getExtWholesaleCost(), WS_PRICING_EXT_WHOLESALE_COST),
      getOrNull(wsPricing.getExtListPrice(), WS_PRICING_EXT_LIST_PRICE),
      getOrNull(wsPricing.getExtTax(), WS_PRICING_EXT_TAX),
      getOrNull(wsPricing.getCouponAmount(), WS_PRICING_COUPON_AMT),
      getOrNull(wsPricing.getExtShipCost(), WS_PRICING_EXT_SHIP_COST),
      getOrNull(wsPricing.getNetPaid(), WS_PRICING_NET_PAID),
      getOrNull(wsPricing.getNetPaidIncludingTax(), WS_PRICING_NET_PAID_INC_TAX),
      getOrNull(wsPricing.getNetPaidIncludingShipping(), WS_PRICING_NET_PAID_INC_SHIP),
      getOrNull(wsPricing.getNetPaidIncludingShippingAndTax(), WS_PRICING_NET_PAID_INC_SHIP_TAX),
      getOrNull(wsPricing.getNetProfit(), WS_PRICING_NET_PROFIT)
    };
  }
}
