package io.trino.tpcds.row;

    import io.trino.tpcds.type.Pricing;
    import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_BILL_ADDR_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_BILL_CDEMO_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_BILL_CUSTOMER_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_BILL_HDEMO_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_CALL_CENTER_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_CATALOG_PAGE_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_ORDER_NUMBER;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_COUPON_AMT;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_EXT_DISCOUNT_AMOUNT;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_EXT_LIST_PRICE;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_EXT_SALES_PRICE;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_EXT_SHIP_COST;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_EXT_TAX;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_EXT_WHOLESALE_COST;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_LIST_PRICE;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_NET_PAID;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_NET_PAID_INC_SHIP;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_NET_PAID_INC_SHIP_TAX;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_NET_PAID_INC_TAX;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_NET_PROFIT;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_QUANTITY;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_SALES_PRICE;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PRICING_WHOLESALE_COST;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_PROMO_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SHIP_ADDR_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SHIP_CDEMO_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SHIP_CUSTOMER_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SHIP_DATE_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SHIP_HDEMO_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SHIP_MODE_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SOLD_DATE_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SOLD_ITEM_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_SOLD_TIME_SK;
    import static io.trino.tpcds.generator.CatalogSalesGeneratorColumn.CS_WAREHOUSE_SK;

public class CatalogSalesRow
    extends KyuubiTPCDSTableRowWithNulls
{
  private final long csSoldDateSk;
  private final long csSoldTimeSk;
  private final long csShipDateSk;
  private final long csBillCustomerSk;
  private final long csBillCdemoSk;
  private final long csBillHdemoSk;
  private final long csBillAddrSk;
  private final long csShipCustomerSk;
  private final long csShipCdemoSk;
  private final long csShipHdemoSk;
  private final long csShipAddrSk;
  private final long csCallCenterSk;
  private final long csCatalogPageSk;
  private final long csShipModeSk;
  private final long csWarehouseSk;
  private final long csSoldItemSk;
  private final long csPromoSk;
  private final long csOrderNumber;
  private final Pricing csPricing;

  public CatalogSalesRow(long csSoldDateSk,
      long csSoldTimeSk,
      long csShipDateSk,
      long csBillCustomerSk,
      long csBillCdemoSk,
      long csBillHdemoSk,
      long csBillAddrSk,
      long csShipCustomerSk,
      long csShipCdemoSk,
      long csShipHdemoSk,
      long csShipAddrSk,
      long csCallCenterSk,
      long csCatalogPageSk,
      long csShipModeSk,
      long csWarehouseSk,
      long csSoldItemSk,
      long csPromoSk,
      long csOrderNumber,
      Pricing csPricing,
      long nullBitMap)
  {
    super(nullBitMap, CS_SOLD_DATE_SK);
    this.csSoldDateSk = csSoldDateSk;
    this.csSoldTimeSk = csSoldTimeSk;
    this.csShipDateSk = csShipDateSk;
    this.csBillCustomerSk = csBillCustomerSk;
    this.csBillCdemoSk = csBillCdemoSk;
    this.csBillHdemoSk = csBillHdemoSk;
    this.csBillAddrSk = csBillAddrSk;
    this.csShipCustomerSk = csShipCustomerSk;
    this.csShipCdemoSk = csShipCdemoSk;
    this.csShipHdemoSk = csShipHdemoSk;
    this.csShipAddrSk = csShipAddrSk;
    this.csCallCenterSk = csCallCenterSk;
    this.csCatalogPageSk = csCatalogPageSk;
    this.csShipModeSk = csShipModeSk;
    this.csWarehouseSk = csWarehouseSk;
    this.csSoldItemSk = csSoldItemSk;
    this.csPromoSk = csPromoSk;
    this.csOrderNumber = csOrderNumber;
    this.csPricing = csPricing;
  }

  public Pricing getCsPricing()
  {
    return csPricing;
  }

  public long getCsSoldItemSk()
  {
    return csSoldItemSk;
  }

  public long getCsCatalogPageSk()
  {
    return csCatalogPageSk;
  }

  public long getCsOrderNumber()
  {
    return csOrderNumber;
  }

  public long getCsBillCustomerSk()
  {
    return csBillCustomerSk;
  }

  public long getCsBillCdemoSk()
  {
    return csBillCdemoSk;
  }

  public long getCsBillHdemoSk()
  {
    return csBillHdemoSk;
  }

  public long getCsBillAddrSk()
  {
    return csBillAddrSk;
  }

  public long getCsCallCenterSk()
  {
    return csCallCenterSk;
  }

  public long getCsShipCustomerSk()
  {
    return csShipCustomerSk;
  }

  public long getCsShipCdemoSk()
  {
    return csShipCdemoSk;
  }

  public long getCsShipAddrSk()
  {
    return csShipAddrSk;
  }

  public long getCsShipDateSk()
  {
    return csShipDateSk;
  }

  public long getCsSoldDateSk() {
    return csSoldDateSk;
  }

  public long getCsSoldTimeSk() {
    return csSoldTimeSk;
  }

  public long getCsShipHdemoSk() {
    return csShipHdemoSk;
  }

  public long getCsShipModeSk() {
    return csShipModeSk;
  }

  public long getCsWarehouseSk() {
    return csWarehouseSk;
  }

  public long getCsPromoSk() {
    return csPromoSk;
  }

  @Override public Object[] values() {
    return new Object[] {
        getOrNullForKey(csSoldDateSk, CS_SOLD_DATE_SK),
        getOrNullForKey(csSoldTimeSk, CS_SOLD_TIME_SK),
        getOrNullForKey(csShipDateSk, CS_SHIP_DATE_SK),
        getOrNullForKey(csBillCustomerSk, CS_BILL_CUSTOMER_SK),
        getOrNullForKey(csBillCdemoSk, CS_BILL_CDEMO_SK),
        getOrNullForKey(csBillHdemoSk, CS_BILL_HDEMO_SK),
        getOrNullForKey(csBillAddrSk, CS_BILL_ADDR_SK),
        getOrNullForKey(csShipCustomerSk, CS_SHIP_CUSTOMER_SK),
        getOrNullForKey(csShipCdemoSk, CS_SHIP_CDEMO_SK),
        getOrNullForKey(csShipHdemoSk, CS_SHIP_HDEMO_SK),
        getOrNullForKey(csShipAddrSk, CS_SHIP_ADDR_SK),
        getOrNullForKey(csCallCenterSk, CS_CALL_CENTER_SK),
        getOrNullForKey(csCatalogPageSk, CS_CATALOG_PAGE_SK),
        getOrNullForKey(csShipModeSk, CS_SHIP_MODE_SK),
        getOrNull(csWarehouseSk, CS_WAREHOUSE_SK),
        getOrNullForKey(csSoldItemSk, CS_SOLD_ITEM_SK),
        getOrNullForKey(csPromoSk, CS_PROMO_SK),
        getOrNull(csOrderNumber, CS_ORDER_NUMBER),
        getOrNull(csPricing.getQuantity(), CS_PRICING_QUANTITY),
        getOrNull(csPricing.getWholesaleCost(), CS_PRICING_WHOLESALE_COST),
        getOrNull(csPricing.getListPrice(), CS_PRICING_LIST_PRICE),
        getOrNull(csPricing.getSalesPrice(), CS_PRICING_SALES_PRICE),
        getOrNull(csPricing.getExtDiscountAmount(), CS_PRICING_EXT_DISCOUNT_AMOUNT),
        getOrNull(csPricing.getExtSalesPrice(), CS_PRICING_EXT_SALES_PRICE),
        getOrNull(csPricing.getExtWholesaleCost(), CS_PRICING_EXT_WHOLESALE_COST),
        getOrNull(csPricing.getExtListPrice(), CS_PRICING_EXT_LIST_PRICE),
        getOrNull(csPricing.getExtTax(), CS_PRICING_EXT_TAX),
        getOrNull(csPricing.getCouponAmount(), CS_PRICING_COUPON_AMT),
        getOrNull(csPricing.getExtShipCost(), CS_PRICING_EXT_SHIP_COST),
        getOrNull(csPricing.getNetPaid(), CS_PRICING_NET_PAID),
        getOrNull(csPricing.getNetPaidIncludingTax(), CS_PRICING_NET_PAID_INC_TAX),
        getOrNull(csPricing.getNetPaidIncludingShipping(), CS_PRICING_NET_PAID_INC_SHIP),
        getOrNull(csPricing.getNetPaidIncludingShippingAndTax(), CS_PRICING_NET_PAID_INC_SHIP_TAX),
        getOrNull(csPricing.getNetProfit(), CS_PRICING_NET_PROFIT)
    };
  }
}
