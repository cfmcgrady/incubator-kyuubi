package io.trino.tpcds.row;

    import io.trino.tpcds.type.Pricing;
    import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_CALL_CENTER_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_CATALOG_PAGE_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_ITEM_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_ORDER_NUMBER;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_PRICING_EXT_SHIP_COST;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_PRICING_EXT_TAX;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_PRICING_FEE;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_PRICING_NET_LOSS;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_PRICING_NET_PAID;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_PRICING_NET_PAID_INC_TAX;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_PRICING_QUANTITY;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_PRICING_REFUNDED_CASH;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_PRICING_REVERSED_CHARGE;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_PRICING_STORE_CREDIT;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_REASON_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_REFUNDED_ADDR_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_REFUNDED_CDEMO_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_REFUNDED_CUSTOMER_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_REFUNDED_HDEMO_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_RETURNED_DATE_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_RETURNED_TIME_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_RETURNING_ADDR_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_RETURNING_CDEMO_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_RETURNING_CUSTOMER_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_RETURNING_HDEMO_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_SHIP_MODE_SK;
    import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_WAREHOUSE_SK;

public class CatalogReturnsRow
    extends KyuubiTPCDSTableRowWithNulls
{
  private final long crReturnedDateSk;
  private final long crReturnedTimeSk;
  private final long crItemSk;
  private final long crRefundedCustomerSk;
  private final long crRefundedCdemoSk;
  private final long crRefundedHdemoSk;
  private final long crRefundedAddrSk;
  private final long crReturningCustomerSk;
  private final long crReturningCdemoSk;
  private final long crReturningHdemoSk;
  private final long crReturningAddrSk;
  private final long crCallCenterSk;
  private final long crCatalogPageSk;
  private final long crShipModeSk;
  private final long crWarehouseSk;
  private final long crReasonSk;
  private final long crOrderNumber;
  private final Pricing crPricing;

  public CatalogReturnsRow(long crReturnedDateSk,
      long crReturnedTimeSk,
      long crItemSk,
      long crRefundedCustomerSk,
      long crRefundedCdemoSk,
      long crRefundedHdemoSk,
      long crRefundedAddrSk,
      long crReturningCustomerSk,
      long crReturningCdemoSk,
      long crReturningHdemoSk,
      long crReturningAddrSk,
      long crCallCenterSk,
      long crCatalogPageSk,
      long crShipModeSk,
      long crWarehouseSk,
      long crReasonSk,
      long crOrderNumber,
      Pricing crPricing,
      long nullBitMap)
  {
    super(nullBitMap, CR_RETURNED_DATE_SK);
    this.crReturnedDateSk = crReturnedDateSk;
    this.crReturnedTimeSk = crReturnedTimeSk;
    this.crItemSk = crItemSk;
    this.crRefundedCustomerSk = crRefundedCustomerSk;
    this.crRefundedCdemoSk = crRefundedCdemoSk;
    this.crRefundedHdemoSk = crRefundedHdemoSk;
    this.crRefundedAddrSk = crRefundedAddrSk;
    this.crReturningCustomerSk = crReturningCustomerSk;
    this.crReturningCdemoSk = crReturningCdemoSk;
    this.crReturningHdemoSk = crReturningHdemoSk;
    this.crReturningAddrSk = crReturningAddrSk;
    this.crCallCenterSk = crCallCenterSk;
    this.crCatalogPageSk = crCatalogPageSk;
    this.crShipModeSk = crShipModeSk;
    this.crWarehouseSk = crWarehouseSk;
    this.crReasonSk = crReasonSk;
    this.crOrderNumber = crOrderNumber;
    this.crPricing = crPricing;
  }

  public long getCrReturnedDateSk() {
    return crReturnedDateSk;
  }

  public long getCrReturnedTimeSk() {
    return crReturnedTimeSk;
  }

  public long getCrItemSk() {
    return crItemSk;
  }

  public long getCrRefundedCustomerSk() {
    return crRefundedCustomerSk;
  }

  public long getCrRefundedCdemoSk() {
    return crRefundedCdemoSk;
  }

  public long getCrRefundedHdemoSk() {
    return crRefundedHdemoSk;
  }

  public long getCrRefundedAddrSk() {
    return crRefundedAddrSk;
  }

  public long getCrReturningCustomerSk() {
    return crReturningCustomerSk;
  }

  public long getCrReturningCdemoSk() {
    return crReturningCdemoSk;
  }

  public long getCrReturningHdemoSk() {
    return crReturningHdemoSk;
  }

  public long getCrReturningAddrSk() {
    return crReturningAddrSk;
  }

  public long getCrCallCenterSk() {
    return crCallCenterSk;
  }

  public long getCrCatalogPageSk() {
    return crCatalogPageSk;
  }

  public long getCrShipModeSk() {
    return crShipModeSk;
  }

  public long getCrWarehouseSk() {
    return crWarehouseSk;
  }

  public long getCrReasonSk() {
    return crReasonSk;
  }

  public long getCrOrderNumber() {
    return crOrderNumber;
  }

  public Pricing getCrPricing() {
    return crPricing;
  }

  @Override public Object[] values() {
    return new Object[] {
        getOrNullForKey(crReturnedDateSk, CR_RETURNED_DATE_SK),
        getOrNullForKey(crReturnedTimeSk, CR_RETURNED_TIME_SK),
        getOrNullForKey(crItemSk, CR_ITEM_SK),
        getOrNullForKey(crRefundedCustomerSk, CR_REFUNDED_CUSTOMER_SK),
        getOrNullForKey(crRefundedCdemoSk, CR_REFUNDED_CDEMO_SK),
        getOrNullForKey(crRefundedHdemoSk, CR_REFUNDED_HDEMO_SK),
        getOrNullForKey(crRefundedAddrSk, CR_REFUNDED_ADDR_SK),
        getOrNullForKey(crReturningCustomerSk, CR_RETURNING_CUSTOMER_SK),
        getOrNullForKey(crReturningCdemoSk, CR_RETURNING_CDEMO_SK),
        getOrNullForKey(crReturningHdemoSk, CR_RETURNING_HDEMO_SK),
        getOrNullForKey(crReturningAddrSk, CR_RETURNING_ADDR_SK),
        getOrNullForKey(crCallCenterSk, CR_CALL_CENTER_SK),
        getOrNullForKey(crCatalogPageSk, CR_CATALOG_PAGE_SK),
        getOrNullForKey(crShipModeSk, CR_SHIP_MODE_SK),
        getOrNullForKey(crWarehouseSk, CR_WAREHOUSE_SK),
        getOrNullForKey(crReasonSk, CR_REASON_SK),
        getOrNull(crOrderNumber, CR_ORDER_NUMBER),
        getOrNull(crPricing.getQuantity(), CR_PRICING_QUANTITY),
        getOrNull(crPricing.getNetPaid(), CR_PRICING_NET_PAID),
        getOrNull(crPricing.getExtTax(), CR_PRICING_EXT_TAX),
        getOrNull(crPricing.getNetPaidIncludingTax(), CR_PRICING_NET_PAID_INC_TAX),
        getOrNull(crPricing.getFee(), CR_PRICING_FEE),
        getOrNull(crPricing.getExtShipCost(), CR_PRICING_EXT_SHIP_COST),
        getOrNull(crPricing.getRefundedCash(), CR_PRICING_REFUNDED_CASH),
        getOrNull(crPricing.getReversedCharge(), CR_PRICING_REVERSED_CHARGE),
        getOrNull(crPricing.getStoreCredit(), CR_PRICING_STORE_CREDIT),
        getOrNull(crPricing.getNetLoss(), CR_PRICING_NET_LOSS)
    };
  }
}
