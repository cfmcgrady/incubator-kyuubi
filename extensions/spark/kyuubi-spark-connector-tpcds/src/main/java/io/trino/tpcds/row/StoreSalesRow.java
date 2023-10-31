package io.trino.tpcds.row;

import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_PRICING_COUPON_AMT;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_PRICING_EXT_LIST_PRICE;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_PRICING_EXT_SALES_PRICE;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_PRICING_EXT_TAX;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_PRICING_EXT_WHOLESALE_COST;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_PRICING_LIST_PRICE;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_PRICING_NET_PAID;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_PRICING_NET_PAID_INC_TAX;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_PRICING_NET_PROFIT;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_PRICING_QUANTITY;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_PRICING_SALES_PRICE;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_PRICING_WHOLESALE_COST;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_SOLD_ADDR_SK;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_SOLD_CDEMO_SK;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_SOLD_CUSTOMER_SK;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_SOLD_DATE_SK;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_SOLD_HDEMO_SK;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_SOLD_ITEM_SK;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_SOLD_PROMO_SK;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_SOLD_STORE_SK;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_SOLD_TIME_SK;
import static io.trino.tpcds.generator.StoreSalesGeneratorColumn.SS_TICKET_NUMBER;

import io.trino.tpcds.type.Pricing;
import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

public class StoreSalesRow extends KyuubiTPCDSTableRowWithNulls {
  private final long ssSoldDateSk;
  private final long ssSoldTimeSk;
  private final long ssSoldItemSk;
  private final long ssSoldCustomerSk;
  private final long ssSoldCdemoSk;
  private final long ssSoldHdemoSk;
  private final long ssSoldAddrSk;
  private final long ssSoldStoreSk;
  private final long ssSoldPromoSk;
  private final long ssTicketNumber;
  private final Pricing ssPricing;

  public StoreSalesRow(
      long nullBitMap,
      long ssSoldDateSk,
      long ssSoldTimeSk,
      long ssSoldItemSk,
      long ssSoldCustomerSk,
      long ssSoldCdemoSk,
      long ssSoldHdemoSk,
      long ssSoldAddrSk,
      long ssSoldStoreSk,
      long ssSoldPromoSk,
      long ssTicketNumber,
      Pricing ssPricing) {
    super(nullBitMap, SS_SOLD_DATE_SK);
    this.ssSoldDateSk = ssSoldDateSk;
    this.ssSoldTimeSk = ssSoldTimeSk;
    this.ssSoldItemSk = ssSoldItemSk;
    this.ssSoldCustomerSk = ssSoldCustomerSk;
    this.ssSoldCdemoSk = ssSoldCdemoSk;
    this.ssSoldHdemoSk = ssSoldHdemoSk;
    this.ssSoldAddrSk = ssSoldAddrSk;
    this.ssSoldStoreSk = ssSoldStoreSk;
    this.ssSoldPromoSk = ssSoldPromoSk;
    this.ssTicketNumber = ssTicketNumber;
    this.ssPricing = ssPricing;
  }

  public long getSsTicketNumber() {
    return ssTicketNumber;
  }

  public long getSsSoldItemSk() {
    return ssSoldItemSk;
  }

  public long getSsSoldCustomerSk() {
    return ssSoldCustomerSk;
  }

  public long getSsSoldDateSk() {
    return ssSoldDateSk;
  }

  public Pricing getSsPricing() {
    return ssPricing;
  }

  public long getSsSoldTimeSk() {
    return ssSoldTimeSk;
  }

  public long getSsSoldCdemoSk() {
    return ssSoldCdemoSk;
  }

  public long getSsSoldHdemoSk() {
    return ssSoldHdemoSk;
  }

  public long getSsSoldAddrSk() {
    return ssSoldAddrSk;
  }

  public long getSsSoldStoreSk() {
    return ssSoldStoreSk;
  }

  public long getSsSoldPromoSk() {
    return ssSoldPromoSk;
  }

  @Override
  public Object[] values() {
    return new Object[] {
      getOrNullForKey(ssSoldDateSk, SS_SOLD_DATE_SK),
      getOrNullForKey(ssSoldTimeSk, SS_SOLD_TIME_SK),
      getOrNullForKey(ssSoldItemSk, SS_SOLD_ITEM_SK),
      getOrNullForKey(ssSoldCustomerSk, SS_SOLD_CUSTOMER_SK),
      getOrNullForKey(ssSoldCdemoSk, SS_SOLD_CDEMO_SK),
      getOrNullForKey(ssSoldHdemoSk, SS_SOLD_HDEMO_SK),
      getOrNullForKey(ssSoldAddrSk, SS_SOLD_ADDR_SK),
      getOrNullForKey(ssSoldStoreSk, SS_SOLD_STORE_SK),
      getOrNullForKey(ssSoldPromoSk, SS_SOLD_PROMO_SK),
      getOrNullForKey(ssTicketNumber, SS_TICKET_NUMBER),
      getOrNull(ssPricing.getQuantity(), SS_PRICING_QUANTITY),
      getOrNull(ssPricing.getWholesaleCost(), SS_PRICING_WHOLESALE_COST),
      getOrNull(ssPricing.getListPrice(), SS_PRICING_LIST_PRICE),
      getOrNull(ssPricing.getSalesPrice(), SS_PRICING_SALES_PRICE),
      getOrNull(ssPricing.getCouponAmount(), SS_PRICING_COUPON_AMT),
      getOrNull(ssPricing.getExtSalesPrice(), SS_PRICING_EXT_SALES_PRICE),
      getOrNull(ssPricing.getExtWholesaleCost(), SS_PRICING_EXT_WHOLESALE_COST),
      getOrNull(ssPricing.getExtListPrice(), SS_PRICING_EXT_LIST_PRICE),
      getOrNull(ssPricing.getExtTax(), SS_PRICING_EXT_TAX),
      getOrNull(ssPricing.getCouponAmount(), SS_PRICING_COUPON_AMT),
      getOrNull(ssPricing.getNetPaid(), SS_PRICING_NET_PAID),
      getOrNull(ssPricing.getNetPaidIncludingTax(), SS_PRICING_NET_PAID_INC_TAX),
      getOrNull(ssPricing.getNetProfit(), SS_PRICING_NET_PROFIT)
    };
  }
}
