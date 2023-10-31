package io.trino.tpcds.row;

import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_ITEM_SK;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_ORDER_NUMBER;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_PRICING_EXT_SHIP_COST;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_PRICING_EXT_TAX;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_PRICING_FEE;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_PRICING_NET_LOSS;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_PRICING_NET_PAID;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_PRICING_NET_PAID_INC_TAX;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_PRICING_QUANTITY;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_PRICING_REFUNDED_CASH;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_PRICING_REVERSED_CHARGE;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_PRICING_STORE_CREDIT;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_REASON_SK;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_REFUNDED_ADDR_SK;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_REFUNDED_CDEMO_SK;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_REFUNDED_CUSTOMER_SK;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_REFUNDED_HDEMO_SK;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_RETURNED_DATE_SK;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_RETURNED_TIME_SK;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_RETURNING_ADDR_SK;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_RETURNING_CDEMO_SK;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_RETURNING_CUSTOMER_SK;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_RETURNING_HDEMO_SK;
import static io.trino.tpcds.generator.WebReturnsGeneratorColumn.WR_WEB_PAGE_SK;

import io.trino.tpcds.type.Pricing;
import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

public class WebReturnsRow extends KyuubiTPCDSTableRowWithNulls {
  private final long wrReturnedDateSk;
  private final long wrReturnedTimeSk;
  private final long wrItemSk;
  private final long wrRefundedCustomerSk;
  private final long wrRefundedCdemoSk;
  private final long wrRefundedHdemoSk;
  private final long wrRefundedAddrSk;
  private final long wrReturningCustomerSk;
  private final long wrReturningCdemoSk;
  private final long wrReturningHdemoSk;
  private final long wrReturningAddrSk;
  private final long wrWebPageSk;
  private final long wrReasonSk;
  private final long wrOrderNumber;
  private final Pricing wrPricing;

  public WebReturnsRow(
      long nullBitMap,
      long wrReturnedDateSk,
      long wrReturnedTimeSk,
      long wrItemSk,
      long wrRefundedCustomerSk,
      long wrRefundedCdemoSk,
      long wrRefundedHdemoSk,
      long wrRefundedAddrSk,
      long wrReturningCustomerSk,
      long wrReturningCgdemoSk,
      long wrReturningHdemoSk,
      long wrReturningAddrSk,
      long wrWebPageSk,
      long wrReasonSk,
      long wrOrderNumber,
      Pricing wrPricing) {
    super(nullBitMap, WR_RETURNED_DATE_SK);
    this.wrReturnedDateSk = wrReturnedDateSk;
    this.wrReturnedTimeSk = wrReturnedTimeSk;
    this.wrItemSk = wrItemSk;
    this.wrRefundedCustomerSk = wrRefundedCustomerSk;
    this.wrRefundedCdemoSk = wrRefundedCdemoSk;
    this.wrRefundedHdemoSk = wrRefundedHdemoSk;
    this.wrRefundedAddrSk = wrRefundedAddrSk;
    this.wrReturningCustomerSk = wrReturningCustomerSk;
    this.wrReturningCdemoSk = wrReturningCgdemoSk;
    this.wrReturningHdemoSk = wrReturningHdemoSk;
    this.wrReturningAddrSk = wrReturningAddrSk;
    this.wrWebPageSk = wrWebPageSk;
    this.wrReasonSk = wrReasonSk;
    this.wrOrderNumber = wrOrderNumber;
    this.wrPricing = wrPricing;
  }

  public long getWrReturnedDateSk() {
    return wrReturnedDateSk;
  }

  public long getWrReturnedTimeSk() {
    return wrReturnedTimeSk;
  }

  public long getWrItemSk() {
    return wrItemSk;
  }

  public long getWrRefundedCustomerSk() {
    return wrRefundedCustomerSk;
  }

  public long getWrRefundedCdemoSk() {
    return wrRefundedCdemoSk;
  }

  public long getWrRefundedHdemoSk() {
    return wrRefundedHdemoSk;
  }

  public long getWrRefundedAddrSk() {
    return wrRefundedAddrSk;
  }

  public long getWrReturningCustomerSk() {
    return wrReturningCustomerSk;
  }

  public long getWrReturningCdemoSk() {
    return wrReturningCdemoSk;
  }

  public long getWrReturningHdemoSk() {
    return wrReturningHdemoSk;
  }

  public long getWrReturningAddrSk() {
    return wrReturningAddrSk;
  }

  public long getWrWebPageSk() {
    return wrWebPageSk;
  }

  public long getWrReasonSk() {
    return wrReasonSk;
  }

  public long getWrOrderNumber() {
    return wrOrderNumber;
  }

  public Pricing getWrPricing() {
    return wrPricing;
  }

  @Override
  public Object[] values() {
    return new Object[] {
      getOrNullForKey(wrReturnedDateSk, WR_RETURNED_DATE_SK),
      getOrNullForKey(wrReturnedTimeSk, WR_RETURNED_TIME_SK),
      getOrNullForKey(wrItemSk, WR_ITEM_SK),
      getOrNullForKey(wrRefundedCustomerSk, WR_REFUNDED_CUSTOMER_SK),
      getOrNullForKey(wrRefundedCdemoSk, WR_REFUNDED_CDEMO_SK),
      getOrNullForKey(wrRefundedHdemoSk, WR_REFUNDED_HDEMO_SK),
      getOrNullForKey(wrRefundedAddrSk, WR_REFUNDED_ADDR_SK),
      getOrNullForKey(wrReturningCustomerSk, WR_RETURNING_CUSTOMER_SK),
      getOrNullForKey(wrReturningCdemoSk, WR_RETURNING_CDEMO_SK),
      getOrNullForKey(wrReturningHdemoSk, WR_RETURNING_HDEMO_SK),
      getOrNullForKey(wrReturningAddrSk, WR_RETURNING_ADDR_SK),
      getOrNullForKey(wrWebPageSk, WR_WEB_PAGE_SK),
      getOrNullForKey(wrReasonSk, WR_REASON_SK),
      getOrNullForKey(wrOrderNumber, WR_ORDER_NUMBER),
      getOrNull(wrPricing.getQuantity(), WR_PRICING_QUANTITY),
      getOrNull(wrPricing.getNetPaid(), WR_PRICING_NET_PAID),
      getOrNull(wrPricing.getExtTax(), WR_PRICING_EXT_TAX),
      getOrNull(wrPricing.getNetPaidIncludingTax(), WR_PRICING_NET_PAID_INC_TAX),
      getOrNull(wrPricing.getFee(), WR_PRICING_FEE),
      getOrNull(wrPricing.getExtShipCost(), WR_PRICING_EXT_SHIP_COST),
      getOrNull(wrPricing.getRefundedCash(), WR_PRICING_REFUNDED_CASH),
      getOrNull(wrPricing.getReversedCharge(), WR_PRICING_REVERSED_CHARGE),
      getOrNull(wrPricing.getStoreCredit(), WR_PRICING_STORE_CREDIT),
      getOrNull(wrPricing.getNetLoss(), WR_PRICING_NET_LOSS)
    };
  }
}
