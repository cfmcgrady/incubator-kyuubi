package io.trino.tpcds.row;

import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_ADDR_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_CDEMO_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_CUSTOMER_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_HDEMO_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_ITEM_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_PRICING_EXT_SHIP_COST;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_PRICING_EXT_TAX;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_PRICING_FEE;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_PRICING_NET_LOSS;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_PRICING_NET_PAID;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_PRICING_NET_PAID_INC_TAX;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_PRICING_QUANTITY;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_PRICING_REFUNDED_CASH;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_PRICING_REVERSED_CHARGE;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_PRICING_STORE_CREDIT;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_REASON_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_RETURNED_DATE_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_RETURNED_TIME_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_STORE_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_TICKET_NUMBER;

import io.trino.tpcds.type.Pricing;
import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

public class StoreReturnsRow extends KyuubiTPCDSTableRowWithNulls {
  private final long srReturnedDateSk;
  private final long srReturnedTimeSk;
  private final long srItemSk;
  private final long srCustomerSk;
  private final long srCdemoSk;
  private final long srHdemoSk;
  private final long srAddrSk;
  private final long srStoreSk;
  private final long srReasonSk;
  private final long srTicketNumber;
  private final Pricing srPricing;

  public StoreReturnsRow(
      long nullBitMap,
      long srReturnedDateSk,
      long srReturnedTimeSk,
      long srItemSk,
      long srCustomerSk,
      long srCdemoSk,
      long srHdemoSk,
      long srAddrSk,
      long srStoreSk,
      long srReasonSk,
      long srTicketNumber,
      Pricing srPricing) {
    super(nullBitMap, SR_RETURNED_DATE_SK);
    this.srReturnedDateSk = srReturnedDateSk;
    this.srReturnedTimeSk = srReturnedTimeSk;
    this.srItemSk = srItemSk;
    this.srCustomerSk = srCustomerSk;
    this.srCdemoSk = srCdemoSk;
    this.srHdemoSk = srHdemoSk;
    this.srAddrSk = srAddrSk;
    this.srStoreSk = srStoreSk;
    this.srReasonSk = srReasonSk;
    this.srTicketNumber = srTicketNumber;
    this.srPricing = srPricing;
  }

  public long getSrReturnedDateSk() {
    return srReturnedDateSk;
  }

  public long getSrReturnedTimeSk() {
    return srReturnedTimeSk;
  }

  public long getSrItemSk() {
    return srItemSk;
  }

  public long getSrCustomerSk() {
    return srCustomerSk;
  }

  public long getSrCdemoSk() {
    return srCdemoSk;
  }

  public long getSrHdemoSk() {
    return srHdemoSk;
  }

  public long getSrAddrSk() {
    return srAddrSk;
  }

  public long getSrStoreSk() {
    return srStoreSk;
  }

  public long getSrReasonSk() {
    return srReasonSk;
  }

  public long getSrTicketNumber() {
    return srTicketNumber;
  }

  public Pricing getSrPricing() {
    return srPricing;
  }

  @Override
  public Object[] values() {
    return new Object[] {
      getOrNullForKey(srReturnedDateSk, SR_RETURNED_DATE_SK),
      getOrNullForKey(srReturnedTimeSk, SR_RETURNED_TIME_SK),
      getOrNullForKey(srItemSk, SR_ITEM_SK),
      getOrNullForKey(srCustomerSk, SR_CUSTOMER_SK),
      getOrNullForKey(srCdemoSk, SR_CDEMO_SK),
      getOrNullForKey(srHdemoSk, SR_HDEMO_SK),
      getOrNullForKey(srAddrSk, SR_ADDR_SK),
      getOrNullForKey(srStoreSk, SR_STORE_SK),
      getOrNullForKey(srReasonSk, SR_REASON_SK),
      getOrNullForKey(srTicketNumber, SR_TICKET_NUMBER),
      getOrNull(srPricing.getQuantity(), SR_PRICING_QUANTITY),
      getOrNull(srPricing.getNetPaid(), SR_PRICING_NET_PAID),
      getOrNull(srPricing.getExtTax(), SR_PRICING_EXT_TAX),
      getOrNull(srPricing.getNetPaidIncludingTax(), SR_PRICING_NET_PAID_INC_TAX),
      getOrNull(srPricing.getFee(), SR_PRICING_FEE),
      getOrNull(srPricing.getExtShipCost(), SR_PRICING_EXT_SHIP_COST),
      getOrNull(srPricing.getRefundedCash(), SR_PRICING_REFUNDED_CASH),
      getOrNull(srPricing.getReversedCharge(), SR_PRICING_REVERSED_CHARGE),
      getOrNull(srPricing.getStoreCredit(), SR_PRICING_STORE_CREDIT),
      getOrNull(srPricing.getNetLoss(), SR_PRICING_NET_LOSS)
    };
  }
}
