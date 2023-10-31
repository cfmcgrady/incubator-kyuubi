package io.trino.tpcds.row;

    import io.trino.tpcds.type.Decimal;
    import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

    import java.util.List;

    import static com.google.common.collect.Lists.newArrayList;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_CHANNEL_CATALOG;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_CHANNEL_DEMO;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_CHANNEL_DETAILS;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_CHANNEL_DMAIL;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_CHANNEL_EMAIL;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_CHANNEL_EVENT;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_CHANNEL_PRESS;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_CHANNEL_RADIO;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_CHANNEL_TV;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_COST;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_DISCOUNT_ACTIVE;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_END_DATE_ID;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_ITEM_SK;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_PROMO_ID;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_PROMO_NAME;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_PROMO_SK;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_PURPOSE;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_RESPONSE_TARGET;
    import static io.trino.tpcds.generator.PromotionGeneratorColumn.P_START_DATE_ID;

public class PromotionRow
    extends KyuubiTPCDSTableRowWithNulls
{
  private final long pPromoSk;
  private final String pPromoId;
  private final long pStartDateId;
  private final long pEndDateId;
  private final long pItemSk;
  private final Decimal pCost;
  private final int pResponseTarget;
  private final String pPromoName;
  private final boolean pChannelDmail;
  private final boolean pChannelEmail;
  private final boolean pChannelCatalog;
  private final boolean pChannelTv;
  private final boolean pChannelRadio;
  private final boolean pChannelPress;
  private final boolean pChannelEvent;
  private final boolean pChannelDemo;
  private final String pChannelDetails;
  private final String pPurpose;
  private final boolean pDiscountActive;

  public PromotionRow(long nullBitMap,
      long pPromoSk,
      String pPromoId,
      long pStartDateId,
      long pEndDateId,
      long pItemSk,
      Decimal pCost,
      int pResponseTarget,
      String pPromoName,
      boolean pChannelDmail,
      boolean pChannelEmail,
      boolean pChannelCatalog,
      boolean pChannelTv,
      boolean pChannelRadio,
      boolean pChannelPress,
      boolean pChannelEvent,
      boolean pChannelDemo,
      String pChannelDetails,
      String pPurpose,
      boolean pDiscountActive)
  {
    super(nullBitMap, P_PROMO_SK);
    this.pPromoSk = pPromoSk;
    this.pPromoId = pPromoId;
    this.pStartDateId = pStartDateId;
    this.pEndDateId = pEndDateId;
    this.pItemSk = pItemSk;
    this.pCost = pCost;
    this.pResponseTarget = pResponseTarget;
    this.pPromoName = pPromoName;
    this.pChannelDmail = pChannelDmail;
    this.pChannelEmail = pChannelEmail;
    this.pChannelCatalog = pChannelCatalog;
    this.pChannelTv = pChannelTv;
    this.pChannelRadio = pChannelRadio;
    this.pChannelPress = pChannelPress;
    this.pChannelEvent = pChannelEvent;
    this.pChannelDemo = pChannelDemo;
    this.pChannelDetails = pChannelDetails;
    this.pPurpose = pPurpose;
    this.pDiscountActive = pDiscountActive;
  }

  public long getpPromoSk() {
    return pPromoSk;
  }

  public String getpPromoId() {
    return pPromoId;
  }

  public long getpStartDateId() {
    return pStartDateId;
  }

  public long getpEndDateId() {
    return pEndDateId;
  }

  public long getpItemSk() {
    return pItemSk;
  }

  public Decimal getpCost() {
    return pCost;
  }

  public int getpResponseTarget() {
    return pResponseTarget;
  }

  public String getpPromoName() {
    return pPromoName;
  }

  public boolean ispChannelDmail() {
    return pChannelDmail;
  }

  public boolean ispChannelEmail() {
    return pChannelEmail;
  }

  public boolean ispChannelCatalog() {
    return pChannelCatalog;
  }

  public boolean ispChannelTv() {
    return pChannelTv;
  }

  public boolean ispChannelRadio() {
    return pChannelRadio;
  }

  public boolean ispChannelPress() {
    return pChannelPress;
  }

  public boolean ispChannelEvent() {
    return pChannelEvent;
  }

  public boolean ispChannelDemo() {
    return pChannelDemo;
  }

  public String getpChannelDetails() {
    return pChannelDetails;
  }

  public String getpPurpose() {
    return pPurpose;
  }

  public boolean ispDiscountActive() {
    return pDiscountActive;
  }
  @Override public Object[] values() {
    return new Object[] {
        getOrNullForKey(pPromoSk, P_PROMO_SK),
        getOrNull(pPromoId, P_PROMO_ID),
        getOrNullForKey(pStartDateId, P_START_DATE_ID),
        getOrNullForKey(pEndDateId, P_END_DATE_ID),
        getOrNullForKey(pItemSk, P_ITEM_SK),
        getOrNull(pCost, P_COST),
        getOrNull(pResponseTarget, P_RESPONSE_TARGET),
        getOrNull(pPromoName, P_PROMO_NAME),
        getOrNullForBoolean(pChannelDmail, P_CHANNEL_DMAIL),
        getOrNullForBoolean(pChannelEmail, P_CHANNEL_EMAIL),
        getOrNullForBoolean(pChannelCatalog, P_CHANNEL_CATALOG),
        getOrNullForBoolean(pChannelTv, P_CHANNEL_TV),
        getOrNullForBoolean(pChannelRadio, P_CHANNEL_RADIO),
        getOrNullForBoolean(pChannelPress, P_CHANNEL_PRESS),
        getOrNullForBoolean(pChannelEvent, P_CHANNEL_EVENT),
        getOrNullForBoolean(pChannelDemo, P_CHANNEL_DEMO),
        getOrNull(pChannelDetails, P_CHANNEL_DETAILS),
        getOrNull(pPurpose, P_PURPOSE),
        getOrNullForBoolean(pDiscountActive, P_DISCOUNT_ACTIVE)
    };
  }
}
