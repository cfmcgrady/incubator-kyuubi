package io.trino.tpcds.row;

    import java.util.List;

    import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

    import static com.google.common.collect.Lists.newArrayList;
    import static io.trino.tpcds.generator.WebPageGeneratorColumn.WP_ACCESS_DATE_SK;
    import static io.trino.tpcds.generator.WebPageGeneratorColumn.WP_AUTOGEN_FLAG;
    import static io.trino.tpcds.generator.WebPageGeneratorColumn.WP_CHAR_COUNT;
    import static io.trino.tpcds.generator.WebPageGeneratorColumn.WP_CREATION_DATE_SK;
    import static io.trino.tpcds.generator.WebPageGeneratorColumn.WP_CUSTOMER_SK;
    import static io.trino.tpcds.generator.WebPageGeneratorColumn.WP_IMAGE_COUNT;
    import static io.trino.tpcds.generator.WebPageGeneratorColumn.WP_LINK_COUNT;
    import static io.trino.tpcds.generator.WebPageGeneratorColumn.WP_MAX_AD_COUNT;
    import static io.trino.tpcds.generator.WebPageGeneratorColumn.WP_PAGE_ID;
    import static io.trino.tpcds.generator.WebPageGeneratorColumn.WP_PAGE_SK;
    import static io.trino.tpcds.generator.WebPageGeneratorColumn.WP_REC_END_DATE_ID;
    import static io.trino.tpcds.generator.WebPageGeneratorColumn.WP_REC_START_DATE_ID;
    import static io.trino.tpcds.generator.WebPageGeneratorColumn.WP_TYPE;
    import static io.trino.tpcds.generator.WebPageGeneratorColumn.WP_URL;

public class WebPageRow
    extends KyuubiTPCDSTableRowWithNulls
{
  private final long wpPageSk;
  private final String wpPageId;
  private final long wpRecStartDateId;
  private final long wpRecEndDateId;
  private final long wpCreationDateSk;
  private final long wpAccessDateSk;
  private final boolean wpAutogenFlag;
  private final long wpCustomerSk;
  private final String wpUrl;
  private final String wpType;
  private final int wpCharCount;
  private final int wpLinkCount;
  private final int wpImageCount;
  private final int wpMaxAdCount;

  public WebPageRow(long nullBitMap,
      long wpPageSk,
      String wpPageId,
      long wpRecStartDateId,
      long wpRecEndDateId,
      long wpCreationDateSk,
      long wpAccessDateSk,
      boolean wpAutogenFlag,
      long wpCustomerSk,
      String wpUrl,
      String wpType,
      int wpCharCount,
      int wpLinkCount,
      int wpImageCount,
      int wpMaxAdCount)
  {
    super(nullBitMap, WP_PAGE_SK);
    this.wpPageSk = wpPageSk;
    this.wpPageId = wpPageId;
    this.wpRecStartDateId = wpRecStartDateId;
    this.wpRecEndDateId = wpRecEndDateId;
    this.wpCreationDateSk = wpCreationDateSk;
    this.wpAccessDateSk = wpAccessDateSk;
    this.wpAutogenFlag = wpAutogenFlag;
    this.wpCustomerSk = wpCustomerSk;
    this.wpUrl = wpUrl;
    this.wpType = wpType;
    this.wpCharCount = wpCharCount;
    this.wpLinkCount = wpLinkCount;
    this.wpImageCount = wpImageCount;
    this.wpMaxAdCount = wpMaxAdCount;
  }

  public long getWpCreationDateSk()
  {
    return wpCreationDateSk;
  }

  public long getWpAccessDateSk()
  {
    return wpAccessDateSk;
  }

  public boolean getWpAutogenFlag()
  {
    return wpAutogenFlag;
  }

  public long getWpCustomerSk()
  {
    return wpCustomerSk;
  }

  public int getWpCharCount()
  {
    return wpCharCount;
  }

  public int getWpLinkCount()
  {
    return wpLinkCount;
  }

  public int getWpImageCount()
  {
    return wpImageCount;
  }

  public int getWpMaxAdCount()
  {
    return wpMaxAdCount;
  }

  public long getWpPageSk() {
    return wpPageSk;
  }

  public String getWpPageId() {
    return wpPageId;
  }

  public long getWpRecStartDateId() {
    return wpRecStartDateId;
  }

  public long getWpRecEndDateId() {
    return wpRecEndDateId;
  }

  public boolean isWpAutogenFlag() {
    return wpAutogenFlag;
  }

  public String getWpUrl() {
    return wpUrl;
  }

  public String getWpType() {
    return wpType;
  }

  @Override public Object[] values() {
    return new Object[] {
        getOrNullForKey(wpPageSk, WP_PAGE_SK),
        getOrNull(wpPageId, WP_PAGE_ID),
        getDateOrNullFromJulianDays(wpRecStartDateId, WP_REC_START_DATE_ID),
        getDateOrNullFromJulianDays(wpRecEndDateId, WP_REC_END_DATE_ID),
        getOrNullForKey(wpCreationDateSk, WP_CREATION_DATE_SK),
        getOrNullForKey(wpAccessDateSk, WP_ACCESS_DATE_SK),
        getOrNullForBoolean(wpAutogenFlag, WP_AUTOGEN_FLAG),
        getOrNullForKey(wpCustomerSk, WP_CUSTOMER_SK),
        getOrNull(wpUrl, WP_URL),
        getOrNull(wpType, WP_TYPE),
        getOrNull(wpCharCount, WP_CHAR_COUNT),
        getOrNull(wpLinkCount, WP_LINK_COUNT),
        getOrNull(wpImageCount, WP_IMAGE_COUNT),
        getOrNull(wpMaxAdCount, WP_MAX_AD_COUNT)
    };
  }
}
