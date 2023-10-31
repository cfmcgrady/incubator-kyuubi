package io.trino.tpcds.row;

    import io.trino.tpcds.type.Address;
    import io.trino.tpcds.type.Decimal;
    import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

    import java.util.List;

    import static com.google.common.collect.Lists.newArrayList;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_ADDRESS_CITY;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_ADDRESS_COUNTRY;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_ADDRESS_COUNTY;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_ADDRESS_GMT_OFFSET;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_ADDRESS_STATE;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_ADDRESS_STREET_NAME1;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_ADDRESS_STREET_NUM;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_ADDRESS_STREET_TYPE;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_ADDRESS_SUITE_NUM;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_ADDRESS_ZIP;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_CLASS;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_CLOSE_DATE;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_COMPANY_ID;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_COMPANY_NAME;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_MANAGER;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_MARKET_CLASS;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_MARKET_DESC;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_MARKET_ID;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_MARKET_MANAGER;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_NAME;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_OPEN_DATE;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_REC_END_DATE_ID;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_REC_START_DATE_ID;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_SITE_ID;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_SITE_SK;
    import static io.trino.tpcds.generator.WebSiteGeneratorColumn.WEB_TAX_PERCENTAGE;
    import static java.lang.String.format;

public class WebSiteRow
    extends KyuubiTPCDSTableRowWithNulls
{
  private final long webSiteSk;
  private final String webSiteId;
  private final long webRecStartDateId;
  private final long webRecEndDateId;
  private final String webName;
  private final long webOpenDate;
  private final long webCloseDate;
  private final String webClass;
  private final String webManager;
  private final int webMarketId;
  private final String webMarketClass;
  private final String webMarketDesc;
  private final String webMarketManager;
  private final int webCompanyId;
  private final String webCompanyName;
  private final Address webAddress;
  private final Decimal webTaxPercentage;

  public WebSiteRow(long nullBitMap,
      long webSiteSk,
      String webSiteId,
      long webRecStartDateId,
      long webRecEndDateId,
      String webName,
      long webOpenDate,
      long webCloseDate,
      String webClass,
      String webManager,
      int webMarketId,
      String webMarketClass,
      String webMarketDesc,
      String webMarketManager,
      int webCompanyId,
      String webCompanyName,
      Address webAddress,
      Decimal webTaxPercentage)
  {
    super(nullBitMap, WEB_SITE_SK);
    this.webSiteSk = webSiteSk;
    this.webSiteId = webSiteId;
    this.webRecStartDateId = webRecStartDateId;
    this.webRecEndDateId = webRecEndDateId;
    this.webName = webName;
    this.webOpenDate = webOpenDate;
    this.webCloseDate = webCloseDate;
    this.webClass = webClass;
    this.webManager = webManager;
    this.webMarketId = webMarketId;
    this.webMarketClass = webMarketClass;
    this.webMarketDesc = webMarketDesc;
    this.webMarketManager = webMarketManager;
    this.webCompanyId = webCompanyId;
    this.webCompanyName = webCompanyName;
    this.webAddress = webAddress;
    this.webTaxPercentage = webTaxPercentage;
  }

  public String getWebName()
  {
    return webName;
  }

  public long getWebOpenDate()
  {
    return webOpenDate;
  }

  public long getWebCloseDate()
  {
    return webCloseDate;
  }

  public String getWebClass()
  {
    return webClass;
  }

  public String getWebManager()
  {
    return webManager;
  }

  public int getWebMarketId()
  {
    return webMarketId;
  }

  public String getWebMarketClass()
  {
    return webMarketClass;
  }

  public String getWebMarketDesc()
  {
    return webMarketDesc;
  }

  public String getWebMarketManager()
  {
    return webMarketManager;
  }

  public int getWebCompanyId()
  {
    return webCompanyId;
  }

  public String getWebCompanyName()
  {
    return webCompanyName;
  }

  public Address getWebAddress()
  {
    return webAddress;
  }

  public Decimal getWebTaxPercentage()
  {
    return webTaxPercentage;
  }

  public long getWebSiteSk() {
    return webSiteSk;
  }

  public String getWebSiteId() {
    return webSiteId;
  }

  public long getWebRecStartDateId() {
    return webRecStartDateId;
  }

  public long getWebRecEndDateId() {
    return webRecEndDateId;
  }

  @Override public Object[] values() {
    return new Object[] {
        getOrNullForKey(webSiteSk, WEB_SITE_SK),
        getOrNull(webSiteId, WEB_SITE_ID),
        getDateOrNullFromJulianDays(webRecStartDateId, WEB_REC_START_DATE_ID),
        getDateOrNullFromJulianDays(webRecEndDateId, WEB_REC_END_DATE_ID),
        getOrNull(webName, WEB_NAME),
        getOrNullForKey(webOpenDate, WEB_OPEN_DATE),
        getOrNullForKey(webCloseDate, WEB_CLOSE_DATE),
        getOrNull(webClass, WEB_CLASS),
        getOrNull(webManager, WEB_MANAGER),
        getOrNull(webMarketId, WEB_MARKET_ID),
        getOrNull(webMarketClass, WEB_MARKET_CLASS),
        getOrNull(webMarketDesc, WEB_MARKET_DESC),
        getOrNull(webMarketManager, WEB_MARKET_MANAGER),
        getOrNull(webCompanyId, WEB_COMPANY_ID),
        getOrNull(webCompanyName, WEB_COMPANY_NAME),
        getOrNull(webAddress.getStreetNumber(), WEB_ADDRESS_STREET_NUM),
        getOrNull(webAddress.getStreetName(), WEB_ADDRESS_STREET_NAME1),
        getOrNull(webAddress.getStreetType(), WEB_ADDRESS_STREET_TYPE),
        getOrNull(webAddress.getSuiteNumber(), WEB_ADDRESS_SUITE_NUM),
        getOrNull(webAddress.getCity(), WEB_ADDRESS_CITY),
        getOrNull(webAddress.getCounty(), WEB_ADDRESS_COUNTY),
        getOrNull(webAddress.getState(), WEB_ADDRESS_STATE),
        getOrNull(format("%05d", webAddress.getZip()), WEB_ADDRESS_ZIP),
        getOrNull(webAddress.getCountry(), WEB_ADDRESS_COUNTRY),
        getOrNull(webAddress.getGmtOffset(), WEB_ADDRESS_GMT_OFFSET),
        getOrNull(webTaxPercentage, WEB_TAX_PERCENTAGE)
    };
  }
}
