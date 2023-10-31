package io.trino.tpcds.row;

import io.trino.tpcds.type.Address;
import io.trino.tpcds.type.Decimal;
import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_ADDRESS;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_CALL_CENTER_ID;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_CALL_CENTER_SK;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_CITY;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_CLASS;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_CLOSED_DATE_ID;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_COMPANY;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_COMPANY_NAME;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_COUNTRY;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_DIVISION;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_DIVISION_NAME;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_EMPLOYEES;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_GMT_OFFSET;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_HOURS;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_MANAGER;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_MARKET_CLASS;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_MARKET_DESC;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_MARKET_ID;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_MARKET_MANAGER;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_NAME;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_OPEN_DATE_ID;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_REC_END_DATE_ID;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_REC_START_DATE_ID;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_SQ_FT;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_STATE;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_STREET_NAME;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_STREET_NUMBER;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_STREET_TYPE;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_SUITE_NUMBER;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_TAX_PERCENTAGE;
import static io.trino.tpcds.generator.CallCenterGeneratorColumn.CC_ZIP;

public class CallCenterRow extends KyuubiTPCDSTableRowWithNulls {
  private final long ccCallCenterSk;
  private final String ccCallCenterId;
  private final long ccRecStartDateId;
  private final long ccRecEndDateId;
  private final long ccClosedDateId;
  private final long ccOpenDateId;
  private final String ccName;
  private final String ccClass;
  private final int ccEmployees;
  private final int ccSqFt;
  private final String ccHours;
  private final String ccManager;
  private final int ccMarketId;
  private final String ccMarketClass;
  private final String ccMarketDesc;
  private final String ccMarketManager;
  private final int ccDivisionId;
  private final String ccDivisionName;
  private final int ccCompany;
  private final String ccCompanyName;
  private final Address ccAddress;
  private final Decimal ccTaxPercentage;

  private CallCenterRow(long ccCallCenterSk,
      String ccCallCenterId,
      long ccRecStartDateId,
      long ccRecEndDateId,
      long ccClosedDateId,
      long ccOpenDateId,
      String ccName,
      String ccClass,
      int ccEmployees,
      int ccSqFt,
      String ccHours,
      String ccManager,
      int ccMarketId,
      String ccMarketClass,
      String ccMarketDesc,
      String ccMarketManager,
      int ccDivisionId,
      String ccDivisionName,
      int ccCompany,
      String ccCompanyName,
      Address ccAddress,
      Decimal ccTaxPercentage,
      long nullBitMap)
  {
    super(nullBitMap, CC_CALL_CENTER_SK);
    this.ccCallCenterSk = ccCallCenterSk;
    this.ccCallCenterId = ccCallCenterId;
    this.ccRecStartDateId = ccRecStartDateId;
    this.ccRecEndDateId = ccRecEndDateId;
    this.ccClosedDateId = ccClosedDateId;
    this.ccOpenDateId = ccOpenDateId;
    this.ccName = ccName;
    this.ccClass = ccClass;
    this.ccEmployees = ccEmployees;
    this.ccSqFt = ccSqFt;
    this.ccHours = ccHours;
    this.ccManager = ccManager;
    this.ccMarketId = ccMarketId;
    this.ccMarketClass = ccMarketClass;
    this.ccMarketDesc = ccMarketDesc;
    this.ccMarketManager = ccMarketManager;
    this.ccDivisionId = ccDivisionId;
    this.ccDivisionName = ccDivisionName;
    this.ccCompany = ccCompany;
    this.ccCompanyName = ccCompanyName;
    this.ccAddress = ccAddress;
    this.ccTaxPercentage = ccTaxPercentage;
  }

  public long getCcCallCenterSk()
  {
    return ccCallCenterSk;
  }

  public String getCcCallCenterId()
  {
    return ccCallCenterId;
  }

  public long getCcRecStartDateId()
  {
    return ccRecStartDateId;
  }

  public long getCcRecEndDateId()
  {
    return ccRecEndDateId;
  }

  public long getCcClosedDateId()
  {
    return ccClosedDateId;
  }

  public long getCcOpenDateId()
  {
    return ccOpenDateId;
  }

  public String getCcName()
  {
    return ccName;
  }

  public String getCcClass()
  {
    return ccClass;
  }

  public int getCcEmployees()
  {
    return ccEmployees;
  }

  public int getCcSqFt()
  {
    return ccSqFt;
  }

  public String getCcHours()
  {
    return ccHours;
  }

  public String getCcManager()
  {
    return ccManager;
  }

  public int getCcMarketId()
  {
    return ccMarketId;
  }

  public String getCcMarketClass()
  {
    return ccMarketClass;
  }

  public String getCcMarketDesc()
  {
    return ccMarketDesc;
  }

  public String getCcMarketManager()
  {
    return ccMarketManager;
  }

  public int getCcDivisionId()
  {
    return ccDivisionId;
  }

  public String getCcDivisionName()
  {
    return ccDivisionName;
  }

  public int getCcCompany()
  {
    return ccCompany;
  }

  public String getCcCompanyName()
  {
    return ccCompanyName;
  }

  public Address getCcAddress()
  {
    return ccAddress;
  }

  public Decimal getCcTaxPercentage()
  {
    return ccTaxPercentage;
  }


  public static class Builder
  {
    private long ccCallCenterSk;
    private String ccCallCenterId;
    private long ccRecStartDateId;
    private long ccRecEndDateId;
    private long ccClosedDateId;
    private long ccOpenDateId;
    private String ccName;
    private String ccClass;
    private int ccEmployees;
    private int ccSqFt;
    private String ccHours;
    private String ccManager;
    private int ccMarketId;
    private String ccMarketClass;
    private String ccMarketDesc;
    private String ccMarketManager;
    private int ccDivisionId;
    private String ccDivisionName;
    private int ccCompany;
    private String ccCompanyName;
    private Address ccAddress;
    private Decimal ccTaxPercentage;
    private long nullBitMap;

    public Builder setCcCallCenterSk(long ccCallCenterSk)
    {
      this.ccCallCenterSk = ccCallCenterSk;
      return this;
    }

    public Builder setCcCallCenterId(String ccCallCenterId)
    {
      this.ccCallCenterId = ccCallCenterId;
      return this;
    }

    public Builder setCcRecStartDateId(long ccRecStartDateId)
    {
      this.ccRecStartDateId = ccRecStartDateId;
      return this;
    }

    public Builder setCcRecEndDateId(long ccRecEndDateId)
    {
      this.ccRecEndDateId = ccRecEndDateId;
      return this;
    }

    public Builder setCcClosedDateId(long ccClosedDateId)
    {
      this.ccClosedDateId = ccClosedDateId;
      return this;
    }

    public Builder setCcOpenDateId(long ccOpenDateId)
    {
      this.ccOpenDateId = ccOpenDateId;
      return this;
    }

    public Builder setCcName(String ccName)
    {
      this.ccName = ccName;
      return this;
    }

    public Builder setCcClass(String ccClass)
    {
      this.ccClass = ccClass;
      return this;
    }

    public Builder setCcEmployees(int ccEmployees)
    {
      this.ccEmployees = ccEmployees;
      return this;
    }

    public Builder setCcSqFt(int ccSqFt)
    {
      this.ccSqFt = ccSqFt;
      return this;
    }

    public Builder setCcHours(String ccHours)
    {
      this.ccHours = ccHours;
      return this;
    }

    public Builder setCcManager(String ccManager)
    {
      this.ccManager = ccManager;
      return this;
    }

    public Builder setCcMarketId(int ccMarketId)
    {
      this.ccMarketId = ccMarketId;
      return this;
    }

    public Builder setCcMarketClass(String ccMarketClass)
    {
      this.ccMarketClass = ccMarketClass;
      return this;
    }

    public Builder setCcMarketDesc(String ccMarketDesc)
    {
      this.ccMarketDesc = ccMarketDesc;
      return this;
    }

    public Builder setCcMarketManager(String ccMarketManager)
    {
      this.ccMarketManager = ccMarketManager;
      return this;
    }

    public Builder setCcDivisionId(int ccDivisionId)
    {
      this.ccDivisionId = ccDivisionId;
      return this;
    }

    public Builder setCcDivisionName(String ccDivisionName)
    {
      this.ccDivisionName = ccDivisionName;
      return this;
    }

    public Builder setCcCompany(int ccCompany)
    {
      this.ccCompany = ccCompany;
      return this;
    }

    public Builder setCcCompanyName(String ccCompanyName)
    {
      this.ccCompanyName = ccCompanyName;
      return this;
    }

    public Builder setCcAddress(Address ccAddress)
    {
      this.ccAddress = ccAddress;
      return this;
    }

    public Builder setCcTaxPercentage(Decimal ccTaxPercentage)
    {
      this.ccTaxPercentage = ccTaxPercentage;
      return this;
    }

    public CallCenterRow build()
    {
      return new CallCenterRow(ccCallCenterSk, ccCallCenterId, ccRecStartDateId, ccRecEndDateId, ccClosedDateId, ccOpenDateId, ccName, ccClass, ccEmployees, ccSqFt, ccHours, ccManager, ccMarketId, ccMarketClass, ccMarketDesc, ccMarketManager, ccDivisionId, ccDivisionName, ccCompany, ccCompanyName, ccAddress, ccTaxPercentage, nullBitMap);
    }

    public void setNullBitMap(long nullBitMap)
    {
      this.nullBitMap = nullBitMap;
    }
  }

  @Override
  public Object[] values() {
    return new Object[]{ getOrNullForKey(ccCallCenterSk, CC_CALL_CENTER_SK),
        getOrNull(ccCallCenterId, CC_CALL_CENTER_ID),
        getDateOrNullFromJulianDays(ccRecStartDateId, CC_REC_START_DATE_ID),
        getDateOrNullFromJulianDays(ccRecEndDateId, CC_REC_END_DATE_ID),
        getOrNullForKey(ccClosedDateId, CC_CLOSED_DATE_ID),
        getOrNullForKey(ccOpenDateId, CC_OPEN_DATE_ID),
        getOrNull(ccName, CC_NAME),
        getOrNull(ccClass, CC_CLASS),
        getOrNull(ccEmployees, CC_EMPLOYEES),
        getOrNull(ccSqFt, CC_SQ_FT),
        getOrNull(ccHours, CC_HOURS),
        getOrNull(ccManager, CC_MANAGER),
        getOrNull(ccMarketId, CC_MARKET_ID),
        getOrNull(ccMarketClass, CC_MARKET_CLASS),
        getOrNull(ccMarketDesc, CC_MARKET_DESC),
        getOrNull(ccMarketManager, CC_MARKET_MANAGER),
        getOrNull(ccDivisionId, CC_DIVISION),
        getOrNull(ccDivisionName, CC_DIVISION_NAME),
        getOrNull(ccCompany, CC_COMPANY),
        getOrNull(ccCompanyName, CC_COMPANY_NAME),
        getOrNull(ccAddress.getStreetNumber(), CC_STREET_NUMBER),
        getOrNull(ccAddress.getStreetName(), CC_STREET_NAME),
        getOrNull(ccAddress.getStreetType(), CC_STREET_TYPE),
        getOrNull(ccAddress.getSuiteNumber(), CC_SUITE_NUMBER),
        getOrNull(ccAddress.getCity(), CC_CITY),
        getOrNull(ccAddress.getCounty(), CC_ADDRESS),
        getOrNull(ccAddress.getState(), CC_STATE),
        getOrNull(java.lang.String.format("%05d", ccAddress.getZip()), CC_ZIP),
        getOrNull(ccAddress.getCountry(), CC_COUNTRY),
        getOrNull(ccAddress.getGmtOffset(), CC_GMT_OFFSET),
        getOrNull(ccTaxPercentage, CC_TAX_PERCENTAGE)
    };
  }
}
