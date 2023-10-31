package io.trino.tpcds.row;

import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_BIRTH_COUNTRY;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_BIRTH_DAY;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_BIRTH_MONTH;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_BIRTH_YEAR;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_CURRENT_ADDR_SK;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_CURRENT_CDEMO_SK;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_CURRENT_HDEMO_SK;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_CUSTOMER_ID;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_CUSTOMER_SK;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_EMAIL_ADDRESS;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_FIRST_NAME;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_FIRST_SALES_DATE_ID;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_FIRST_SHIPTO_DATE_ID;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_LAST_NAME;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_LAST_REVIEW_DATE;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_PREFERRED_CUST_FLAG;
import static io.trino.tpcds.generator.CustomerGeneratorColumn.C_SALUTATION;

import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

public class CustomerRow extends KyuubiTPCDSTableRowWithNulls {
  private final long cCustomerSk;
  private final String cCustomerId;
  private final long cCurrentCdemoSk;
  private final long cCurrentHdemoSk;
  private final long cCurrentAddrSk;
  private final int cFirstShiptoDateId;
  private final int cFirstSalesDateId;
  private final String cSalutation;
  private final String cFirstName;
  private final String cLastName;
  private final boolean cPreferredCustFlag;
  private final int cBirthDay;
  private final int cBirthMonth;
  private final int cBirthYear;
  private final String cBirthCountry;
  private final String cLogin;
  private final String cEmailAddress;
  private final int cLastReviewDate;

  public CustomerRow(
      long cCustomerSk,
      String cCustomerId,
      long cCurrentCdemoSk,
      long cCurrentHdemoSk,
      long cCurrentAddrSk,
      int cFirstShiptoDateId,
      int cFirstSalesDateId,
      String cSalutation,
      String cFirstName,
      String cLastName,
      boolean cPreferredCustFlag,
      int cBirthDay,
      int cBirthMonth,
      int cBirthYear,
      String cBirthCountry,
      String cEmailAddress,
      int cLastReviewDate,
      long nullBitMap) {
    super(nullBitMap, C_CUSTOMER_SK);
    this.cCustomerSk = cCustomerSk;
    this.cCustomerId = cCustomerId;
    this.cCurrentCdemoSk = cCurrentCdemoSk;
    this.cCurrentHdemoSk = cCurrentHdemoSk;
    this.cCurrentAddrSk = cCurrentAddrSk;
    this.cFirstShiptoDateId = cFirstShiptoDateId;
    this.cFirstSalesDateId = cFirstSalesDateId;
    this.cSalutation = cSalutation;
    this.cFirstName = cFirstName;
    this.cLastName = cLastName;
    this.cPreferredCustFlag = cPreferredCustFlag;
    this.cBirthDay = cBirthDay;
    this.cBirthMonth = cBirthMonth;
    this.cBirthYear = cBirthYear;
    this.cBirthCountry = cBirthCountry;
    this.cLogin = null; // never gets set to anything
    this.cEmailAddress = cEmailAddress;
    this.cLastReviewDate = cLastReviewDate;
  }

  public long getcCustomerSk() {
    return cCustomerSk;
  }

  public String getcCustomerId() {
    return cCustomerId;
  }

  public long getcCurrentCdemoSk() {
    return cCurrentCdemoSk;
  }

  public long getcCurrentHdemoSk() {
    return cCurrentHdemoSk;
  }

  public long getcCurrentAddrSk() {
    return cCurrentAddrSk;
  }

  public int getcFirstShiptoDateId() {
    return cFirstShiptoDateId;
  }

  public int getcFirstSalesDateId() {
    return cFirstSalesDateId;
  }

  public String getcSalutation() {
    return cSalutation;
  }

  public String getcFirstName() {
    return cFirstName;
  }

  public String getcLastName() {
    return cLastName;
  }

  public boolean iscPreferredCustFlag() {
    return cPreferredCustFlag;
  }

  public int getcBirthDay() {
    return cBirthDay;
  }

  public int getcBirthMonth() {
    return cBirthMonth;
  }

  public int getcBirthYear() {
    return cBirthYear;
  }

  public String getcBirthCountry() {
    return cBirthCountry;
  }

  public String getcLogin() {
    return cLogin;
  }

  public String getcEmailAddress() {
    return cEmailAddress;
  }

  public int getcLastReviewDate() {
    return cLastReviewDate;
  }

  @Override
  public Object[] values() {
    return new Object[] {
      getOrNullForKey(cCustomerSk, C_CUSTOMER_SK),
      getOrNull(cCustomerId, C_CUSTOMER_ID),
      getOrNullForKey(cCurrentCdemoSk, C_CURRENT_CDEMO_SK),
      getOrNullForKey(cCurrentHdemoSk, C_CURRENT_HDEMO_SK),
      getOrNullForKey(cCurrentAddrSk, C_CURRENT_ADDR_SK),
      getOrNull(cFirstShiptoDateId, C_FIRST_SHIPTO_DATE_ID),
      getOrNull(cFirstSalesDateId, C_FIRST_SALES_DATE_ID),
      getOrNull(cSalutation, C_SALUTATION),
      getOrNull(cFirstName, C_FIRST_NAME),
      getOrNull(cLastName, C_LAST_NAME),
      getOrNullForBoolean(cPreferredCustFlag, C_PREFERRED_CUST_FLAG),
      getOrNull(cBirthDay, C_BIRTH_DAY),
      getOrNull(cBirthMonth, C_BIRTH_MONTH),
      getOrNull(cBirthYear, C_BIRTH_YEAR),
      getOrNull(cBirthCountry, C_BIRTH_COUNTRY),
      cLogin,
      getOrNull(cEmailAddress, C_EMAIL_ADDRESS),
      getOrNull(cLastReviewDate, C_LAST_REVIEW_DATE)
    };
  }
}
