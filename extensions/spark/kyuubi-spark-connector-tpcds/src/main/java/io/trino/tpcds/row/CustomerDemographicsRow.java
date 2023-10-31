package io.trino.tpcds.row;

import static io.trino.tpcds.generator.CustomerDemographicsGeneratorColumn.CD_CREDIT_RATING;
import static io.trino.tpcds.generator.CustomerDemographicsGeneratorColumn.CD_DEMO_SK;
import static io.trino.tpcds.generator.CustomerDemographicsGeneratorColumn.CD_DEP_COLLEGE_COUNT;
import static io.trino.tpcds.generator.CustomerDemographicsGeneratorColumn.CD_DEP_COUNT;
import static io.trino.tpcds.generator.CustomerDemographicsGeneratorColumn.CD_DEP_EMPLOYED_COUNT;
import static io.trino.tpcds.generator.CustomerDemographicsGeneratorColumn.CD_EDUCATION_STATUS;
import static io.trino.tpcds.generator.CustomerDemographicsGeneratorColumn.CD_GENDER;
import static io.trino.tpcds.generator.CustomerDemographicsGeneratorColumn.CD_MARITAL_STATUS;
import static io.trino.tpcds.generator.CustomerDemographicsGeneratorColumn.CD_PURCHASE_ESTIMATE;

import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

public class CustomerDemographicsRow extends KyuubiTPCDSTableRowWithNulls {
  private final long cdDemoSk;
  private final String cdGender;
  private final String cdMaritalStatus;
  private final String cdEducationStatus;
  private final int cdPurchaseEstimate;
  private final String cdCreditRating;
  private final int cdDepCount;
  private final int cdDepEmployedCount;
  private final int cdDepCollegeCount;

  public CustomerDemographicsRow(
      long nullBitMap,
      long cdDemoSk,
      String cdGender,
      String cdMaritalStatus,
      String cdEducationStatus,
      int cdPurchaseEstimate,
      String cdCreditRating,
      int cdDepCount,
      int cdDepEmployedCount,
      int cdDepCollegeCount) {
    super(nullBitMap, CD_DEMO_SK);
    this.cdDemoSk = cdDemoSk;
    this.cdGender = cdGender;
    this.cdMaritalStatus = cdMaritalStatus;
    this.cdEducationStatus = cdEducationStatus;
    this.cdPurchaseEstimate = cdPurchaseEstimate;
    this.cdCreditRating = cdCreditRating;
    this.cdDepCount = cdDepCount;
    this.cdDepEmployedCount = cdDepEmployedCount;
    this.cdDepCollegeCount = cdDepCollegeCount;
  }

  public long getCdDemoSk() {
    return cdDemoSk;
  }

  public String getCdGender() {
    return cdGender;
  }

  public String getCdMaritalStatus() {
    return cdMaritalStatus;
  }

  public String getCdEducationStatus() {
    return cdEducationStatus;
  }

  public int getCdPurchaseEstimate() {
    return cdPurchaseEstimate;
  }

  public String getCdCreditRating() {
    return cdCreditRating;
  }

  public int getCdDepCount() {
    return cdDepCount;
  }

  public int getCdDepEmployedCount() {
    return cdDepEmployedCount;
  }

  public int getCdDepCollegeCount() {
    return cdDepCollegeCount;
  }

  @Override
  public Object[] values() {
    return new Object[] {
      getOrNullForKey(cdDemoSk, CD_DEMO_SK),
      getOrNull(cdGender, CD_GENDER),
      getOrNull(cdMaritalStatus, CD_MARITAL_STATUS),
      getOrNull(cdEducationStatus, CD_EDUCATION_STATUS),
      getOrNull(cdPurchaseEstimate, CD_PURCHASE_ESTIMATE),
      getOrNull(cdCreditRating, CD_CREDIT_RATING),
      getOrNull(cdDepCount, CD_DEP_COUNT),
      getOrNull(cdDepEmployedCount, CD_DEP_EMPLOYED_COUNT),
      getOrNull(cdDepCollegeCount, CD_DEP_COLLEGE_COUNT)
    };
  }
}
