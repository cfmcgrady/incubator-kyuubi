package io.trino.tpcds.row;

import static io.trino.tpcds.generator.CatalogPageGeneratorColumn.CP_CATALOG_NUMBER;
import static io.trino.tpcds.generator.CatalogPageGeneratorColumn.CP_CATALOG_PAGE_ID;
import static io.trino.tpcds.generator.CatalogPageGeneratorColumn.CP_CATALOG_PAGE_NUMBER;
import static io.trino.tpcds.generator.CatalogPageGeneratorColumn.CP_CATALOG_PAGE_SK;
import static io.trino.tpcds.generator.CatalogPageGeneratorColumn.CP_DEPARTMENT;
import static io.trino.tpcds.generator.CatalogPageGeneratorColumn.CP_DESCRIPTION;
import static io.trino.tpcds.generator.CatalogPageGeneratorColumn.CP_END_DATE_ID;
import static io.trino.tpcds.generator.CatalogPageGeneratorColumn.CP_START_DATE_ID;
import static io.trino.tpcds.generator.CatalogPageGeneratorColumn.CP_TYPE;

import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

public class CatalogPageRow extends KyuubiTPCDSTableRowWithNulls {
  private final long cpCatalogPageSk;
  private final String cpCatalogPageId;
  private final long cpStartDateId;
  private final long cpEndDateId;
  private final String cpDepartment;
  private final int cpCatalogNumber;
  private final int cpCatalogPageNumber;
  private final String cpDescription;
  private final String cpType;

  public CatalogPageRow(
      long cpCatalogPageSk,
      String cpCatalogPageId,
      long cpStartDateId,
      long cpEndDateId,
      String cpDepartment,
      int cpCatalogNumber,
      int cpCatalogPageNumber,
      String cpDescription,
      String cpType,
      long nullBitMap) {
    super(nullBitMap, CP_CATALOG_PAGE_SK);
    this.cpCatalogPageSk = cpCatalogPageSk;
    this.cpCatalogPageId = cpCatalogPageId;
    this.cpStartDateId = cpStartDateId;
    this.cpEndDateId = cpEndDateId;
    this.cpDepartment = cpDepartment;
    this.cpCatalogNumber = cpCatalogNumber;
    this.cpCatalogPageNumber = cpCatalogPageNumber;
    this.cpDescription = cpDescription;
    this.cpType = cpType;
  }

  public long getCpCatalogPageSk() {
    return cpCatalogPageSk;
  }

  public String getCpCatalogPageId() {
    return cpCatalogPageId;
  }

  public long getCpStartDateId() {
    return cpStartDateId;
  }

  public long getCpEndDateId() {
    return cpEndDateId;
  }

  public String getCpDepartment() {
    return cpDepartment;
  }

  public int getCpCatalogNumber() {
    return cpCatalogNumber;
  }

  public int getCpCatalogPageNumber() {
    return cpCatalogPageNumber;
  }

  public String getCpDescription() {
    return cpDescription;
  }

  public String getCpType() {
    return cpType;
  }

  @Override
  public Object[] values() {
    return new Object[] {
      getOrNullForKey(cpCatalogPageSk, CP_CATALOG_PAGE_SK),
      getOrNull(cpCatalogPageId, CP_CATALOG_PAGE_ID),
      getOrNullForKey(cpStartDateId, CP_START_DATE_ID),
      getOrNullForKey(cpEndDateId, CP_END_DATE_ID),
      getOrNull(cpDepartment, CP_DEPARTMENT),
      getOrNull(cpCatalogNumber, CP_CATALOG_NUMBER),
      getOrNull(cpCatalogPageNumber, CP_CATALOG_PAGE_NUMBER),
      getOrNull(cpDescription, CP_DESCRIPTION),
      getOrNull(cpType, CP_TYPE)
    };
  }
}
