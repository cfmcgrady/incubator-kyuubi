/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tpcds.row;

import io.trino.tpcds.type.Decimal;
import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_BRAND;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_BRAND_ID;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_CATEGORY;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_CATEGORY_ID;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_CLASS;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_CLASS_ID;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_COLOR;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_CONTAINER;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_CURRENT_PRICE;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_FORMULATION;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_ITEM_DESC;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_ITEM_ID;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_ITEM_SK;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_MANAGER_ID;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_MANUFACT;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_MANUFACT_ID;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_PRODUCT_NAME;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_REC_END_DATE_ID;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_REC_START_DATE_ID;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_SIZE;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_UNITS;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_WHOLESALE_COST;

public class ItemRow extends KyuubiTPCDSTableRowWithNulls {
  private final long iItemSk;
  private final String iItemId;
  private final long iRecStartDateId;
  private final long iRecEndDateId;
  private final String iItemDesc;
  private final Decimal iCurrentPrice;    // list price
  private final Decimal iWholesaleCost;
  private final long iBrandId;
  private final String iBrand;
  private final long iClassId;
  private final String iClass;
  private final long iCategoryId;
  private final String iCategory;
  private final long iManufactId;
  private final String iManufact;
  private final String iSize;
  private final String iFormulation;
  private final String iColor;
  private final String iUnits;
  private final String iContainer;
  private final long iManagerId;
  private final String iProductName;
  private final long iPromoSk;

  public ItemRow(long nullBitMap, long iItemSk, String iItemId, long iRecStartDateId,
      long iRecEndDateId, String iItemDesc, Decimal iCurrentPrice, Decimal iWholesaleCost,
      long iBrandId, String iBrand, long iClassId, String iClass, long iCategoryId,
      String iCategory, long iManufactId, String iManufact, String iSize, String iFormulation,
      String iColor, String iUnits, String iContainer, long iManagerId, String iProductName,
      long iPromoSk) {
    super(nullBitMap, I_ITEM_SK);
    this.iItemSk = iItemSk;
    this.iItemId = iItemId;
    this.iRecStartDateId = iRecStartDateId;
    this.iRecEndDateId = iRecEndDateId;
    this.iItemDesc = iItemDesc;
    this.iCurrentPrice = iCurrentPrice;
    this.iWholesaleCost = iWholesaleCost;
    this.iBrandId = iBrandId;
    this.iBrand = iBrand;
    this.iClassId = iClassId;
    this.iClass = iClass;
    this.iCategoryId = iCategoryId;
    this.iCategory = iCategory;
    this.iManufactId = iManufactId;
    this.iManufact = iManufact;
    this.iSize = iSize;
    this.iFormulation = iFormulation;
    this.iColor = iColor;
    this.iUnits = iUnits;
    this.iContainer = iContainer;
    this.iManagerId = iManagerId;
    this.iProductName = iProductName;
    this.iPromoSk = iPromoSk;
  }

  public String getiItemDesc() {
    return iItemDesc;
  }

  public Decimal getiCurrentPrice() {
    return iCurrentPrice;
  }

  public Decimal getiWholesaleCost() {
    return iWholesaleCost;
  }

  public long getiBrandId() {
    return iBrandId;
  }

  public long getiClassId() {
    return iClassId;
  }

  public long getiManufactId() {
    return iManufactId;
  }

  public String getiManufact() {
    return iManufact;
  }

  public String getiSize() {
    return iSize;
  }

  public String getiFormulation() {
    return iFormulation;
  }

  public String getiColor() {
    return iColor;
  }

  public String getiUnits() {
    return iUnits;
  }

  public long getiItemSk() {
    return iItemSk;
  }

  public String getiItemId() {
    return iItemId;
  }

  public long getiRecStartDateId() {
    return iRecStartDateId;
  }

  public long getiRecEndDateId() {
    return iRecEndDateId;
  }

  public String getiBrand() {
    return iBrand;
  }

  public String getiClass() {
    return iClass;
  }

  public long getiCategoryId() {
    return iCategoryId;
  }

  public String getiCategory() {
    return iCategory;
  }

  public String getiContainer() {
    return iContainer;
  }

  public long getiManagerId() {
    return iManagerId;
  }

  public String getiProductName() {
    return iProductName;
  }

  public long getiPromoSk() {
    return iPromoSk;
  }

  @Override public Object[] values() {
    return new Object[] {
        getOrNullForKey(iItemSk, I_ITEM_SK),
        getOrNull(iItemId, I_ITEM_ID),
        getDateOrNullFromJulianDays(iRecStartDateId, I_REC_START_DATE_ID),
        getDateOrNullFromJulianDays(iRecEndDateId, I_REC_END_DATE_ID),
        getOrNull(iItemDesc, I_ITEM_DESC),
        getOrNull(iCurrentPrice, I_CURRENT_PRICE),
        getOrNull(iWholesaleCost, I_WHOLESALE_COST),
        getOrNullForKey(iBrandId, I_BRAND_ID),
        getOrNull(iBrand, I_BRAND),
        getOrNullForKey(iClassId, I_CLASS_ID),
        getOrNull(iClass, I_CLASS),
        getOrNullForKey(iCategoryId, I_CATEGORY_ID),
        getOrNull(iCategory, I_CATEGORY),
        getOrNullForKey(iManufactId, I_MANUFACT_ID),
        getOrNull(iManufact, I_MANUFACT),
        getOrNull(iSize, I_SIZE),
        getOrNull(iFormulation, I_FORMULATION),
        getOrNull(iColor, I_COLOR),
        getOrNull(iUnits, I_UNITS),
        getOrNull(iContainer, I_CONTAINER),
        getOrNullForKey(iManagerId, I_MANAGER_ID),
        getOrNull(iProductName, I_PRODUCT_NAME)
    };
  }
}
