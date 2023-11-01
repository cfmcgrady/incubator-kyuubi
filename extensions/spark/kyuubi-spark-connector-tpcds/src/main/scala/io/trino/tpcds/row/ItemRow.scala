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

package io.trino.tpcds.row

import java.util.{List => JList}

import io.trino.tpcds.`type`.Decimal
import io.trino.tpcds.generator.ItemGeneratorColumn._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class ItemRow(
    nullBitMap: Long,
    private val iItemSk: Long,
    private val iItemId: String,
    private val iRecStartDateId: Long,
    private val iRecEndDateId: Long,
    private val iItemDesc: String,
    private val iCurrentPrice: Decimal,
    private val iWholesaleCost: Decimal,
    private val iBrandId: Long,
    private val iBrand: String,
    private val iClassId: Long,
    private val iClass: String,
    private val iCategoryId: Long,
    private val iCategory: String,
    private val iManufactId: Long,
    private val iManufact: String,
    private val iSize: String,
    private val iFormulation: String,
    private val iColor: String,
    private val iUnits: String,
    private val iContainer: String,
    private val iManagerId: Long,
    private val iProductName: String,
    private val iPromoSk: Long) extends TableRowWithNulls(nullBitMap, I_ITEM_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = I_ITEM_SK

  def getiItemDesc: String = iItemDesc
  def getiCurrentPrice: Decimal = iCurrentPrice
  def getiWholesaleCost: Decimal = iWholesaleCost
  def getiBrandId: Long = iBrandId
  def getiClassId: Long = iClassId
  def getiManufactId: Long = iManufactId
  def getiManufact: String = iManufact
  def getiSize: String = iSize
  def getiFormulation: String = iFormulation
  def getiColor: String = iColor
  def getiUnits: String = iUnits
  def getiItemSk: Long = iItemSk
  def getiItemId: String = iItemId
  def getiRecStartDateId: Long = iRecStartDateId
  def getiRecEndDateId: Long = iRecEndDateId
  def getiBrand: String = iBrand
  def getiClass: String = iClass
  def getiCategoryId: Long = iCategoryId
  def getiCategory: String = iCategory
  def getiContainer: String = iContainer
  def getiManagerId: Long = iManagerId
  def getiProductName: String = iProductName
  def getiPromoSk: Long = iPromoSk

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(iItemSk, I_ITEM_SK),
        getStringOrNullInternal(iItemId, I_ITEM_ID),
        getDateOrNullFromJulianDays(iRecStartDateId, I_REC_START_DATE_ID),
        getDateOrNullFromJulianDays(iRecEndDateId, I_REC_END_DATE_ID),
        getStringOrNullInternal(iItemDesc, I_ITEM_DESC),
        getDecimalOrNull(iCurrentPrice, I_CURRENT_PRICE, 7, 2),
        getDecimalOrNull(iWholesaleCost, I_WHOLESALE_COST, 7, 2),
        getIntOrNull(iBrandId, I_BRAND_ID),
        getStringOrNullInternal(iBrand, I_BRAND),
        getIntOrNull(iClassId, I_CLASS_ID),
        getStringOrNullInternal(iClass, I_CLASS),
        getIntOrNull(iCategoryId, I_CATEGORY_ID),
        getStringOrNullInternal(iCategory, I_CATEGORY),
        getIntOrNull(iManufactId, I_MANUFACT_ID),
        getStringOrNullInternal(iManufact, I_MANUFACT),
        getStringOrNullInternal(iSize, I_SIZE),
        getStringOrNullInternal(iFormulation, I_FORMULATION),
        getStringOrNullInternal(iColor, I_COLOR),
        getStringOrNullInternal(iUnits, I_UNITS),
        getStringOrNullInternal(iContainer, I_CONTAINER),
        getIntOrNull(iManagerId, I_MANAGER_ID),
        getStringOrNullInternal(iProductName, I_PRODUCT_NAME)))
}
