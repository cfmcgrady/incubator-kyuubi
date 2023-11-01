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

import io.trino.tpcds.generator.CatalogPageGeneratorColumn._
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow

import java.util.{List => JList}

class CatalogPageRow(
   private val cpCatalogPageSk: Long,
   private val cpCatalogPageId: String,
   private val cpStartDateId: Long,
   private val cpEndDateId: Long,
   private val cpDepartment: String,
   private val cpCatalogNumber: Int,
   private val cpCatalogPageNumber: Int,
   private val cpDescription: String,
   private val cpType: String,
   nullBitMap: Long) extends TableRowWithNulls(nullBitMap, CP_CATALOG_PAGE_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = CP_CATALOG_PAGE_SK

  def getCpCatalogPageSk: Long = cpCatalogPageSk
  def getCpCatalogPageId: String = cpCatalogPageId
  def getCpStartDateId: Long = cpStartDateId
  def getCpEndDateId: Long = cpEndDateId
  def getCpDepartment: String = cpDepartment
  def getCpCatalogNumber: Int = cpCatalogNumber
  def getCpCatalogPageNumber: Int = cpCatalogPageNumber
  def getCpDescription: String = cpDescription
  def getCpType: String = cpType

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(cpCatalogPageSk, CP_CATALOG_PAGE_SK),
        getStringOrNullInternal(cpCatalogPageId, CP_CATALOG_PAGE_ID),
        getIntOrNull(cpStartDateId, CP_START_DATE_ID),
        getIntOrNull(cpEndDateId, CP_END_DATE_ID),
        getStringOrNullInternal(cpDepartment, CP_DEPARTMENT),
        getIntOrNull(cpCatalogNumber, CP_CATALOG_NUMBER),
        getIntOrNull(cpCatalogPageNumber, CP_CATALOG_PAGE_NUMBER),
        getStringOrNullInternal(cpDescription, CP_DESCRIPTION),
        getStringOrNullInternal(cpType, CP_TYPE)))
}
