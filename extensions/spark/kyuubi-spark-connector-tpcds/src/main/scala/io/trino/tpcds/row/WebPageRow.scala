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

import io.trino.tpcds.generator.WebPageGeneratorColumn._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class WebPageRow(
    nullBitMap: Long,
    private val wpPageSk: Long,
    private val wpPageId: String,
    private val wpRecStartDateId: Long,
    private val wpRecEndDateId: Long,
    private val wpCreationDateSk: Long,
    private val wpAccessDateSk: Long,
    private val wpAutogenFlag: Boolean,
    private val wpCustomerSk: Long,
    private val wpUrl: String,
    private val wpType: String,
    private val wpCharCount: Int,
    private val wpLinkCount: Int,
    private val wpImageCount: Int,
    private val wpMaxAdCount: Int) extends TableRowWithNulls(nullBitMap, WP_PAGE_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = WP_PAGE_SK

  def getWpCreationDateSk: Long = wpCreationDateSk
  def getWpAccessDateSk: Long = wpAccessDateSk
  def isWpAutogenFlag: Boolean = wpAutogenFlag
  def getWpCustomerSk: Long = wpCustomerSk
  def getWpCharCount: Int = wpCharCount
  def getWpLinkCount: Int = wpLinkCount
  def getWpImageCount: Int = wpImageCount
  def getWpMaxAdCount: Int = wpMaxAdCount
  def getWpPageSk: Long = wpPageSk
  def getWpPageId: String = wpPageId
  def getWpRecStartDateId: Long = wpRecStartDateId
  def getWpRecEndDateId: Long = wpRecEndDateId
  def getWpUrl: String = wpUrl
  def getWpType: String = wpType

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(wpPageSk, WP_PAGE_SK),
        getStringOrNullInternal(wpPageId, WP_PAGE_ID),
        getDateOrNullFromJulianDays(wpRecStartDateId, WP_REC_START_DATE_ID),
        getDateOrNullFromJulianDays(wpRecEndDateId, WP_REC_END_DATE_ID),
        getLongOrNull(wpCreationDateSk, WP_CREATION_DATE_SK),
        getLongOrNull(wpAccessDateSk, WP_ACCESS_DATE_SK),
        getStringOrNullInternal(wpAutogenFlag, WP_AUTOGEN_FLAG),
        getLongOrNull(wpCustomerSk, WP_CUSTOMER_SK),
        getStringOrNullInternal(wpUrl, WP_URL),
        getStringOrNullInternal(wpType, WP_TYPE),
        getIntOrNull(wpCharCount, WP_CHAR_COUNT),
        getIntOrNull(wpLinkCount, WP_LINK_COUNT),
        getIntOrNull(wpImageCount, WP_IMAGE_COUNT),
        getIntOrNull(wpMaxAdCount, WP_MAX_AD_COUNT)))
}
