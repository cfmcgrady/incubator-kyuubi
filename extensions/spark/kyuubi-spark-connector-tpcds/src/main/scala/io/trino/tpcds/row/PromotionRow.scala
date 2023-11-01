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

import io.trino.tpcds.generator.PromotionGeneratorColumn._
import io.trino.tpcds.`type`.Decimal
import java.util.{List => JList}

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow

class PromotionRow(

                    nullBitMap: Long,
                    private val pPromoSk: Long,
                    private val pPromoId: String,
                    private val pStartDateId: Long,
                    private val pEndDateId: Long,
                    private val pItemSk: Long,
                    private val pCost: Decimal,
                    private val pResponseTarget: Int,
                    private val pPromoName: String,
                    private val pChannelDmail: Boolean,
                    private val pChannelEmail: Boolean,
                    private val pChannelCatalog: Boolean,
                    private val pChannelTv: Boolean,
                    private val pChannelRadio: Boolean,
                    private val pChannelPress: Boolean,
                    private val pChannelEvent: Boolean,
                    private val pChannelDemo: Boolean,
                    private val pChannelDetails: String,
                    private val pPurpose: String,
                    private val pDiscountActive: Boolean
  ) extends TableRowWithNulls(nullBitMap, P_PROMO_SK)
  with KyuubiTableRowWithNulls {

    override val nullBitMapInternal = nullBitMap
    override val firstColumnInternal = P_PROMO_SK

  def getpPromoSk: Long = pPromoSk
  def getpPromoId: String = pPromoId
  def getpStartDateId: Long = pStartDateId
  def getpEndDateId: Long = pEndDateId
  def getpItemSk: Long = pItemSk
  def getpCost: Decimal = pCost
  def getpResponseTarget: Int = pResponseTarget
  def getpPromoName: String = pPromoName
  def ispChannelDmail: Boolean = pChannelDmail
  def ispChannelEmail: Boolean = pChannelEmail
  def ispChannelCatalog: Boolean = pChannelCatalog
  def ispChannelTv: Boolean = pChannelTv
  def ispChannelRadio: Boolean = pChannelRadio
  def ispChannelPress: Boolean = pChannelPress
  def ispChannelEvent: Boolean = pChannelEvent
  def ispChannelDemo: Boolean = pChannelDemo
  def getpChannelDetails: String = pChannelDetails
  def getpPurpose: String = pPurpose
  def ispDiscountActive: Boolean = pDiscountActive

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
    getLongOrNull(pPromoSk, P_PROMO_SK),
    getStringOrNullInternal(pPromoId, P_PROMO_ID),
    getLongOrNull(pStartDateId, P_START_DATE_ID),
    getLongOrNull(pEndDateId, P_END_DATE_ID),
    getLongOrNull(pItemSk, P_ITEM_SK),
    getDecimalOrNull(pCost, P_COST, 5, 2),
    getIntOrNull(pResponseTarget, P_RESPONSE_TARGET),
    getStringOrNullInternal(pPromoName, P_PROMO_NAME),
    getStringOrNullInternal(pChannelDmail, P_CHANNEL_DMAIL),
    getStringOrNullInternal(pChannelEmail, P_CHANNEL_EMAIL),
    getStringOrNullInternal(pChannelCatalog, P_CHANNEL_CATALOG),
    getStringOrNullInternal(pChannelTv, P_CHANNEL_TV),
    getStringOrNullInternal(pChannelRadio, P_CHANNEL_RADIO),
    getStringOrNullInternal(pChannelPress, P_CHANNEL_PRESS),
    getStringOrNullInternal(pChannelEvent, P_CHANNEL_EVENT),
    getStringOrNullInternal(pChannelDemo, P_CHANNEL_DEMO),
    getStringOrNullInternal(pChannelDetails, P_CHANNEL_DETAILS),
    getStringOrNullInternal(pPurpose, P_PURPOSE),
    getStringOrNullInternal(pDiscountActive, P_DISCOUNT_ACTIVE)))
}
