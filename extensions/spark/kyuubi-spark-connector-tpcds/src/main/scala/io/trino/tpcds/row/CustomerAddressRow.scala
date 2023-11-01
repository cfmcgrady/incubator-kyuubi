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

import io.trino.tpcds.generator.CustomerAddressGeneratorColumn._
import io.trino.tpcds.`type`.Address
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import java.util.{List => JList}

class CustomerAddressRow(
    nullBitMap: Long,
    private val caAddrSk: Long,
    private val caAddrId: String,
    private val caAddress: Address,
    private val caLocationType: String) extends TableRowWithNulls(nullBitMap, CA_ADDRESS_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = CA_ADDRESS_SK

  def getCaAddrSk: Long = caAddrSk
  def getCaAddrId: String = caAddrId
  def getCaAddress: Address = caAddress
  def getCaLocationType: String = caLocationType

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(caAddrSk, CA_ADDRESS_SK),
        getStringOrNullInternal(caAddrId, CA_ADDRESS_ID),
        getStringOrNullInternal(caAddress.getStreetNumber.toString, CA_ADDRESS_STREET_NUM),
        getStringOrNullInternal(caAddress.getStreetName, CA_ADDRESS_STREET_NAME),
        getStringOrNullInternal(caAddress.getStreetType, CA_ADDRESS_STREET_TYPE),
        getStringOrNullInternal(caAddress.getSuiteNumber, CA_ADDRESS_SUITE_NUM),
        getStringOrNullInternal(caAddress.getCity, CA_ADDRESS_CITY),
        getStringOrNullInternal(caAddress.getCounty, CA_ADDRESS_COUNTY),
        getStringOrNullInternal(caAddress.getState, CA_ADDRESS_STATE),
        getStringOrNullInternal(f"${caAddress.getZip}%05d", CA_ADDRESS_ZIP),
        getStringOrNullInternal(caAddress.getCountry, CA_ADDRESS_COUNTRY),
        getDecimalOrNull(caAddress.getGmtOffset, CA_ADDRESS_GMT_OFFSET, 5, 2),
        getStringOrNullInternal(caLocationType, CA_LOCATION_TYPE)))
}
