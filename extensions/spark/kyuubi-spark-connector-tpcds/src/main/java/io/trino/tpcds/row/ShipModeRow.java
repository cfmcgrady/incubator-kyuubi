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

import java.util.List;

import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

import static com.google.common.collect.Lists.newArrayList;
import static io.trino.tpcds.generator.ShipModeGeneratorColumn.SM_CARRIER;
import static io.trino.tpcds.generator.ShipModeGeneratorColumn.SM_CODE;
import static io.trino.tpcds.generator.ShipModeGeneratorColumn.SM_CONTRACT;
import static io.trino.tpcds.generator.ShipModeGeneratorColumn.SM_SHIP_MODE_ID;
import static io.trino.tpcds.generator.ShipModeGeneratorColumn.SM_SHIP_MODE_SK;
import static io.trino.tpcds.generator.ShipModeGeneratorColumn.SM_TYPE;

public class ShipModeRow
    extends KyuubiTPCDSTableRowWithNulls
{
  private final long smShipModeSk;
  private final String smShipModeId;
  private final String smType;
  private final String smCode;
  private final String smCarrier;
  private final String smContract;

  public ShipModeRow(long nullBitMap, long smShipModeSk, String smShipModeId, String smType, String smCode, String smCarrier, String smContract)
  {
    super(nullBitMap, SM_SHIP_MODE_SK);
    this.smShipModeSk = smShipModeSk;
    this.smShipModeId = smShipModeId;
    this.smType = smType;
    this.smCode = smCode;
    this.smCarrier = smCarrier;
    this.smContract = smContract;
  }

  public long getSmShipModeSk() {
    return smShipModeSk;
  }

  public String getSmShipModeId() {
    return smShipModeId;
  }

  public String getSmType() {
    return smType;
  }

  public String getSmCode() {
    return smCode;
  }

  public String getSmCarrier() {
    return smCarrier;
  }

  public String getSmContract() {
    return smContract;
  }

  @Override public Object[] values() {
    return new Object[] {
        getOrNullForKey(smShipModeSk, SM_SHIP_MODE_SK),
        getOrNull(smShipModeId, SM_SHIP_MODE_ID),
        getOrNull(smType, SM_TYPE),
        getOrNull(smCode, SM_CODE),
        getOrNull(smCarrier, SM_CARRIER),
        getOrNull(smContract, SM_CONTRACT)
    };
  }
}
