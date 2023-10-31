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

import static io.trino.tpcds.generator.DbgenVersionGeneratorColumn.DV_CMDLINE_ARGS;
import static io.trino.tpcds.generator.DbgenVersionGeneratorColumn.DV_CREATE_DATE;
import static io.trino.tpcds.generator.DbgenVersionGeneratorColumn.DV_CREATE_TIME;
import static io.trino.tpcds.generator.DbgenVersionGeneratorColumn.DV_VERSION;

import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

public class DbgenVersionRow extends KyuubiTPCDSTableRowWithNulls {
  private final String dvVersion;
  private final String dvCreateDate;
  private final String dvCreateTime;
  private final String dvCmdlineArgs;

  public DbgenVersionRow(
      long nullBitMap,
      String dvVersion,
      String dvCreateDate,
      String dvCreateTime,
      String dvCmdlineArgs) {
    super(nullBitMap, DV_VERSION);
    this.dvVersion = dvVersion;
    this.dvCreateDate = dvCreateDate;
    this.dvCreateTime = dvCreateTime;
    this.dvCmdlineArgs = dvCmdlineArgs;
  }

  public String getDvVersion() {
    return dvVersion;
  }

  public String getDvCreateDate() {
    return dvCreateDate;
  }

  public String getDvCreateTime() {
    return dvCreateTime;
  }

  public String getDvCmdlineArgs() {
    return dvCmdlineArgs;
  }

  @Override
  public Object[] values() {
    return new Object[] {
      getOrNull(dvVersion, DV_VERSION),
      getOrNull(dvCreateDate, DV_CREATE_DATE),
      getOrNull(dvCreateTime, DV_CREATE_TIME),
      getOrNull(dvCmdlineArgs, DV_CMDLINE_ARGS)
    };
  }
}
