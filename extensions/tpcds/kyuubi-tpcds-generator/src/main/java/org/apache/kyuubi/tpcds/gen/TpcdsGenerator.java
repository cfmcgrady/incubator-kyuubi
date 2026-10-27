/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.tpcds.gen;

/** Entry point shared by the Spark connector and the standalone data writer. */
public final class TpcdsGenerator {
  public static RowCursor partition(TpcdsTable table, double scale, int partitions, int partition) {
    return range(table, scale, RowRange.split(new Scale(scale).rows(table), partitions, partition));
  }

  public static RowCursor range(TpcdsTable table, double scale, RowRange range) {
    switch (table) {
      case STORE_SALES:
      case STORE_RETURNS:
        return new StoreChannelGenerator(table == TpcdsTable.STORE_RETURNS, scale, range);
      case CATALOG_SALES:
      case CATALOG_RETURNS:
      case WEB_SALES:
      case WEB_RETURNS:
        return new RemoteChannelGenerator(table, scale, range);
      default:
        return new DimensionGenerator(table, scale, range);
    }
  }

  private TpcdsGenerator() {}
}
