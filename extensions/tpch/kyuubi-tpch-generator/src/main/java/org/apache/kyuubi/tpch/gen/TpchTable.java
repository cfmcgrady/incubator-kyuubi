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

package org.apache.kyuubi.tpch.gen;

import java.util.Locale;

/** The eight base tables, with the existing connector's names and SQL type families. */
public enum TpchTable {
  CUSTOMER(
      "customer",
      150000L,
      "c_custkey:IDENTIFIER:0;"
          + "c_name:VARCHAR:25;"
          + "c_address:VARCHAR:40;"
          + "c_nationkey:IDENTIFIER:0;"
          + "c_phone:VARCHAR:15;"
          + "c_acctbal:DOUBLE:0;"
          + "c_mktsegment:VARCHAR:10;"
          + "c_comment:VARCHAR:117"),
  ORDERS(
      "orders",
      1500000L,
      "o_orderkey:IDENTIFIER:0;"
          + "o_custkey:IDENTIFIER:0;"
          + "o_orderstatus:VARCHAR:1;"
          + "o_totalprice:DOUBLE:0;"
          + "o_orderdate:DATE:0;"
          + "o_orderpriority:VARCHAR:15;"
          + "o_clerk:VARCHAR:15;"
          + "o_shippriority:INTEGER:0;"
          + "o_comment:VARCHAR:79"),
  LINE_ITEM(
      "lineitem",
      1500000L,
      "l_orderkey:IDENTIFIER:0;"
          + "l_partkey:IDENTIFIER:0;"
          + "l_suppkey:IDENTIFIER:0;"
          + "l_linenumber:INTEGER:0;"
          + "l_quantity:DOUBLE:0;"
          + "l_extendedprice:DOUBLE:0;"
          + "l_discount:DOUBLE:0;"
          + "l_tax:DOUBLE:0;"
          + "l_returnflag:VARCHAR:1;"
          + "l_linestatus:VARCHAR:1;"
          + "l_shipdate:DATE:0;"
          + "l_commitdate:DATE:0;"
          + "l_receiptdate:DATE:0;"
          + "l_shipinstruct:VARCHAR:25;"
          + "l_shipmode:VARCHAR:10;"
          + "l_comment:VARCHAR:44"),
  PART(
      "part",
      200000L,
      "p_partkey:IDENTIFIER:0;"
          + "p_name:VARCHAR:55;"
          + "p_mfgr:VARCHAR:25;"
          + "p_brand:VARCHAR:10;"
          + "p_type:VARCHAR:25;"
          + "p_size:INTEGER:0;"
          + "p_container:VARCHAR:10;"
          + "p_retailprice:DOUBLE:0;"
          + "p_comment:VARCHAR:23"),
  PART_SUPPLIER(
      "partsupp",
      200000L,
      "ps_partkey:IDENTIFIER:0;"
          + "ps_suppkey:IDENTIFIER:0;"
          + "ps_availqty:INTEGER:0;"
          + "ps_supplycost:DOUBLE:0;"
          + "ps_comment:VARCHAR:199"),
  SUPPLIER(
      "supplier",
      10000L,
      "s_suppkey:IDENTIFIER:0;"
          + "s_name:VARCHAR:25;"
          + "s_address:VARCHAR:40;"
          + "s_nationkey:IDENTIFIER:0;"
          + "s_phone:VARCHAR:15;"
          + "s_acctbal:DOUBLE:0;"
          + "s_comment:VARCHAR:101"),
  NATION(
      "nation",
      25L,
      "n_nationkey:IDENTIFIER:0;"
          + "n_name:VARCHAR:25;"
          + "n_regionkey:IDENTIFIER:0;"
          + "n_comment:VARCHAR:152"),
  REGION(
      "region", 5L, "r_regionkey:IDENTIFIER:0;" + "r_name:VARCHAR:25;" + "r_comment:VARCHAR:152");

  public enum Kind {
    IDENTIFIER,
    INTEGER,
    DOUBLE,
    DATE,
    VARCHAR
  }

  private final String name;
  private final long baseRows;
  private final Column[] columns;

  TpchTable(String name, long baseRows, String schema) {
    this.name = name;
    this.baseRows = baseRows;
    String[] fields = schema.split(";");
    columns = new Column[fields.length];
    for (int i = 0; i < fields.length; i++) {
      String[] parts = fields[i].split(":");
      columns[i] = new Column(parts[0], Kind.valueOf(parts[1]), Integer.parseInt(parts[2]));
    }
  }

  public String getTableName() {
    return name;
  }

  public static TpchTable getTable(String name) {
    for (TpchTable table : values())
      if (table.name.equals(name.toLowerCase(Locale.ROOT))) return table;
    throw new IllegalArgumentException("Unknown TPC-H table: " + name);
  }

  public boolean isSmall() {
    return this == NATION || this == REGION;
  }

  public int columnCount() {
    return columns.length;
  }

  public Column column(int ordinal) {
    return columns[ordinal];
  }

  public Column[] getColumns() {
    return columns.clone();
  }

  /** Parent parts/orders for child tables; output rows for other tables. */
  public long parentRows(double scale) {
    validateScale(scale);
    if (scale == 0) return 0;
    if (isSmall()) return baseRows;
    if (scale < 1) return Math.max(1, (long) (scale * 1000) * baseRows / 1000);
    return (long) scale * baseRows;
  }

  public static void validateScale(double scale) {
    if (!Double.isFinite(scale)
        || scale < 0
        || scale > 100000
        || (scale > 0 && scale < 0.001)
        || (scale >= 1 && scale != Math.rint(scale)))
      throw new IllegalArgumentException("Unsupported TPC-H scale: " + scale);
  }

  public static final class Column {
    private final String name;
    private final Kind kind;
    private final int length;

    private Column(String name, Kind kind, int length) {
      this.name = name;
      this.kind = kind;
      this.length = length;
    }

    public String getName() {
      return name;
    }

    public Kind getKind() {
      return kind;
    }

    public int getLength() {
      return length;
    }
  }
}
