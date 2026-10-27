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

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class TpchGeneratorTest {
  @TestFactory
  List<DynamicTest> allTablesAndColumnsMatchNative() throws Exception {
    List<DynamicTest> result = new ArrayList<>();
    EnumSet<TpchTable> tables = EnumSet.noneOf(TpchTable.class);
    try (BufferedReader input = reader(resource("manifest.tsv"))) {
      String line;
      while ((line = input.readLine()) != null) {
        if (line.startsWith("#") || line.isEmpty()) continue;
        String[] fields = line.split("\t");
        tables.add(TpchTable.getTable(fields[0]));
        result.add(DynamicTest.dynamicTest(fields[6], () -> verify(fields)));
      }
    }
    assertEquals(EnumSet.allOf(TpchTable.class), tables);
    return result;
  }

  private void verify(String[] fields) throws Exception {
    TpchTable table = TpchTable.getTable(fields[0]);
    RowCursor actual =
        TpchGenerator.range(
            table,
            Double.parseDouble(fields[1]),
            new RowRange(Long.parseLong(fields[2]), Long.parseLong(fields[3])));
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    long count = 0;
    try (BufferedReader input =
        reader(new DigestInputStream(new GZIPInputStream(resource(fields[6])), digest))) {
      String expected;
      while ((expected = input.readLine()) != null) {
        assertTrue(actual.next(), "Early end at row " + count);
        assertEquals(expected, line(actual), "Row " + count++);
        String[] columns = expected.split("\\|", -1);
        for (int i = 0; i < table.columnCount(); i++) {
          if (table.column(i).getKind() == TpchTable.Kind.VARCHAR) {
            assertEquals(
                columns[i],
                new String(
                    actual.getBytes(i),
                    actual.getByteOffset(i),
                    actual.getByteLength(i),
                    StandardCharsets.UTF_8),
                "UTF-8 column " + i);
          }
        }
      }
    }
    assertEquals(Long.parseLong(fields[4]), count);
    assertEquals(fields[5], HexFormat.of().formatHex(digest.digest()), "Fixture checksum");
    assertFalse(actual.next(), "Extra output rows");
    assertFalse(actual.next(), "Remains exhausted");
  }

  @Test
  void allPartitionsTogetherMatchFullNativeTiny() throws Exception {
    for (TpchTable table : TpchTable.values()) {
      try (BufferedReader input =
          reader(new GZIPInputStream(resource(table.getTableName() + "-tiny-full.tbl.gz")))) {
        for (int partition = 1; partition <= 7; partition++) {
          RowCursor rows = TpchGenerator.partition(table, 0.01, 7, partition);
          while (rows.next()) assertEquals(input.readLine(), line(rows), table.getTableName());
        }
        assertNull(input.readLine(), table.getTableName());
      }
    }
  }

  @Test
  void emptyAndInvalidRanges() {
    for (TpchTable table : TpchTable.values()) {
      RowCursor empty = TpchGenerator.partition(table, 0, 2, 2);
      assertFalse(empty.next());
      assertThrows(IllegalStateException.class, () -> empty.getLong(0));
      assertThrows(IllegalStateException.class, () -> empty.getBytes(1));
      assertThrows(IllegalStateException.class, () -> empty.getByteOffset(1));
      assertThrows(IllegalStateException.class, () -> empty.getByteLength(1));
      long total = table.parentRows(1);
      assertThrows(
          IllegalArgumentException.class,
          () -> TpchGenerator.range(table, 1, new RowRange(total, total + 1)));
      assertThrows(IllegalArgumentException.class, () -> TpchGenerator.partition(table, 1, 0, 1));
      assertThrows(IllegalArgumentException.class, () -> TpchGenerator.partition(table, 1, 2, 3));
      RowCursor beyond = TpchGenerator.range(table, 1, new RowRange(total, total));
      assertFalse(beyond.next());
    }
    for (double scale :
        new double[] {-1, Double.NaN, Double.POSITIVE_INFINITY, 100001, 1.5, 0.0001})
      assertThrows(IllegalArgumentException.class, () -> TpchTable.CUSTOMER.parentRows(scale));
    assertThrows(IllegalArgumentException.class, () -> new RowRange(-1, 0));
  }

  @Test
  void supplierCommentEditsDoNotMutateSharedText() {
    byte[] pool = "before Customer after Complaints untouched".getBytes(StandardCharsets.US_ASCII);
    byte[] original = pool.clone();
    TextValue comment = new TextValue(pool.length);
    comment.set(pool, 7, 25);
    comment.replace(0, "Customer ".getBytes(StandardCharsets.US_ASCII));
    comment.replace(15, "Recommends".getBytes(StandardCharsets.US_ASCII));
    assertArrayEquals(original, pool);
    assertEquals(
        "Customer after Recommends",
        new String(comment.bytes, comment.offset, comment.length, StandardCharsets.US_ASCII));
    // The same cursor can alternate between a shared slice and a privately edited comment.
    comment.set(pool, 0, pool.length);
    comment.replace(0, "AFTER!".getBytes(StandardCharsets.US_ASCII));
    assertArrayEquals(original, pool);
    assertEquals((byte) 'A', comment.bytes[0]);
  }

  @Test
  void nativeRemainderAndMorePartitionsThanParents() {
    RowRange first = RowRange.partition(TpchTable.CUSTOMER, 0.01, 7, 1);
    RowRange last = RowRange.partition(TpchTable.CUSTOMER, 0.01, 7, 7);
    assertEquals(0, first.start());
    assertEquals(214, first.end());
    assertEquals(1284, last.start());
    assertEquals(1500, last.end());
    assertFalse(TpchGenerator.partition(TpchTable.SUPPLIER, 0.01, 101, 1).next());
    RowCursor rows = TpchGenerator.partition(TpchTable.SUPPLIER, 0.01, 101, 101);
    int count = 0;
    while (rows.next()) count++;
    assertEquals(100, count);
  }

  @Test
  void independentCursorsDoNotShareMutableRandomState() {
    RowCursor one =
        TpchGenerator.range(TpchTable.LINE_ITEM, 10000, new RowRange(3000000000L, 3000000030L));
    RowCursor two =
        TpchGenerator.range(TpchTable.LINE_ITEM, 10000, new RowRange(3000000000L, 3000000030L));
    RowCursor noise = TpchGenerator.partition(TpchTable.PART, 1, 1, 1);
    while (one.next()) {
      noise.next();
      assertTrue(two.next());
      assertEquals(line(one), line(two));
    }
    assertFalse(two.next());
  }

  static String line(RowCursor rows) {
    StringBuilder text = new StringBuilder();
    for (int i = 0; i < rows.columnCount(); i++) {
      if (i > 0) text.append('|');
      text.append(rows.getString(i));
    }
    return text.toString();
  }

  static InputStream resource(String name) {
    InputStream input = TpchGeneratorTest.class.getResourceAsStream("/native/" + name);
    if (input == null) throw new IllegalArgumentException("Missing fixture: " + name);
    return input;
  }

  static BufferedReader reader(InputStream input) {
    return new BufferedReader(new InputStreamReader(input, StandardCharsets.US_ASCII));
  }
}
