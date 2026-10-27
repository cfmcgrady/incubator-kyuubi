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

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class TpcdsGeneratorTest {
  @TestFactory
  List<DynamicTest> everyTableAndFieldMatchesNative() throws Exception {
    List<DynamicTest> tests = new ArrayList<>();
    EnumSet<TpcdsTable> tables = EnumSet.noneOf(TpcdsTable.class);
    try (BufferedReader manifest = reader(resource("manifest.tsv"))) {
      String line;
      while ((line = manifest.readLine()) != null) {
        if (line.startsWith("#")) continue;
        String[] fields = line.split("\t");
        tables.add(TpcdsTable.getTable(fields[0]));
        tests.add(DynamicTest.dynamicTest(fields[6], () -> verify(fields)));
      }
    }
    assertEquals(EnumSet.allOf(TpcdsTable.class), tables);
    return tests;
  }

  private void verify(String[] fields) throws Exception {
    TpcdsTable table = TpcdsTable.getTable(fields[0]);
    RowCursor actual =
        TpcdsGenerator.range(
            table,
            Double.parseDouble(fields[1]),
            new RowRange(Long.parseLong(fields[2]), Long.parseLong(fields[3])));
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    int count = 0;
    try (BufferedReader expected =
        reader(new DigestInputStream(new GZIPInputStream(resource(fields[6])), digest))) {
      String line;
      while ((line = expected.readLine()) != null) {
        assertTrue(actual.next(), "exhausted at row " + count);
        assertEquals(line, line(actual), "row " + count);
        for (int i = 0; i < actual.columnCount(); i++) {
          if (actual.isNull(i)) assertTrue(table.column(i).isNullable(), table.column(i).getName());
          String type = table.column(i).getType();
          if (type.startsWith("char") || type.startsWith("varchar")) {
            if (actual.isNull(i)) {
              assertNull(actual.getBytes(i));
              assertEquals(0, actual.getByteLength(i));
            } else {
              assertEquals(
                  actual.getString(i),
                  new String(
                      actual.getBytes(i),
                      actual.getByteOffset(i),
                      actual.getByteLength(i),
                      StandardCharsets.UTF_8));
            }
          }
        }
        count++;
      }
    }
    assertEquals(Integer.parseInt(fields[4]), count);
    assertFalse(actual.next(), "extra rows");
    assertFalse(actual.next(), "cursor must remain exhausted");
    StringBuilder checksum = new StringBuilder();
    for (byte b : digest.digest()) checksum.append(String.format("%02x", b & 255));
    assertEquals(fields[5], checksum.toString(), "fixture checksum");
  }

  @Test
  void utf8BuffersMatchJdkEncodingAndDoNotModifyDictionaries() {
    Utf8Value value = new Utf8Value();
    Distribution.get("countries");
    byte[] country = Distribution.encoded("CÔTE D'IVOIRE");
    assertNotNull(country);
    byte[] saved = country.clone();
    for (String text :
        new String[] {"CÔTE D'IVOIRE", "", "ASCII", "éàÿ", "中文", "𝄞", "\uD800x\uDC00"}) {
      value.encode(text);
      assertArrayEquals(
          text.getBytes(StandardCharsets.UTF_8), Arrays.copyOf(value.bytes, value.length));
      value.number(Long.MIN_VALUE, 5);
      assertEquals(Long.toString(Long.MIN_VALUE), value.toString());
      value.businessKey(1);
      assertEquals("AAAAAAAABAAAAAAA", value.toString());
    }
    assertArrayEquals(saved, country);
    for (long number : new long[] {0, 1, -1, 1234, -1234, 123456, Long.MAX_VALUE, Long.MIN_VALUE}) {
      value.number(number, 5);
      assertEquals(String.format(java.util.Locale.ROOT, "%05d", number), value.toString());
    }
  }

  @Test
  void precomputedNullMappingsPreserveDuplicateAndReorderedBits() {
    for (TpcdsTable table : TpcdsTable.values()) {
      for (int bit = 0; bit < Long.SIZE; bit++) {
        long bitmap = 1L << bit, expected = 0;
        for (int column = 0; column < table.columnCount(); column++) {
          if ((bitmap & (1L << table.column(column).nullBit)) != 0) expected |= 1L << column;
        }
        assertEquals(expected, table.outputNulls(bitmap), table + " bit " + bit);
      }
    }
  }

  @Test
  void emptyPartitionsAndInvalidRangesForEveryTable() {
    for (TpcdsTable table : TpcdsTable.values()) {
      RowCursor empty = TpcdsGenerator.partition(table, 0, 2, 2);
      assertFalse(empty.next(), table.getName());
      assertThrows(IllegalStateException.class, () -> empty.getLong(0));
      long total = new Scale(1).rows(table);
      assertThrows(
          IllegalArgumentException.class,
          () -> TpcdsGenerator.range(table, 1, new RowRange(total, total + 1)));
      assertThrows(IllegalArgumentException.class, () -> TpcdsGenerator.partition(table, 1, 0, 1));
    }
    for (double scale : new double[] {Double.NaN, Double.POSITIVE_INFINITY, -1, 100001, 1.5}) {
      assertThrows(IllegalArgumentException.class, () -> new Scale(scale));
    }
  }

  @Test
  void nonstandardScaleUsesNativeCardinalitiesAndForeignKeyDomains() {
    Scale scale = new Scale(20);
    assertEquals(666000, scale.rows(TpcdsTable.CUSTOMER));
    assertEquals(333000, scale.rows(TpcdsTable.CUSTOMER_ADDRESS));
    assertEquals(112000, scale.rows(TpcdsTable.ITEM));
    for (TpcdsTable table :
        new TpcdsTable[] {TpcdsTable.CATALOG_SALES, TpcdsTable.STORE_SALES, TpcdsTable.WEB_SALES}) {
      RowCursor rows = TpcdsGenerator.range(table, 20, new RowRange(100, 200));
      int customer = table == TpcdsTable.STORE_SALES ? 3 : table == TpcdsTable.WEB_SALES ? 4 : 3;
      int item = table == TpcdsTable.STORE_SALES ? 2 : table == TpcdsTable.WEB_SALES ? 3 : 15;
      while (rows.next()) {
        assertTrue(rows.getLong(customer) <= scale.rows(TpcdsTable.CUSTOMER));
        assertTrue(rows.getLong(item) <= scale.rows(TpcdsTable.ITEM));
      }
    }
  }

  @Test
  void allSalesChannelsSeekWithLongOffsets() {
    for (TpcdsTable table : TpcdsTable.values()) {
      if (!table.getName().endsWith("_sales") && !table.getName().endsWith("_returns")) continue;
      long first = Integer.MAX_VALUE - 3L, last = first + 20;
      List<String> expected = rows(table, 100000, first, last);
      for (long split :
          new long[] {Integer.MAX_VALUE - 1L, Integer.MAX_VALUE, Integer.MAX_VALUE + 1L}) {
        List<String> actual = rows(table, 100000, first, split);
        actual.addAll(rows(table, 100000, split, last));
        assertEquals(expected, actual, table + " split " + split);
      }
    }
  }

  @Test
  void historicalDimensionsCanResumeInsideABusinessKey() {
    for (TpcdsTable table : TpcdsTable.values()) {
      if (!table.history) continue;
      long end = Math.min(30, new Scale(1).rows(table));
      List<String> expected = rows(table, 1, 0, end);
      for (int start = 1; start < end; start++) {
        assertEquals(
            expected.subList(start, (int) end),
            rows(table, 1, start, end),
            table + " start " + start);
      }
    }
  }

  private static List<String> rows(TpcdsTable table, double scale, long first, long last) {
    RowCursor rows = TpcdsGenerator.range(table, scale, new RowRange(first, last));
    List<String> result = new ArrayList<>();
    while (rows.next()) result.add(line(rows));
    return result;
  }

  private static String line(RowCursor row) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < row.columnCount(); i++) {
      String field = row.getString(i);
      if (field != null) result.append(field);
      result.append('|');
    }
    return result.toString();
  }

  private static InputStream resource(String name) {
    InputStream input = TpcdsGeneratorTest.class.getResourceAsStream("/native/all/" + name);
    assertNotNull(input, name);
    return input;
  }

  private static BufferedReader reader(InputStream input) {
    return new BufferedReader(new InputStreamReader(input, StandardCharsets.ISO_8859_1));
  }
}
