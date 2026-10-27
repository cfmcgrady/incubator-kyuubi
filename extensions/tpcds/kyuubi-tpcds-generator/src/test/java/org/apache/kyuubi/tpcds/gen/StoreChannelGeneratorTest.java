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
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;

class StoreChannelGeneratorTest {
  @Test
  void everyFieldMatchesPinnedNativeSamples() throws Exception {
    try (BufferedReader manifest = reader(resource("manifest.tsv"))) {
      String entry;
      while ((entry = manifest.readLine()) != null) {
        if (entry.startsWith("#")) {
          continue;
        }
        String[] fields = entry.split("\t");
        StoreChannelGenerator generator =
            StoreChannelGenerator.partition(
                fields[3].equals("store_returns"), Double.parseDouble(fields[0]),
                Integer.parseInt(fields[1]), Integer.parseInt(fields[2]));
        int count = 0;
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (BufferedReader expected =
            reader(new DigestInputStream(new GZIPInputStream(resource(fields[6])), digest))) {
          String line;
          while ((line = expected.readLine()) != null) {
            assertTrue(generator.next(), fields[6] + " exhausted at " + count);
            assertEquals(line, line(generator), fields[6] + " row " + count);
            count++;
          }
        }
        assertFalse(generator.next(), fields[6] + " has extra rows");
        assertEquals(Integer.parseInt(fields[4]), count, fields[6]);
        StringBuilder hash = new StringBuilder();
        for (byte value : digest.digest()) {
          hash.append(String.format("%02x", value & 0xff));
        }
        assertEquals(fields[5], hash.toString(), fields[6] + " reference checksum");
      }
    }
  }

  @Test
  void arbitrarySplitsPreserveEveryValueAcrossTheIntegerBoundary() {
    long start = Integer.MAX_VALUE - 3L;
    long end = start + 20;
    for (boolean returns : new boolean[] {false, true}) {
      List<String> expected = rows(returns, start, end);
      assertFalse(expected.isEmpty());
      for (long split = start + 1; split < end; split++) {
        List<String> actual = rows(returns, start, split);
        actual.addAll(rows(returns, split, end));
        assertEquals(expected, actual, "returns=" + returns + " split=" + split);
      }
    }
  }

  @Test
  void cursorsHandleEmptyAndInvalidRanges() {
    StoreChannelGenerator empty = new StoreChannelGenerator(false, 0, new RowRange(0, 0));
    assertFalse(empty.next());
    assertFalse(empty.next());
    assertThrows(IllegalStateException.class, () -> empty.getLong(0));
    assertThrows(
        IllegalArgumentException.class,
        () -> new StoreChannelGenerator(false, 1, new RowRange(0, 240001)));
    assertThrows(
        IllegalArgumentException.class,
        () -> new StoreChannelGenerator(false, Double.NaN, new RowRange(0, 0)));
    assertFalse(StoreChannelGenerator.partition(false, 1, 2, 2).next());
  }

  private static List<String> rows(boolean returns, long start, long end) {
    StoreChannelGenerator generator =
        new StoreChannelGenerator(returns, 10000, new RowRange(start, end));
    List<String> result = new ArrayList<>();
    while (generator.next()) {
      result.add(line(generator));
    }
    return result;
  }

  private static String line(StoreChannelGenerator generator) {
    StringBuilder line = new StringBuilder();
    for (int i = 0; i < generator.columnCount(); i++) {
      String value = generator.getString(i);
      if (value != null) {
        line.append(value);
      }
      line.append('|');
    }
    return line.toString();
  }

  private static InputStream resource(String name) {
    InputStream stream = StoreChannelGeneratorTest.class.getResourceAsStream("/native/" + name);
    assertNotNull(stream, name);
    return stream;
  }

  private static BufferedReader reader(InputStream stream) {
    return new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
  }
}
