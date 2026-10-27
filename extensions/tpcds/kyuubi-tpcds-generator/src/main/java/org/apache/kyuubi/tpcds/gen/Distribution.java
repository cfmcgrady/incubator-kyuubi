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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/** Immutable benchmark dictionaries, shared by independent task-local random streams. */
final class Distribution {
  private static final ConcurrentHashMap<String, Distribution> CACHE = new ConcurrentHashMap<>();
  private final String[][] values;
  private final int[][] cumulative;

  private Distribution(String[][] values, int[][] cumulative) {
    this.values = values;
    this.cumulative = cumulative;
  }

  static Distribution get(String name) {
    return CACHE.computeIfAbsent(name, Distribution::read);
  }

  int size() {
    return values.length;
  }

  String value(int index, int column) {
    return values[index][column];
  }

  long number(int index, int column) {
    return Long.parseLong(value(index, column));
  }

  int weight(int index, int column) {
    return cumulative[column][index] - (index == 0 ? 0 : cumulative[column][index - 1]);
  }

  int total(int column) {
    return cumulative[column][size() - 1];
  }

  int pick(RandomStreams random, int stream, int weights) {
    long target = random.uniform(stream, 1, total(weights));
    int low = 0, high = size() - 1;
    while (low < high) {
      int middle = (low + high) >>> 1;
      if (cumulative[weights][middle] >= target) high = middle;
      else low = middle + 1;
    }
    return low;
  }

  String pick(RandomStreams random, int stream, int column, int weights) {
    return value(pick(random, stream, weights), column);
  }

  private static Distribution read(String name) {
    String path = "distribution/" + name + ".dst";
    InputStream input = Distribution.class.getResourceAsStream(path);
    if (input == null) throw new IllegalStateException("Missing TPC-DS dictionary: " + path);
    List<String[]> values = new ArrayList<>();
    List<int[]> weights = new ArrayList<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(input, StandardCharsets.ISO_8859_1))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("--")) continue;
        int colon = line.lastIndexOf(':');
        if (colon < 0) throw new IllegalArgumentException("Invalid dictionary entry in " + path);
        String[] fields = line.substring(0, colon).split("(?<!\\\\),", -1);
        for (int i = 0; i < fields.length; i++) fields[i] = fields[i].trim().replace("\\,", ",");
        values.add(fields);
        String[] counts = line.substring(colon + 1).split(",");
        int[] row = new int[counts.length];
        for (int i = 0; i < row.length; i++) row[i] = Integer.parseInt(counts[i].trim());
        weights.add(row);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Reading " + path, e);
    }
    int[][] cumulative = new int[weights.get(0).length][values.size()];
    for (int w = 0; w < cumulative.length; w++) {
      int sum = 0;
      for (int i = 0; i < values.size(); i++) {
        sum = Math.addExact(sum, weights.get(i)[w]);
        cumulative[w][i] = sum;
      }
    }
    return new Distribution(values.toArray(new String[0][]), cumulative);
  }
}
