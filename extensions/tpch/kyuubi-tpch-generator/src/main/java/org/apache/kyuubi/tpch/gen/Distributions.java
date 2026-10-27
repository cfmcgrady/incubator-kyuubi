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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Immutable standard dictionaries. Nation weights are cumulative region codes, not probabilities.
 */
final class Distributions {
  private static final Map<String, Distributions> ALL = load();
  final String[] words;
  final byte[][] bytes;
  private final int[] cumulative;

  private Distributions(List<String> words, List<Integer> weights) {
    this.words = words.toArray(new String[0]);
    bytes =
        words.stream().map(word -> word.getBytes(StandardCharsets.US_ASCII)).toArray(byte[][]::new);
    cumulative = new int[weights.size()];
    int sum = 0;
    for (int i = 0; i < weights.size(); i++) {
      sum += weights.get(i);
      cumulative[i] = sum;
    }
  }

  static Distributions get(String name) {
    Distributions result = ALL.get(name);
    if (result == null) throw new IllegalArgumentException("Missing distribution: " + name);
    return result;
  }

  int weight(int index) {
    return cumulative[index];
  }

  String pick(RandomStreams random, int stream) {
    return words[pickIndex(random, stream)];
  }

  byte[] pickBytes(RandomStreams random, int stream) {
    return bytes[pickIndex(random, stream)];
  }

  private int pickIndex(RandomStreams random, int stream) {
    int value = (int) random.uniform(stream, 1, cumulative[cumulative.length - 1]);
    int lo = 0, hi = cumulative.length - 1;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (cumulative[mid] >= value) hi = mid;
      else lo = mid + 1;
    }
    return lo;
  }

  private static Map<String, Distributions> load() {
    Map<String, Distributions> result = new HashMap<>();
    try (InputStream input = Distributions.class.getResourceAsStream("dists.dss")) {
      if (input == null) throw new IllegalStateException("Missing TPC-H dictionaries");
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(input, StandardCharsets.US_ASCII))) {
        String line, name = null;
        List<String> words = new ArrayList<>();
        List<Integer> weights = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
          int comment = line.indexOf('#');
          if (comment >= 0) line = line.substring(0, comment);
          line = line.trim();
          if (line.isEmpty()) continue;
          String lower = line.toLowerCase(Locale.ROOT);
          if (lower.startsWith("begin ")) {
            name = lower.substring(6).trim();
            words = new ArrayList<>();
            weights = new ArrayList<>();
          } else if (lower.startsWith("end ")) {
            result.put(name, new Distributions(words, weights));
            name = null;
          } else if (name != null) {
            String[] fields = line.split("\\|", -1);
            if (fields[0].equalsIgnoreCase("count")) continue;
            words.add(fields[0].trim());
            weights.add(Integer.parseInt(fields[1].trim()));
          }
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("Cannot load TPC-H dictionaries", e);
    }
    return Map.copyOf(result);
  }
}
