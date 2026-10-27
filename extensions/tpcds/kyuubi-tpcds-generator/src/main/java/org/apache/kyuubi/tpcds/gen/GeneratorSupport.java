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

import java.time.LocalDate;

final class GeneratorSupport {
  static final long JULIAN_EPOCH = 2440588;
  static final long FIRST_DATE = julian(1998, 1, 1);
  static final long LAST_DATE = julian(2003, 12, 31);
  static final long TODAY = julian(2003, 1, 8);
  static final String ALPHANUM = "abcdefghijklmnopqrstuvxyzABCDEFGHIJKLMNOPQRSTUVXYZ0123456789";

  static long julian(int year, int month, int day) {
    return LocalDate.of(year, month, day).toEpochDay() + JULIAN_EPOCH;
  }

  static LocalDate date(long julian) {
    return LocalDate.ofEpochDay(julian - JULIAN_EPOCH);
  }

  static long historyKey(long id, long date, long maximum) {
    long group = id / 3 * 6, duration = LAST_DATE - FIRST_DATE;
    long key =
        id % 3 == 1
            ? group + 1
            : id % 3 == 2
                ? group + 2 + (date > FIRST_DATE + duration / 2 ? 1 : 0)
                : group
                    - 2
                    + (date > FIRST_DATE + duration / 3 ? 1 : 0)
                    + (date > FIRST_DATE + duration / 3 * 2 ? 1 : 0);
    return Math.min(key, maximum);
  }

  static long join(RandomStreams random, int stream, Scale scale, TpcdsTable table, long date) {
    long key = random.uniform(stream, 1, scale.ids(table));
    return table.history ? (date > LAST_DATE ? -1 : historyKey(key, date, scale.rows(table))) : key;
  }

  static long nullBitmap(RandomStreams random, int stream, TpcdsTable table) {
    long selector = random.uniform(stream, 0, 9999);
    long bits = random.uniform(stream, 1, Integer.MAX_VALUE);
    return selector < table.nullThreshold ? bits & ~table.requiredBits : 0;
  }

  static String pick(RandomStreams random, int stream, String dictionary) {
    return pick(random, stream, dictionary, 0);
  }

  static String pick(RandomStreams random, int stream, String dictionary, int weights) {
    return Distribution.get(dictionary).pick(random, stream, 0, weights);
  }

  static String name(RandomStreams random, int stream) {
    return pick(random, stream, "first_names") + " " + pick(random, stream, "last_names");
  }

  static String word(String dictionary, long index, int maximum, StringBuilder result) {
    Distribution syllables = Distribution.get(dictionary);
    result.setLength(0);
    while (index > 0) {
      String syllable = syllables.value((int) (index % syllables.size()), 0);
      if (result.length() + syllable.length() > maximum) break;
      result.append(syllable);
      index /= syllables.size();
    }
    return result.toString();
  }

  static String characters(
      RandomStreams random, int stream, String alphabet, int minimum, int maximum, char[] result) {
    int length = (int) random.uniform(stream, minimum, maximum);
    for (int i = 0; i < maximum; i++) {
      char value = alphabet.charAt((int) random.uniform(stream, 0, alphabet.length() - 1));
      if (i < length) result[i] = value;
    }
    return new String(result, 0, length);
  }

  static String text(
      RandomStreams random, int stream, int minimum, int maximum, StringBuilder result) {
    int length = (int) random.uniform(stream, minimum, maximum);
    result.setLength(0);
    boolean capitalize = true;
    while (result.length() < length) {
      String syntax = pick(random, stream, "sentences");
      int start = result.length();
      for (int i = 0; i < syntax.length(); i++) {
        char symbol = syntax.charAt(i);
        String dictionary;
        switch (symbol) {
          case 'N':
            dictionary = "nouns";
            break;
          case 'V':
            dictionary = "verbs";
            break;
          case 'J':
            dictionary = "adjectives";
            break;
          case 'D':
            dictionary = "adverbs";
            break;
          case 'X':
            dictionary = "auxiliaries";
            break;
          case 'P':
            dictionary = "prepositions";
            break;
          case 'A':
            dictionary = "articles";
            break;
          case 'T':
            dictionary = "terminators";
            break;
          default:
            dictionary = null;
        }
        if (dictionary == null) result.append(symbol);
        else result.append(pick(random, stream, dictionary));
      }
      if (capitalize) result.setCharAt(start, Character.toUpperCase(result.charAt(start)));
      capitalize = result.charAt(result.length() - 1) == '.';
      if (result.length() < length) result.append(' ');
    }
    return result.substring(0, length);
  }

  private GeneratorSupport() {}
}
