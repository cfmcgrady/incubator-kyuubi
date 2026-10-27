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

import static org.apache.kyuubi.tpcds.gen.GeneratorSupport.*;

final class Address {
  long number, zip, offset;
  String street, type, suite, city, county, state;

  void generate(RandomStreams random, int stream, TpcdsTable table, Scale scale) {
    number = random.uniform(stream, 1, 1000);
    street = pick(random, stream, "street_names") + " " + pick(random, stream, "street_names", 1);
    type = pick(random, stream, "street_types");
    int suiteNumber = (int) random.uniform(stream, 1, 100);
    suite =
        "Suite "
            + ((suiteNumber & 1) != 0
                ? Integer.toString((suiteNumber >> 1) * 10)
                : Character.toString((char) ('A' + (suiteNumber >> 1) % 25)));
    Distribution cities = Distribution.get("cities"), counties = Distribution.get("fips");
    int cityIndex =
        table.isSmall()
            ? (int) random.uniform(stream, 0, Math.min(scale.cities(), scale.rows(table)) - 1)
            : cities.pick(random, stream, 5);
    city = cities.value(cityIndex, 0);
    int region =
        table.isSmall()
            ? (int) random.uniform(stream, 0, Math.min(scale.counties(), scale.rows(table)) - 1)
            : counties.pick(random, stream, 0);
    county = counties.value(region, 1);
    state = counties.value(region, 2);
    offset = counties.number(region, 5);
    int prefix = Integer.parseInt(counties.value(region, 4));
    zip = hash(city);
    if (prefix == 0 && zip < 9400) zip += 600;
    zip += prefix * 10000;
  }

  void write(GeneratedRow row, int start) {
    row.putNumberText(start, number, 1);
    row.put(start + 1, street);
    row.put(start + 2, type);
    row.put(start + 3, suite);
    row.put(start + 4, city);
    row.put(start + 5, county);
    row.put(start + 6, state);
    row.putNumberText(start + 7, zip, 5);
    row.put(start + 8, "United States");
    row.put(start + 9, offset * 100);
  }

  private static int hash(String value) {
    int current = 0, result = 0;
    for (int i = 0; i < value.length(); i++) {
      current = current * 26 + value.charAt(i) - 'A';
      if (current > 1000000) {
        result += current % 10000;
        current = 0;
      }
    }
    return (result + current % 1000) % 10000;
  }
}
