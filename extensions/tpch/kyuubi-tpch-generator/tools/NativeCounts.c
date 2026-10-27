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

#define main dbgen_original_main
#include "driver.c"
#undef main

/* Uses the unmodified native dss_random function and initial O_LCNT stream. */
int main(void) {
  const long long period = 2147483646LL;
  const long long rows[] = {
      15000LL, 1500000LL, 15000000LL, 45000000LL, 150000000LL, 450000000LL,
      1500000000LL, 4500000000LL, 15000000000LL, 45000000000LL, 150000000000LL};
  long long prefix[11] = {0};
  long long total = 0, i;
  int j;
  DSS_HUGE value;
  for (i = 1; i <= period; i++) {
    dss_random(&value, 1, 7, O_LCNT_SD);
    total += value;
    for (j = 0; j < 11; j++) {
      if (i == rows[j] % period) prefix[j] = total;
    }
  }
  if (total != period * 4) return 2;
  for (j = 0; j < 11; j++) {
    printf("%lld\t%lld\n", rows[j], rows[j] / period * total + prefix[j]);
  }
  return 0;
}
