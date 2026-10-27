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

import static org.apache.kyuubi.tpcds.gen.TpcdsTable.*;

/** Precomputed key bounds for the store channel's hot loop. */
final class StoreScale {
  final long orders;
  final long customers;
  final long addresses;
  final int items;
  final int itemIds;
  final int stores;
  final int storeIds;
  final int promotions;
  final int reasons;

  StoreScale(double scale) {
    Scale sizes = new Scale(scale);
    orders = sizes.rows(STORE_SALES);
    customers = sizes.rows(CUSTOMER);
    addresses = sizes.rows(CUSTOMER_ADDRESS);
    items = Math.toIntExact(sizes.rows(ITEM));
    itemIds = Math.toIntExact(sizes.ids(ITEM));
    stores = Math.toIntExact(sizes.rows(STORE));
    storeIds = Math.toIntExact(sizes.ids(STORE));
    promotions = Math.toIntExact(sizes.rows(PROMOTION));
    reasons = Math.toIntExact(sizes.rows(REASON));
  }
}
