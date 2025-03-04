/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTaskPerformanceSplits {
  @Test
  public void testPeriodStatsets() {
    PeriodicStatsAccumulator cumulative = new CumulativePeriodicStats(8);
    PeriodicStatsAccumulator status = new StatePeriodicStats(8);

    cumulative.extend(0.0D, 0);
    cumulative.extend(0.4375D, 700); // 200 per octant
    cumulative.extend(0.5625D, 1100); // 0.5 = 900
    cumulative.extend(0.625D, 1300);
    cumulative.extend(1.0D, 7901);

    int total = 0;
    int[] results = cumulative.getValues();

    for (int i = 0; i < 8; ++i) {
      System.err.println("segment i = " + results[i]);
    }

    assertEquals(200, results[0], "Bad interpolation in cumulative segment 0");
    assertEquals(200, results[1], "Bad interpolation in cumulative segment 1");
    assertEquals(200, results[2], "Bad interpolation in cumulative segment 2");
    assertEquals(300, results[3], "Bad interpolation in cumulative segment 3");
    assertEquals(400, results[4], "Bad interpolation in cumulative segment 4");
    assertEquals(2200, results[5], "Bad interpolation in cumulative segment 5");
    // these are rounded down
    assertEquals(2200, results[6], "Bad interpolation in cumulative segment 6");
    assertEquals(2201, results[7], "Bad interpolation in cumulative segment 7");

    status.extend(0.0D, 0);
    status.extend(1.0D/16.0D, 300); // + 75 for bucket 0
    status.extend(3.0D/16.0D, 700); // + 200 for 0, +300 for 1
    status.extend(7.0D/16.0D, 2300); // + 450 for 1, + 1500 for 2, + 1050 for 3
    status.extend(1.0D, 1400);  // +1125 for 3, +2100 for 4, +1900 for 5,
    ;                           // +1700 for 6, +1500 for 7

    results = status.getValues();

    assertEquals(275, results[0], "Bad interpolation in status segment 0");
    assertEquals(750, results[1], "Bad interpolation in status segment 1");
    assertEquals(1500, results[2], "Bad interpolation in status segment 2");
    assertEquals(2175, results[3], "Bad interpolation in status segment 3");
    assertEquals(2100, results[4], "Bad interpolation in status segment 4");
    assertEquals(1900, results[5], "Bad interpolation in status segment 5");
    assertEquals(1700, results[6], "Bad interpolation in status segment 6");
    assertEquals(1500, results[7], "Bad interpolation in status segment 7");
  }
}
