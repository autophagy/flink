/*
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

package org.apache.flink.table.runtime.functions;

import java.time.Duration;

/**
 * Interface for data view implementations that support TTL (Time-To-Live) functionality.
 *
 * <p>This interface is used by the test harness to manage time and trigger expiration of state
 * entries. Implementations like {@link TestListView} and {@link TestMapView} track timestamps
 * internally and remove expired entries when time advances.
 *
 * <p>Package-private interface, not exposed to user code.
 */
interface TTLAwareDataView {

    /**
     * Updates the current system time. Called by the harness before eval() to ensure the data view
     * knows the current time for timestamp tracking.
     *
     * @param timeMillis current system time in milliseconds since epoch 0
     */
    void setCurrentTime(long timeMillis);

    /**
     * Removes entries that have exceeded their TTL. Called by the harness during
     * advanceSystemClock().
     *
     * @param ttl the time-to-live duration, or null if no TTL is configured
     */
    void evacuateExpiredEntries(Duration ttl);
}
