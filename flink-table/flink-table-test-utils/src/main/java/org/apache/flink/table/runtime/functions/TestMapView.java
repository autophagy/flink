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

import org.apache.flink.table.api.dataview.MapView;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test-specific MapView implementation with per-entry TTL tracking. Extends MapView to maintain
 * backward compatibility with user code.
 *
 * <p>Timestamps are tracked internally and are invisible to users.
 */
class TestMapView<K, V> extends MapView<K, V> implements TTLAwareDataView {

    private final Map<K, Long> entryTimestamps = new HashMap<>();
    private long currentTimeMillis = 0L;
    private boolean timeInitialized = false;

    @Override
    public void put(K key, V value) throws Exception {
        validateTimeInitialized();
        super.put(key, value);
        entryTimestamps.put(key, currentTimeMillis);
    }

    @Override
    public void putAll(Map<K, V> map) throws Exception {
        validateTimeInitialized();
        super.putAll(map);
        for (K key : map.keySet()) {
            entryTimestamps.put(key, currentTimeMillis);
        }
    }

    @Override
    public void remove(K key) throws Exception {
        super.remove(key);
        entryTimestamps.remove(key);
    }

    @Override
    public void clear() {
        super.clear();
        entryTimestamps.clear();
    }

    @Override
    public void setMap(Map<K, V> map) {
        super.setMap(map);
        // Note: Don't validate timeInitialized here - this is called during deserialization
        // before setCurrentTime(). Timestamps will be properly set later via injectTimestamps().
        entryTimestamps.clear();
        if (map != null) {
            for (K key : map.keySet()) {
                entryTimestamps.put(key, currentTimeMillis);
            }
        }
    }

    /**
     * Updates the current system time. Called by harness before eval(). Package-private, not
     * visible to user code.
     */
    @Override
    public void setCurrentTime(long timeMillis) {
        this.currentTimeMillis = timeMillis;
        this.timeInitialized = true;
    }

    /**
     * Validates that setCurrentTime() was called before mutations. Prevents silent timestamp
     * corruption.
     */
    private void validateTimeInitialized() {
        if (!timeInitialized) {
            throw new IllegalStateException(
                    "setCurrentTime() must be called before modifying TestMapView. "
                            + "This is a harness programming error.");
        }
    }

    /**
     * Injects timestamp metadata into this view. Called by harness before passing state to eval().
     * Package-private, not visible to user code.
     *
     * @param timestamps map of entry timestamps (key -> timestamp)
     */
    void injectTimestamps(Map<K, Long> timestamps) {
        entryTimestamps.clear();
        entryTimestamps.putAll(timestamps);
    }

    /**
     * Extracts timestamp metadata from this view. Called by harness after eval() completes.
     * Package-private, not visible to user code.
     *
     * @return map of entry timestamps (key -> timestamp)
     */
    Map<K, Long> extractTimestamps() {
        return new HashMap<>(entryTimestamps);
    }

    /**
     * Removes map entries that have exceeded their TTL. Package-private, called by harness during
     * advanceSystemClock().
     *
     * @param ttl The time-to-live duration
     */
    @Override
    public void evacuateExpiredEntries(Duration ttl) {
        if (ttl == null) {
            return; // No TTL, nothing to evacuate
        }

        long ttlMillis = ttl.toMillis();
        List<K> keysToRemove = new ArrayList<>();

        // Find expired entries
        for (Map.Entry<K, Long> entry : entryTimestamps.entrySet()) {
            K key = entry.getKey();
            long timestamp = entry.getValue();
            long expirationTime = timestamp + ttlMillis;

            if (currentTimeMillis >= expirationTime) {
                keysToRemove.add(key);
            }
        }

        // Remove expired entries
        for (K key : keysToRemove) {
            getMap().remove(key);
            entryTimestamps.remove(key);
        }
    }
}
