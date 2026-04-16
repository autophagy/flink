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

import org.apache.flink.table.api.dataview.ListView;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test-specific ListView implementation with per-element TTL tracking. Extends ListView to maintain
 * backward compatibility with user code.
 *
 * <p>Timestamps are tracked internally and are invisible to users.
 */
class TestListView<T> extends ListView<T> implements TTLAwareDataView {

    /** Wrapper that pairs an element with its insertion timestamp. */
    private static class TimestampedElement<T> {
        final T value;
        final long timestamp;

        TimestampedElement(T value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    private final List<TimestampedElement<T>> timestampedList = new ArrayList<>();
    private long currentTimeMillis = 0L;
    private boolean timeInitialized = false;

    @Override
    public void add(T value) throws Exception {
        validateTimeInitialized();
        timestampedList.add(new TimestampedElement<>(value, currentTimeMillis));
    }

    @Override
    public void addAll(List<T> list) throws Exception {
        validateTimeInitialized();
        for (T value : list) {
            timestampedList.add(new TimestampedElement<>(value, currentTimeMillis));
        }
    }

    @Override
    public boolean remove(T value) throws Exception {
        for (int i = 0; i < timestampedList.size(); i++) {
            if (java.util.Objects.equals(timestampedList.get(i).value, value)) {
                timestampedList.remove(i);
                return true;
            }
        }
        return false;
    }

    @Override
    public void clear() {
        timestampedList.clear();
    }

    @Override
    public void setList(List<T> list) {
        // Sync timestampedList from the new list
        // Note: Don't validate timeInitialized here - this is called during deserialization
        // before setCurrentTime(). Timestamps will be properly set later via injectTimestamps().
        timestampedList.clear();
        if (list != null) {
            for (T value : list) {
                timestampedList.add(new TimestampedElement<>(value, currentTimeMillis));
            }
        }
    }

    @Override
    public List<T> getList() {
        // Return unwrapped values - used by converter for serialization
        return unwrapValues();
    }

    @Override
    public List<T> get() throws Exception {
        // Always return the unwrapped values
        return unwrapValues();
    }

    private List<T> unwrapValues() {
        return timestampedList.stream().map(element -> element.value).collect(Collectors.toList());
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
                    "setCurrentTime() must be called before modifying TestListView. "
                            + "This is a harness programming error.");
        }
    }

    /**
     * Injects timestamp metadata into this view. Called by harness before passing state to eval().
     * Package-private, not visible to user code.
     *
     * @param timestamps list of timestamps, one per element (parallel to list data)
     */
    void injectTimestamps(List<Long> timestamps) {
        // Rebuild timestampedList from current data + provided timestamps
        List<T> currentValues = getList(); // Get from parent
        timestampedList.clear();

        for (int i = 0; i < currentValues.size(); i++) {
            T value = currentValues.get(i);
            long timestamp = (i < timestamps.size()) ? timestamps.get(i) : currentTimeMillis;
            timestampedList.add(new TimestampedElement<>(value, timestamp));
        }
    }

    /**
     * Extracts timestamp metadata from this view. Called by harness after eval() completes.
     * Package-private, not visible to user code.
     *
     * @return list of timestamps, one per element (parallel to list data)
     */
    List<Long> extractTimestamps() {
        List<Long> timestamps = new java.util.ArrayList<>();
        for (TimestampedElement<T> element : timestampedList) {
            timestamps.add(element.timestamp);
        }
        return timestamps;
    }

    /**
     * Removes list elements that have exceeded their TTL. Package-private, called by harness during
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

        // Simple filter - remove expired elements
        timestampedList.removeIf(element -> currentTimeMillis >= element.timestamp + ttlMillis);
    }
}
