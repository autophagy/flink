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

import java.util.List;
import java.util.Map;

/**
 * Metadata for tracking element/entry timestamps in TTL-enabled state.
 *
 * <p>This interface provides explicit separation between state data (stored as RowData) and
 * timestamp metadata (stored separately). This avoids caching complexity and makes timestamp
 * tracking explicit.
 *
 * <p>Package-private, not exposed to user code.
 */
interface TimestampMetadata {}

/** Timestamp metadata for value state (POJOs, Row) - tracks single timestamp. */
class ValueTimestamp implements TimestampMetadata {
    final long timestamp;

    ValueTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}

/** Timestamp metadata for ListView state - tracks timestamp per list element. */
class ListTimestamps implements TimestampMetadata {
    final List<Long> elementTimestamps;

    ListTimestamps(List<Long> elementTimestamps) {
        this.elementTimestamps = elementTimestamps;
    }
}

/** Timestamp metadata for MapView state - tracks timestamp per map entry. */
class MapTimestamps implements TimestampMetadata {
    final Map<Object, Long> entryTimestamps;

    @SuppressWarnings({"unchecked", "rawtypes"})
    MapTimestamps(Map entryTimestamps) {
        this.entryTimestamps = entryTimestamps;
    }
}
