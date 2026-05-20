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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * State manager for {@link ProcessTableFunctionTestHarness}.
 *
 * <p>Handles state storage, lifecycle, and conversion between external and internal storage
 * formats. Supports TTL-based eviction for value state, ListView, and MapView state entries.
 */
@Internal
class TestHarnessStateManager {

    private final Map<Row, Map<String, Object>> stateByPartition = new HashMap<>();
    private final List<ProcessTableFunctionTestHarness.StateArgumentInfo> stateArguments;
    private final Map<String, StateConverter> stateConverters;

    private long currentSystemTimeMillis = 0L;

    // Per-element timestamps for ListView state, persisted between eval cycles while the
    // TtlAwareListView is recreated each time to force an internal-format round-trip.
    private final Map<Row, Map<String, List<Long>>> listStateTimestamps = new HashMap<>();

    // Per-entry timestamps for MapView state, same persistence rationale as above.
    private final Map<Row, Map<String, Map<Object, Long>>> mapStateTimestamps = new HashMap<>();

    // One timestamp per whole value, recorded only when the internal representation changes.
    private final Map<Row, Map<String, Long>> valueStateTimestamps = new HashMap<>();

    TestHarnessStateManager(
            List<ProcessTableFunctionTestHarness.StateArgumentInfo> stateArguments,
            Map<String, StateConverter> stateConverters) {
        this.stateArguments = stateArguments;
        this.stateConverters = stateConverters;
    }

    /**
     * Load state for a partition key. Creates new state instances if none exist. Converts internal
     * storage to external objects (POJOs, ListView, MapView). For TTL-enabled ListView/MapView
     * state, wraps the result in TtlAwareListView/TtlAwareMapView with saved timestamps.
     */
    Map<String, Object> loadStateForPartition(Row partitionKey) {
        Map<String, Object> internalState =
                stateByPartition.computeIfAbsent(partitionKey, k -> createNewPartitionState());

        Map<String, Object> externalState = new HashMap<>();
        for (ProcessTableFunctionTestHarness.StateArgumentInfo stateArg : stateArguments) {
            Object internalData = internalState.get(stateArg.name);
            Object external = convertToExternal(internalData, stateArg);
            external = wrapWithTtlIfNeeded(partitionKey, stateArg, external);
            externalState.put(stateArg.name, external);
        }
        return externalState;
    }

    /**
     * Update mutated state after eval() invocation. Extracts timestamps from
     * TtlAwareListView/TtlAwareMapView before converting to internal format. Records value state
     * write timestamps.
     */
    void updateStateForPartition(Row partitionKey, Map<String, Object> externalState)
            throws Exception {
        Map<String, Object> oldInternalState = stateByPartition.get(partitionKey);
        Map<String, Object> internalState = new HashMap<>();
        for (ProcessTableFunctionTestHarness.StateArgumentInfo stateArg : stateArguments) {
            Object external = externalState.get(stateArg.name);
            saveTimestampsIfNeeded(partitionKey, stateArg, external);
            Object internalData = convertToInternal(external, stateArg);

            if (stateArg.hasTtl() && stateKind(stateArg) == StateKind.VALUE) {
                Object oldInternal =
                        oldInternalState != null ? oldInternalState.get(stateArg.name) : null;
                if (!Objects.equals(oldInternal, internalData)) {
                    Map<String, Long> valueTsMap =
                            valueStateTimestamps.computeIfAbsent(
                                    partitionKey, k -> new HashMap<>());
                    valueTsMap.put(stateArg.name, currentSystemTimeMillis);
                }
            }

            internalState.put(stateArg.name, internalData);
        }
        stateByPartition.put(partitionKey, internalState);
    }

    /** Clear all state for a partition. */
    void clearStateForPartition(Row partitionKey) {
        stateByPartition.remove(partitionKey);
        valueStateTimestamps.remove(partitionKey);
        listStateTimestamps.remove(partitionKey);
        mapStateTimestamps.remove(partitionKey);
    }

    /** Clear specific state entry for a given partition, resetting it to its default value. */
    void clearStateEntry(Row partitionKey, String stateName) {
        Map<String, Object> internalState = stateByPartition.get(partitionKey);
        if (internalState != null) {
            ProcessTableFunctionTestHarness.StateArgumentInfo stateArg =
                    findStateArgument(stateName);
            internalState.put(stateName, createNewStateInternalData(stateArg));
        }
        Map<String, Long> valueTsMap = valueStateTimestamps.get(partitionKey);
        if (valueTsMap != null) {
            valueTsMap.remove(stateName);
        }
        Map<String, List<Long>> listTsMap = listStateTimestamps.get(partitionKey);
        if (listTsMap != null) {
            listTsMap.remove(stateName);
        }
        Map<String, Map<Object, Long>> mapTsMap = mapStateTimestamps.get(partitionKey);
        if (mapTsMap != null) {
            mapTsMap.remove(stateName);
        }
    }

    /** Set initial state for a given partition. */
    void setInitialState(String stateName, Row partitionKey, Object externalState)
            throws Exception {
        ProcessTableFunctionTestHarness.StateArgumentInfo stateArg = findStateArgument(stateName);
        Object internalData = convertToInternal(externalState, stateArg);

        Map<String, Object> internalState =
                stateByPartition.computeIfAbsent(partitionKey, k -> createNewPartitionState());
        internalState.put(stateName, internalData);

        if (stateArg.hasTtl()) {
            recordInitialTimestamps(partitionKey, stateArg, externalState);
        }
    }

    /** Get the state for given partition. */
    @SuppressWarnings("unchecked")
    <T> T getStateForKey(String stateName, Row partitionKey) {
        Map<String, Object> internalState = stateByPartition.get(partitionKey);
        if (internalState == null) {
            return null;
        }
        Object internalData = internalState.get(stateName);
        if (internalData == null) {
            return null;
        }
        return (T) convertToExternal(internalData, findStateArgument(stateName));
    }

    /** Get all partition keys that have a specific state entry. */
    Set<Row> getStateKeys(String stateName) {
        return stateByPartition.entrySet().stream()
                .filter(entry -> entry.getValue().containsKey(stateName))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /** Get all state values for a state name across all partitions. */
    @SuppressWarnings("unchecked")
    <T> Map<Row, T> getAllState(String stateName) {
        ProcessTableFunctionTestHarness.StateArgumentInfo stateArg = findStateArgument(stateName);
        Map<Row, T> result = new HashMap<>();
        for (Map.Entry<Row, Map<String, Object>> entry : stateByPartition.entrySet()) {
            Object internalData = entry.getValue().get(stateName);
            if (internalData != null) {
                result.put(entry.getKey(), (T) convertToExternal(internalData, stateArg));
            }
        }
        return result;
    }

    // ---- TTL / Clock ----

    void advanceSystemClock(long newTimeMillis) {
        if (newTimeMillis < currentSystemTimeMillis) {
            throw new IllegalArgumentException(
                    "Cannot move system clock backwards. Current time: "
                            + currentSystemTimeMillis
                            + ", requested: "
                            + newTimeMillis);
        }
        currentSystemTimeMillis = newTimeMillis;
        evictExpiredState();
    }

    long getCurrentSystemTime() {
        return currentSystemTimeMillis;
    }

    // ---- Private helpers ----

    private Map<String, Object> createNewPartitionState() {
        Map<String, Object> newState = new HashMap<>();
        for (ProcessTableFunctionTestHarness.StateArgumentInfo stateArg : stateArguments) {
            newState.put(stateArg.name, createNewStateInternalData(stateArg));
        }
        return newState;
    }

    private Object createNewStateInternalData(
            ProcessTableFunctionTestHarness.StateArgumentInfo stateArg) {
        return stateConverters.get(stateArg.name).createNewInternalState();
    }

    private Object convertToExternal(
            Object internalData, ProcessTableFunctionTestHarness.StateArgumentInfo stateArg) {
        return stateConverters.get(stateArg.name).toExternal(internalData);
    }

    private Object convertToInternal(
            Object external, ProcessTableFunctionTestHarness.StateArgumentInfo stateArg)
            throws Exception {
        return stateConverters.get(stateArg.name).toInternal(external);
    }

    private ProcessTableFunctionTestHarness.StateArgumentInfo findStateArgument(String stateName) {
        for (ProcessTableFunctionTestHarness.StateArgumentInfo stateArg : stateArguments) {
            if (stateArg.name.equals(stateName)) {
                return stateArg;
            }
        }
        String available =
                stateArguments.stream().map(arg -> arg.name).collect(Collectors.joining(", "));
        throw new IllegalArgumentException(
                "Unknown state: '" + stateName + "'. Available states: [" + available + "]");
    }

    // ---- TTL wrapping / unwrapping ----

    private static StateKind stateKind(ProcessTableFunctionTestHarness.StateArgumentInfo stateArg) {
        Class<?> conversionClass = stateArg.dataType.getConversionClass();
        if (ListView.class.isAssignableFrom(conversionClass)) {
            return StateKind.LIST_VIEW;
        } else if (MapView.class.isAssignableFrom(conversionClass)) {
            return StateKind.MAP_VIEW;
        } else {
            return StateKind.VALUE;
        }
    }

    /**
     * Wraps a plain ListView/MapView in a TtlAwareListView/TtlAwareMapView if TTL is configured for
     * this state argument, injecting saved timestamps.
     */
    @SuppressWarnings("unchecked")
    private Object wrapWithTtlIfNeeded(
            Row partitionKey,
            ProcessTableFunctionTestHarness.StateArgumentInfo stateArg,
            Object external) {
        if (!stateArg.hasTtl()) {
            return external;
        }
        StateKind kind = stateKind(stateArg);
        switch (kind) {
            case LIST_VIEW:
                {
                    ListView<Object> plainList = (ListView<Object>) external;
                    TtlAwareListView<Object> ttlList =
                            new TtlAwareListView<>(() -> currentSystemTimeMillis);
                    List<Long> savedTs = getSavedListTimestamps(partitionKey, stateArg.name);
                    if (savedTs != null) {
                        if (savedTs.size() != plainList.getList().size()) {
                            throw new IllegalStateException(
                                    "ListView timestamp count ("
                                            + savedTs.size()
                                            + ") does not match element count ("
                                            + plainList.getList().size()
                                            + ") for state '"
                                            + stateArg.name
                                            + "'");
                        }
                        ttlList.setListWithTimestamps(
                                new ArrayList<>(plainList.getList()), savedTs);
                    } else {
                        ttlList.setList(new ArrayList<>(plainList.getList()));
                    }
                    return ttlList;
                }
            case MAP_VIEW:
                {
                    MapView<Object, Object> plainMap = (MapView<Object, Object>) external;
                    TtlAwareMapView<Object, Object> ttlMap =
                            new TtlAwareMapView<>(() -> currentSystemTimeMillis);
                    Map<Object, Long> savedTs = getSavedMapTimestamps(partitionKey, stateArg.name);
                    if (savedTs != null) {
                        ttlMap.setMapWithTimestamps(new HashMap<>(plainMap.getMap()), savedTs);
                    } else {
                        ttlMap.setMap(new HashMap<>(plainMap.getMap()));
                    }
                    return ttlMap;
                }
            default:
                return external;
        }
    }

    /** Extracts timestamps from TtlAwareListView/TtlAwareMapView for DataView state with TTL. */
    @SuppressWarnings("unchecked")
    private void saveTimestampsIfNeeded(
            Row partitionKey,
            ProcessTableFunctionTestHarness.StateArgumentInfo stateArg,
            Object external) {
        if (!stateArg.hasTtl()) {
            return;
        }
        StateKind kind = stateKind(stateArg);
        switch (kind) {
            case LIST_VIEW:
                if (external instanceof TtlAwareListView) {
                    TtlAwareListView<Object> ttlList = (TtlAwareListView<Object>) external;
                    listStateTimestamps
                            .computeIfAbsent(partitionKey, k -> new HashMap<>())
                            .put(stateArg.name, new ArrayList<>(ttlList.getTimestamps()));
                }
                break;
            case MAP_VIEW:
                if (external instanceof TtlAwareMapView) {
                    TtlAwareMapView<Object, Object> ttlMap =
                            (TtlAwareMapView<Object, Object>) external;
                    mapStateTimestamps
                            .computeIfAbsent(partitionKey, k -> new HashMap<>())
                            .put(stateArg.name, new HashMap<>(ttlMap.getTimestamps()));
                }
                break;
            default:
                break;
        }
    }

    /** Records timestamps for initial state set via the builder. */
    @SuppressWarnings("unchecked")
    private void recordInitialTimestamps(
            Row partitionKey,
            ProcessTableFunctionTestHarness.StateArgumentInfo stateArg,
            Object externalState) {
        StateKind kind = stateKind(stateArg);
        switch (kind) {
            case LIST_VIEW:
                {
                    ListView<Object> listView = (ListView<Object>) externalState;
                    List<Long> timestamps = new ArrayList<>();
                    for (int i = 0; i < listView.getList().size(); i++) {
                        timestamps.add(currentSystemTimeMillis);
                    }
                    listStateTimestamps
                            .computeIfAbsent(partitionKey, k -> new HashMap<>())
                            .put(stateArg.name, timestamps);
                    break;
                }
            case MAP_VIEW:
                {
                    MapView<Object, Object> mapView = (MapView<Object, Object>) externalState;
                    Map<Object, Long> timestamps = new HashMap<>();
                    for (Object key : mapView.getMap().keySet()) {
                        timestamps.put(key, currentSystemTimeMillis);
                    }
                    mapStateTimestamps
                            .computeIfAbsent(partitionKey, k -> new HashMap<>())
                            .put(stateArg.name, timestamps);
                    break;
                }
            case VALUE:
                {
                    Map<String, Long> valueTsMap =
                            valueStateTimestamps.computeIfAbsent(
                                    partitionKey, k -> new HashMap<>());
                    valueTsMap.put(stateArg.name, currentSystemTimeMillis);
                    break;
                }
        }
    }

    private List<Long> getSavedListTimestamps(Row partitionKey, String stateName) {
        Map<String, List<Long>> tsMap = listStateTimestamps.get(partitionKey);
        return tsMap != null ? tsMap.get(stateName) : null;
    }

    private Map<Object, Long> getSavedMapTimestamps(Row partitionKey, String stateName) {
        Map<String, Map<Object, Long>> tsMap = mapStateTimestamps.get(partitionKey);
        return tsMap != null ? tsMap.get(stateName) : null;
    }

    // ---- TTL eviction ----

    private void evictExpiredState() {
        for (ProcessTableFunctionTestHarness.StateArgumentInfo stateArg : stateArguments) {
            if (!stateArg.hasTtl()) {
                continue;
            }
            long ttlMillis = stateArg.ttl.toMillis();
            StateKind kind = stateKind(stateArg);

            for (Row partitionKey : new ArrayList<>(stateByPartition.keySet())) {
                Map<String, Object> internalState = stateByPartition.get(partitionKey);
                if (internalState == null) {
                    continue;
                }

                switch (kind) {
                    case VALUE:
                        evictValueState(partitionKey, stateArg, ttlMillis, internalState);
                        break;
                    case LIST_VIEW:
                    case MAP_VIEW:
                        evictDataViewState(partitionKey, stateArg, ttlMillis, internalState);
                        break;
                }
            }
        }
    }

    private void evictValueState(
            Row partitionKey,
            ProcessTableFunctionTestHarness.StateArgumentInfo stateArg,
            long ttlMillis,
            Map<String, Object> internalState) {
        Map<String, Long> valueTsMap = valueStateTimestamps.get(partitionKey);
        if (valueTsMap == null) {
            return;
        }
        Long lastWrite = valueTsMap.get(stateArg.name);
        if (lastWrite != null && currentSystemTimeMillis - lastWrite >= ttlMillis) {
            internalState.put(stateArg.name, createNewStateInternalData(stateArg));
            valueTsMap.remove(stateArg.name);
        }
    }

    private void evictDataViewState(
            Row partitionKey,
            ProcessTableFunctionTestHarness.StateArgumentInfo stateArg,
            long ttlMillis,
            Map<String, Object> internalState) {
        Object internalData = internalState.get(stateArg.name);
        if (internalData == null) {
            return;
        }

        Object external = convertToExternal(internalData, stateArg);
        Object wrapped = wrapWithTtlIfNeeded(partitionKey, stateArg, external);

        if (wrapped instanceof TtlAwareListView) {
            ((TtlAwareListView<?>) wrapped).evictExpired(currentSystemTimeMillis, ttlMillis);
        } else if (wrapped instanceof TtlAwareMapView) {
            ((TtlAwareMapView<?, ?>) wrapped).evictExpired(currentSystemTimeMillis, ttlMillis);
        }

        saveTimestampsIfNeeded(partitionKey, stateArg, wrapped);
        try {
            internalState.put(stateArg.name, convertToInternal(wrapped, stateArg));
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert evicted state", e);
        }
    }

    private enum StateKind {
        VALUE,
        LIST_VIEW,
        MAP_VIEW
    }
}
