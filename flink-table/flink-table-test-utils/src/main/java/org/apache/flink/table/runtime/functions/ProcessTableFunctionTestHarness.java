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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.SystemTypeInference;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Test harness for {@link ProcessTableFunction}.
 *
 * <p>Provides a fluent builder API for configuring and testing ProcessTableFunctions (PTFs) with
 * table and scalar arguments, lifecycle management, and output collection.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ProcessTableFunctionTestHarness<Row> harness =
 *     ProcessTableFunctionTestHarness.ofClass(MyPTF.class)
 *         .withTableArgumentType("input", DataTypes.of("ROW<id INT, name STRING>"))
 *         .withScalarArgument("threshold", 100)
 *         .build();
 *
 * harness.processElement(Row.of(1, "Alice"));
 * harness.processElement(Row.of(2, "Bob"));
 *
 * List<Row> output = harness.getOutput();
 * }</pre>
 */
@PublicEvolving
public class ProcessTableFunctionTestHarness<OUT> implements AutoCloseable {

    private final ProcessTableFunction<OUT> function;
    private final FunctionContext functionContext;
    private final List<OUT> output;
    private boolean isOpen;
    private final HarnessCollector collector;

    private final String defaultTableArgument;
    private final java.lang.reflect.Method evalMethod;
    private final List<ArgumentInfo> arguments;

    private final Map<String, ArgumentInfo> argumentsByName;
    private final boolean isSingleTableFunction;
    private final Map<String, Object> scalarArgumentValues;

    private final Map<String, DataStructureConverter<Object, Object>> inputConverters;
    private final Map<String, DataStructureConverter<Object, Object>> outputConverters;
    private final Map<String, DataStructureConverter<Object, Object>> stateConverters;

    // State storage: partitionKey -> (stateArgumentName -> RowData)
    // State is stored as RowData internally to ensure it can be serialized like in live PTF runs
    private final Map<Row, Map<String, RowData>> stateByPartition;

    // System clock (milliseconds since epoch 0)
    private long systemTimeMillis = 0L;

    // Timestamp metadata storage for all state types:
    // partitionKey -> (stateArgumentName -> TimestampMetadata)
    // - ValueTimestamp for value state (POJOs, Row)
    // - ListTimestamps for ListView state
    // - MapTimestamps for MapView state
    // Stored separately from RowData to avoid caching complexity
    private final Map<Row, Map<String, TimestampMetadata>> timestampsByPartition = new HashMap<>();

    private ProcessTableFunctionTestHarness(
            ProcessTableFunction<OUT> function,
            FunctionContext functionContext,
            String defaultTableArgument,
            java.lang.reflect.Method evalMethod,
            List<ArgumentInfo> arguments,
            Map<String, ArgumentInfo> argumentsByName,
            boolean isSingleTableFunction,
            Map<String, Object> scalarArgumentValues,
            Map<String, DataStructureConverter<Object, Object>> inputConverters,
            Map<String, DataStructureConverter<Object, Object>> outputConverters,
            Map<String, DataStructureConverter<Object, Object>> stateConverters)
            throws Exception {
        this.function = function;
        this.functionContext = functionContext;
        this.defaultTableArgument = defaultTableArgument;
        this.evalMethod = evalMethod;
        this.arguments = arguments;
        this.argumentsByName = argumentsByName;
        this.isSingleTableFunction = isSingleTableFunction;
        this.scalarArgumentValues = scalarArgumentValues;
        this.inputConverters = inputConverters;
        this.outputConverters = outputConverters;
        this.stateConverters = stateConverters;
        this.stateByPartition = new HashMap<>();
        this.output = new ArrayList<>();
        this.collector = new HarnessCollector();
        this.isOpen = false;

        openFunction();
    }

    /** Creates a new harness builder for the given ProcessTableFunction class. */
    public static <OUT> Builder<OUT> ofClass(
            Class<? extends ProcessTableFunction<OUT>> functionClass) {
        return new Builder<>(functionClass);
    }

    private void openFunction() throws Exception {
        function.open(functionContext);
        function.setCollector(collector);
        isOpen = true;
    }

    @Override
    public void close() throws Exception {
        if (isOpen) {
            function.close();
            isOpen = false;
        }
    }

    /**
     * Process a single element for the default table argument.
     *
     * <p>For PTFs with a single table argument, this processes one row. For multiple table
     * arguments, use {@link #processElementForTable(String, Row)}.
     */
    public void processElement(Row row) throws Exception {
        if (!isSingleTableFunction) {
            throw new IllegalStateException(
                    "PTF has multiple table arguments. Use processElementForTable(argumentName, row) "
                            + "to specify which table argument should receive the row.");
        }

        processElementForTable(defaultTableArgument, row);
    }

    /** Process a single element constructed from values. */
    public void processElement(Object... values) throws Exception {
        processElement(Row.of(values));
    }

    /** Process a single element with a specific RowKind. */
    public void processElement(RowKind rowKind, Object... values) throws Exception {
        processElement(Row.ofKind(rowKind, values));
    }

    /** Process a single element for a specific table argument. */
    public void processElementForTable(String tableArgument, Row row) throws Exception {
        checkState(isOpen, "Harness not open");
        checkNotNull(tableArgument, "tableArgument must not be null");

        // Try named arguments first
        ArgumentInfo arg = argumentsByName.get(tableArgument);
        if (arg == null) {
            throw new IllegalArgumentException("Unknown table argument: " + tableArgument);
        }
        if (!(arg instanceof TableArgumentInfo)) {
            throw new IllegalArgumentException(
                    "Argument '" + tableArgument + "' is not a table argument");
        }
        invokeEval((TableArgumentInfo) arg, row);
    }

    /** Process a single element for a specific table argument. */
    public void processElementForTable(String tableArgument, Object... values) throws Exception {
        processElementForTable(tableArgument, Row.of(values));
    }

    /** Process a single element for a specific table argument with RowKind. */
    public void processElementForTable(String tableArgument, RowKind rowKind, Object... values)
            throws Exception {
        processElementForTable(tableArgument, Row.ofKind(rowKind, values));
    }

    /**
     * Invokes the PTF's eval() method with scalar arguments only.
     *
     * <p>This method is specifically for scalar-only PTFs (PTFs with only scalar arguments and no
     * table arguments). For PTFs that accept table arguments, use {@link #processElement(Row)} or
     * {@link #processElementForTable(String, Row)} instead.
     *
     * @throws IllegalStateException if the PTF has any table arguments
     * @throws Exception if the eval() invocation fails
     */
    public void invoke() throws Exception {
        checkState(isOpen, "Harness not open");

        // Validate this is a scalar-only PTF
        boolean hasTableArguments =
                arguments.stream().anyMatch(arg -> arg instanceof TableArgumentInfo);
        if (hasTableArguments) {
            throw new IllegalStateException(
                    "invoke() is only for scalar-only PTFs. This PTF has table arguments. "
                            + "Use processElement() or processElementForTable() instead.");
        }

        // Clear collector context since there's no active table argument
        collector.setContext(null, null);

        // Build arguments array with only scalar values
        Object[] args = new Object[arguments.size()];
        for (int i = 0; i < arguments.size(); i++) {
            ArgumentInfo arg = arguments.get(i);
            if (arg instanceof ScalarArgumentInfo) {
                args[i] = scalarArgumentValues.get(arg.name);
            } else {
                throw new IllegalStateException(
                        "Unexpected non-scalar argument at position " + i + ": " + arg.name);
            }
        }

        // Invoke eval() method
        try {
            evalMethod.invoke(function, args);
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                Exception userException = (Exception) cause;
                userException.addSuppressed(
                        new Exception(
                                String.format(
                                        "Exception occurred during scalar-only PTF eval() invocation. "
                                                + "Scalar arguments: %s",
                                        scalarArgumentValues)));
                throw userException;
            } else {
                throw new RuntimeException("Error invoking PTF eval() method", e);
            }
        }
    }

    /** Returns all collected output rows. */
    public List<OUT> getOutput() {
        return List.copyOf(output);
    }

    /** Clears all collected output. */
    public void clearOutput() {
        output.clear();
    }

    // -------------------------------------------------------------------------
    // State Introspection
    // -------------------------------------------------------------------------

    /**
     * Gets the state for a particular state argument and partition key.
     *
     * @param stateArgument the name of the state argument
     * @param key the partition key as a Row
     * @param stateClass the class of the state object
     * @return the state object converted to external format
     * @throws Exception if state cannot be retrieved or converted
     */
    public <S> S getStateForKey(String stateArgument, Row key, Class<S> stateClass)
            throws Exception {
        StateArgumentInfo stateInfo = findStateArgument(stateArgument);

        Map<String, RowData> stateMap = stateByPartition.get(key);
        if (stateMap == null) {
            return null;
        }

        RowData stateRowData = stateMap.get(stateInfo.name);

        DataStructureConverter<Object, Object> converter = stateConverters.get(stateInfo.name);
        @SuppressWarnings("unchecked")
        S result = (S) converter.toExternalOrNull(stateRowData);
        return result;
    }

    /**
     * Sets the state for a specific state argument and partition key.
     *
     * @param stateArgument the name of the state argument
     * @param key the partition key as a Row
     * @param state the state object in external format
     * @throws Exception if state cannot be set or converted
     */
    public <S> void setStateForKey(String stateArgument, Row key, S state) throws Exception {
        StateArgumentInfo stateInfo = findStateArgument(stateArgument);

        Map<String, RowData> stateMap =
                stateByPartition.computeIfAbsent(key, k -> createFreshState());

        DataStructureConverter<Object, Object> converter = stateConverters.get(stateArgument);
        RowData stateRowData = (RowData) converter.toInternalOrNull(state);

        stateMap.put(stateArgument, stateRowData);

        // Create timestamp metadata for ListView/MapView state
        // All elements/entries get currentTimeMillis since they're freshly set
        if (stateInfo.ttl != null) {
            if (state instanceof org.apache.flink.table.api.dataview.ListView) {
                @SuppressWarnings({"unchecked", "rawtypes"})
                org.apache.flink.table.api.dataview.ListView listView =
                        (org.apache.flink.table.api.dataview.ListView) state;
                try {
                    Iterable<?> elements = listView.get();
                    java.util.List<Long> timestamps = new java.util.ArrayList<>();
                    for (Object ignored : elements) {
                        timestamps.add(systemTimeMillis);
                    }
                    timestampsByPartition
                            .computeIfAbsent(key, k -> new HashMap<>())
                            .put(stateArgument, new ListTimestamps(timestamps));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to extract ListView elements", e);
                }
            } else if (state instanceof org.apache.flink.table.api.dataview.MapView) {
                @SuppressWarnings({"unchecked", "rawtypes"})
                org.apache.flink.table.api.dataview.MapView mapView =
                        (org.apache.flink.table.api.dataview.MapView) state;
                java.util.Map<?, ?> map = mapView.getMap();
                java.util.Map<Object, Long> timestamps = new HashMap<>();
                for (Object key2 : map.keySet()) {
                    timestamps.put(key2, systemTimeMillis);
                }
                timestampsByPartition
                        .computeIfAbsent(key, k -> new HashMap<>())
                        .put(stateArgument, new MapTimestamps(timestamps));
            } else {
                // Value state - create ValueTimestamp wrapper
                timestampsByPartition
                        .computeIfAbsent(key, k -> new HashMap<>())
                        .put(stateArgument, new ValueTimestamp(systemTimeMillis));
            }
        }
    }

    /**
     * Gets all partition keys that have state for a state argument.
     *
     * @param stateArgument the name of the state argument
     * @return set of all partition keys as Rows
     * @throws Exception if keys cannot be retrieved
     */
    public Set<Row> getStateKeys(String stateArgument) throws Exception {
        StateArgumentInfo stateInfo = findStateArgument(stateArgument);

        Set<Row> keys = new java.util.HashSet<>();
        for (Map.Entry<Row, Map<String, RowData>> entry : stateByPartition.entrySet()) {
            RowData stateRowData = entry.getValue().get(stateInfo.name);
            if (stateRowData != null) {
                keys.add(entry.getKey());
            }
        }
        return keys;
    }

    /**
     * Gets state for all partitions for a state argument.
     *
     * @param stateArgument the name of the state argument
     * @param stateClass the class of the state object
     * @return map of partition keys to state objects in external format
     * @throws Exception if state cannot be retrieved
     */
    public <S> Map<Row, S> getAllState(String stateArgument, Class<S> stateClass) throws Exception {
        StateArgumentInfo stateInfo = findStateArgument(stateArgument);

        DataStructureConverter<Object, Object> converter = stateConverters.get(stateInfo.name);

        Map<Row, S> result = new java.util.HashMap<>();
        for (Map.Entry<Row, Map<String, RowData>> entry : stateByPartition.entrySet()) {
            Row key = entry.getKey();
            RowData stateRowData = entry.getValue().get(stateInfo.name);
            @SuppressWarnings("unchecked")
            S state = (S) converter.toExternalOrNull(stateRowData);
            result.put(key, state);
        }
        return result;
    }

    /**
     * Clears state for the partition key and argument.
     *
     * @param stateArgument the name of the state argument
     * @param key the partition key as a Row
     * @throws Exception if state cannot be cleared
     */
    public void clearStateForKey(String stateArgument, Row key) throws Exception {
        StateArgumentInfo stateInfo = findStateArgument(stateArgument);

        Map<String, RowData> stateMap = stateByPartition.get(key);
        if (stateMap == null) {
            return;
        }

        Object freshExternalState;
        if (Row.class.isAssignableFrom(stateInfo.stateClass)) {
            int fieldCount = stateInfo.dataType.getChildren().size();
            Object[] fields = new Object[fieldCount];
            freshExternalState = Row.of(fields);
        } else {
            try {
                freshExternalState = stateInfo.stateClass.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to create fresh state for " + stateArgument, e);
            }
        }

        DataStructureConverter<Object, Object> converter = stateConverters.get(stateArgument);
        RowData freshRowData = (RowData) converter.toInternalOrNull(freshExternalState);
        stateMap.put(stateArgument, freshRowData);
    }

    /**
     * Clears state for the state argument across all partitions.
     *
     * @param stateArgument the name of the state argument
     * @throws Exception if state cannot be cleared
     */
    public void clearState(String stateArgument) throws Exception {
        // Clear the specified state argument for all partitions
        // We iterate through all partitions and clear just that one state (keys are already Rows)
        for (Row partitionKey : stateByPartition.keySet()) {
            clearStateForKey(stateArgument, partitionKey);
        }
    }

    /**
     * Clears all state for all arguments and partitions.
     *
     * @throws Exception if state cannot be cleared
     */
    public void clearAllState() throws Exception {
        stateByPartition.clear();
        timestampsByPartition.clear();
    }

    // -------------------------------------------------------------------------
    // Time Control (for State TTL Testing)
    // -------------------------------------------------------------------------

    /**
     * Advances the system clock by the given number of milliseconds. Evacuates state with TTL that
     * has exceeded its time-to-live.
     *
     * @param millis The number of milliseconds to advance (must be non-negative)
     * @throws Exception if state evacuation fails
     */
    public void advanceSystemClock(long millis) throws Exception {
        if (millis < 0) {
            throw new IllegalArgumentException(
                    "Cannot advance system clock by negative amount: " + millis);
        }
        systemTimeMillis += millis;
        evacuateExpiredState();
    }

    /**
     * Advances the system clock to the given Instant. Evacuates state with TTL that has exceeded
     * its time-to-live.
     *
     * @param instant The target time
     * @throws Exception if state evacuation fails
     */
    public void advanceSystemClock(Instant instant) throws Exception {
        checkNotNull(instant, "instant must not be null");
        long newTimeMillis = instant.toEpochMilli();
        if (newTimeMillis < systemTimeMillis) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot move system clock backward from %d to %d",
                            systemTimeMillis, newTimeMillis));
        }
        systemTimeMillis = newTimeMillis;
        evacuateExpiredState();
    }

    /**
     * Advances the system clock to the given LocalDateTime. Assumes system default timezone for
     * conversion to milliseconds. Evacuates state with TTL that has exceeded its time-to-live.
     *
     * @param localDateTime The target time
     * @throws Exception if state evacuation fails
     */
    public void advanceSystemClock(LocalDateTime localDateTime) throws Exception {
        checkNotNull(localDateTime, "localDateTime must not be null");
        long newTimeMillis =
                localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        if (newTimeMillis < systemTimeMillis) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot move system clock backward from %d to %d",
                            systemTimeMillis, newTimeMillis));
        }
        systemTimeMillis = newTimeMillis;
        evacuateExpiredState();
    }

    private void evacuateExpiredState() throws Exception {
        for (Map.Entry<Row, Map<String, RowData>> partitionEntry : stateByPartition.entrySet()) {
            Row partitionKey = partitionEntry.getKey();
            Map<String, RowData> stateMap = partitionEntry.getValue();

            for (ArgumentInfo arg : arguments) {
                if (!(arg instanceof StateArgumentInfo)) {
                    continue;
                }

                StateArgumentInfo stateArg = (StateArgumentInfo) arg;
                if (stateArg.ttl == null) {
                    continue;
                }

                RowData stateRowData = stateMap.get(stateArg.name);

                if (stateRowData == null) {
                    continue;
                }

                DataStructureConverter<Object, Object> converter =
                        stateConverters.get(stateArg.name);

                Map<String, TimestampMetadata> timestampMap =
                        timestampsByPartition.get(partitionKey);
                TimestampMetadata metadata =
                        (timestampMap != null) ? timestampMap.get(stateArg.name) : null;

                if (metadata instanceof ListTimestamps || metadata instanceof MapTimestamps) {
                    Object externalState = converter.toExternalOrNull(stateRowData);

                    injectTimestampsIntoView(externalState, partitionKey, stateArg.name);

                    if (externalState instanceof TTLAwareDataView) {
                        ((TTLAwareDataView) externalState).evacuateExpiredEntries(stateArg.ttl);
                    }

                    RowData updatedRowData = (RowData) converter.toInternalOrNull(externalState);
                    stateMap.put(stateArg.name, updatedRowData);

                    extractTimestampsFromView(externalState, partitionKey, stateArg.name);

                } else if (metadata instanceof ValueTimestamp) {
                    long lastUpdateTime = ((ValueTimestamp) metadata).timestamp;
                    long expirationTime = lastUpdateTime + stateArg.ttl.toMillis();
                    if (systemTimeMillis >= expirationTime) {
                        stateMap.put(stateArg.name, null);
                        timestampMap.remove(stateArg.name);
                    }
                }
            }

            boolean allNull = stateMap.values().stream().allMatch(rowData -> rowData == null);
            if (allNull) {
                stateByPartition.remove(partitionKey);
                timestampsByPartition.remove(partitionKey);
            }
        }

        timestampsByPartition.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }

    // -------------------------------------------------------------------------
    // State Introspection Helper Methods
    // -------------------------------------------------------------------------

    /**
     * Injects timestamps into a TestListView or TestMapView instance.
     *
     * @param externalState the state object (TestListView or TestMapView)
     * @param partitionKey the partition key
     * @param stateName the state argument name
     */
    private void injectTimestampsIntoView(
            Object externalState, Row partitionKey, String stateName) {
        if (externalState instanceof TestListView) {
            TestListView<?> testListView = (TestListView<?>) externalState;
            testListView.setCurrentTime(systemTimeMillis);

            // Inject timestamps from metadata storage
            Map<String, TimestampMetadata> timestampMap = timestampsByPartition.get(partitionKey);
            if (timestampMap != null) {
                TimestampMetadata metadata = timestampMap.get(stateName);
                if (metadata instanceof ListTimestamps) {
                    testListView.injectTimestamps(((ListTimestamps) metadata).elementTimestamps);
                }
            }
        } else if (externalState instanceof TestMapView) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            TestMapView testMapView = (TestMapView) externalState;
            testMapView.setCurrentTime(systemTimeMillis);

            // Inject timestamps from metadata storage
            Map<String, TimestampMetadata> timestampMap = timestampsByPartition.get(partitionKey);
            if (timestampMap != null) {
                TimestampMetadata metadata = timestampMap.get(stateName);
                if (metadata instanceof MapTimestamps) {
                    testMapView.injectTimestamps(((MapTimestamps) metadata).entryTimestamps);
                }
            }
        }
    }

    /**
     * Extracts timestamps from a TestListView or TestMapView and stores in metadata.
     *
     * @param externalState the state object (TestListView or TestMapView)
     * @param partitionKey the partition key
     * @param stateName the state argument name
     */
    private void extractTimestampsFromView(
            Object externalState, Row partitionKey, String stateName) {
        if (externalState instanceof TestListView) {
            TestListView<?> testListView = (TestListView<?>) externalState;
            java.util.List<Long> timestamps = testListView.extractTimestamps();
            timestampsByPartition
                    .computeIfAbsent(partitionKey, k -> new HashMap<>())
                    .put(stateName, new ListTimestamps(timestamps));
        } else if (externalState instanceof TestMapView) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            TestMapView testMapView = (TestMapView) externalState;
            java.util.Map<Object, Long> timestamps = testMapView.extractTimestamps();
            timestampsByPartition
                    .computeIfAbsent(partitionKey, k -> new HashMap<>())
                    .put(stateName, new MapTimestamps(timestamps));
        }
    }

    private StateArgumentInfo findStateArgument(String stateArgument) {
        for (ArgumentInfo arg : arguments) {
            if (arg instanceof StateArgumentInfo && arg.name.equals(stateArgument)) {
                return (StateArgumentInfo) arg;
            }
        }
        throw new IllegalArgumentException(
                "State argument '" + stateArgument + "' not found in PTF signature");
    }

    /**
     * Computes the partition key for a given row based on the active table argument's partition
     * columns.
     *
     * <p>For non-partitioned tables or ROW_SEMANTIC_TABLE, returns an empty Row. For partitioned
     * tables, returns a Row containing the partition column values.
     */
    private Row computePartitionKey(TableArgumentInfo tableArg, Row row) {
        if (tableArg.partitionColumnNames == null || tableArg.partitionColumnNames.length == 0) {
            // No partitioning - return empty Row
            return Row.of();
        }

        // Extract partition values into a Row
        Object[] keyValues = new Object[tableArg.partitionColumnNames.length];
        for (int i = 0; i < tableArg.partitionColumnNames.length; i++) {
            int fieldIndex = getFieldIndex(tableArg.dataType, tableArg.partitionColumnNames[i]);
            keyValues[i] = row.getField(fieldIndex);
        }

        return Row.of(keyValues);
    }

    /**
     * Gets the field index for a given field name within a DataType's row structure.
     *
     * @throws IllegalStateException if the field name is not found
     */
    private int getFieldIndex(DataType dataType, String fieldName) {
        org.apache.flink.table.types.logical.RowType rowType =
                (org.apache.flink.table.types.logical.RowType) dataType.getLogicalType();
        int index = 0;
        for (org.apache.flink.table.types.logical.RowType.RowField field : rowType.getFields()) {
            if (field.getName().equals(fieldName)) {
                return index;
            }
            index++;
        }
        throw new IllegalStateException(
                String.format("Field '%s' not found in data type %s", fieldName, dataType));
    }

    /**
     * Gets existing state for a partition or creates fresh state if this is the first access.
     *
     * <p>State objects are created as new instances with all fields set to null/default values.
     */
    private Map<String, RowData> getOrCreateState(Row partitionKey) {
        Map<String, RowData> stateMap = stateByPartition.get(partitionKey);
        if (stateMap == null) {
            stateMap = createFreshState();
            stateByPartition.put(partitionKey, stateMap);

            // Initialize timestamps for value state with TTL
            for (ArgumentInfo arg : arguments) {
                if (arg instanceof StateArgumentInfo) {
                    StateArgumentInfo stateArg = (StateArgumentInfo) arg;
                    if (stateArg.ttl != null
                            && !org.apache.flink.table.api.dataview.ListView.class.isAssignableFrom(
                                    stateArg.stateClass)
                            && !org.apache.flink.table.api.dataview.MapView.class.isAssignableFrom(
                                    stateArg.stateClass)) {
                        timestampsByPartition
                                .computeIfAbsent(partitionKey, k -> new HashMap<>())
                                .put(stateArg.name, new ValueTimestamp(systemTimeMillis));
                    }
                }
            }
        }
        return stateMap;
    }

    /**
     * Creates fresh state objects for all state parameters.
     *
     * <p>Creates external state objects (POJOs or Rows), then converts them to internal RowData
     * representation for storage.
     */
    private Map<String, RowData> createFreshState() {
        Map<String, RowData> stateMap = new HashMap<>();
        for (ArgumentInfo arg : arguments) {
            if (arg instanceof StateArgumentInfo) {
                StateArgumentInfo stateArg = (StateArgumentInfo) arg;
                try {
                    // Create fresh external state object
                    Object externalState;
                    if (Row.class.isAssignableFrom(stateArg.stateClass)) {
                        // Create empty Row with null fields
                        int fieldCount = stateArg.dataType.getChildren().size();
                        Object[] fields = new Object[fieldCount];
                        externalState = Row.of(fields);
                    } else {
                        // Create POJO instance using default constructor
                        externalState = stateArg.stateClass.getDeclaredConstructor().newInstance();
                    }

                    // Convert to internal RowData for storage
                    DataStructureConverter<Object, Object> converter =
                            stateConverters.get(stateArg.name);
                    RowData rowData = (RowData) converter.toInternalOrNull(externalState);
                    stateMap.put(stateArg.name, rowData);
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to create state instance for " + stateArg.name, e);
                }
            }
        }
        return stateMap;
    }

    /**
     * Given a target table argument and a row to process, construct the right set of arguments for
     * the PTF's eval function and attempt to invoke it.
     */
    private void invokeEval(TableArgumentInfo activeTableArg, Row activeRow) throws Exception {
        // Set collector context so it can prepend columns if needed
        collector.setContext(activeTableArg, activeRow);

        // Compute partition key for state lookup
        Row partitionKey = computePartitionKey(activeTableArg, activeRow);

        // Get or create state for this partition (stored as RowData internally)
        Map<String, RowData> stateMap = getOrCreateState(partitionKey);

        // Build full arguments array in eval() signature order
        Object[] args = new Object[arguments.size()];

        // Track state arguments and their converters for post-eval conversion
        List<StateArgumentWithConverter> stateArgumentsForConversion = new ArrayList<>();

        for (int i = 0; i < arguments.size(); i++) {
            ArgumentInfo arg = arguments.get(i);

            if (arg instanceof StateArgumentInfo) {
                StateArgumentInfo stateArg = (StateArgumentInfo) arg;
                DataStructureConverter<Object, Object> converter =
                        stateConverters.get(stateArg.name);
                RowData rowData = stateMap.get(stateArg.name);

                Object externalState = converter.toExternalOrNull(rowData);

                injectTimestampsIntoView(externalState, partitionKey, stateArg.name);

                args[i] = externalState;

                stateArgumentsForConversion.add(
                        new StateArgumentWithConverter(i, stateArg, converter));

            } else if (arg instanceof TableArgumentInfo) {
                TableArgumentInfo tableArg = (TableArgumentInfo) arg;
                if (tableArg.name.equals(activeTableArg.name)) {
                    // Active table argument: convert input row to expected type
                    // First convert to internal RowData, then to external type (Row or POJO)
                    DataStructureConverter<Object, Object> inputConverter =
                            inputConverters.get(tableArg.name);
                    DataStructureConverter<Object, Object> outputConverter =
                            outputConverters.get(tableArg.name);

                    args[i] =
                            outputConverter.toExternalOrNull(
                                    inputConverter.toInternalOrNull(activeRow));
                } else {
                    // Inactive table argument: pass null
                    args[i] = null;
                }

            } else if (arg instanceof ScalarArgumentInfo) {
                // Scalar arguments: pull from pre-configured values
                args[i] = scalarArgumentValues.get(arg.name);

            } else {
                throw new IllegalStateException(
                        "Unexpected argument type at position " + i + ": " + arg.getClass());
            }
        }

        try {
            evalMethod.invoke(function, args);
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                Exception userException = (Exception) cause;
                String partitionInfo =
                        activeTableArg.partitionColumnNames != null
                                        && activeTableArg.partitionColumnNames.length > 0
                                ? String.format(
                                        ", partition columns: %s",
                                        java.util.Arrays.toString(
                                                activeTableArg.partitionColumnNames))
                                : ", no partitioning";
                userException.addSuppressed(
                        new Exception(
                                String.format(
                                        "Exception occurred during PTF eval() while processing table argument '%s'%s. "
                                                + "Input row: %s",
                                        activeTableArg.name, partitionInfo, activeRow)));
                throw userException;
            } else {
                throw new RuntimeException("Error invoking PTF eval() method", e);
            }
        }

        // After eval returns, convert mutated state objects back to RowData for storage
        for (StateArgumentWithConverter stateArg : stateArgumentsForConversion) {
            Object externalState = args[stateArg.argIndex];
            RowData rowData = (RowData) stateArg.converter.toInternalOrNull(externalState);
            stateMap.put(stateArg.stateArg.name, rowData);
        }

        // Extract timestamps from TestListView/TestMapView after eval() and update value state
        // timestamps
        for (StateArgumentWithConverter stateArg : stateArgumentsForConversion) {
            Object externalState = args[stateArg.argIndex];

            if (stateArg.stateArg.ttl != null) {
                if (externalState instanceof TestListView || externalState instanceof TestMapView) {
                    // Extract timestamps from TestListView/TestMapView
                    extractTimestampsFromView(externalState, partitionKey, stateArg.stateArg.name);
                } else {
                    // Value state - create ValueTimestamp wrapper
                    timestampsByPartition
                            .computeIfAbsent(partitionKey, k -> new HashMap<>())
                            .put(stateArg.stateArg.name, new ValueTimestamp(systemTimeMillis));
                }
            }
        }
    }

    /** Helper class to track state arguments and their converters during eval() invocation. */
    private static class StateArgumentWithConverter {
        final int argIndex;
        final StateArgumentInfo stateArg;
        final DataStructureConverter<Object, Object> converter;

        StateArgumentWithConverter(
                int argIndex,
                StateArgumentInfo stateArg,
                DataStructureConverter<Object, Object> converter) {
            this.argIndex = argIndex;
            this.stateArg = stateArg;
            this.converter = converter;
        }
    }

    /**
     * Collector implementation that stores output in the harness.
     *
     * <p>For SET_SEMANTIC_TABLE arguments, automatically prepends partition key columns to the PTF
     * output. If the argument has PASS_COLUMNS_THROUGH trait, prepends all input columns.
     */
    private class HarnessCollector implements Collector<OUT> {
        // Context set before each eval() invocation
        private TableArgumentInfo activeTableArg;
        private Row activeRow;

        void setContext(TableArgumentInfo tableArg, Row row) {
            this.activeTableArg = tableArg;
            this.activeRow = row;
        }

        @Override
        public void collect(OUT record) {
            if (activeTableArg == null) {
                // No active table argument - just collect as-is
                output.add(record);
                return;
            }

            // Determine which columns to prepend
            if (activeTableArg.hasPassColumnsThrough) {
                // PASS_COLUMNS_THROUGH: Prepend ALL input columns
                output.add(prependAllColumns(record));
            } else if (activeTableArg.isSetSemantic()
                    && activeTableArg.partitionColumnNames != null) {
                // SET_SEMANTIC_TABLE: Prepend partition key columns only
                output.add(prependPartitionKeys(record));
            } else {
                // ROW_SEMANTIC_TABLE or no partitioning: no prepending
                output.add(record);
            }
        }

        @SuppressWarnings("unchecked")
        private OUT prependPartitionKeys(OUT ptfOutput) {
            if (!(ptfOutput instanceof Row)) {
                throw new IllegalStateException(
                        "Cannot prepend partition keys to non-Row output type: "
                                + ptfOutput.getClass());
            }

            Row ptfRow = (Row) ptfOutput;

            // For multi-table PTFs, prepend partition keys from ALL SET_SEMANTIC_TABLE arguments
            // Active table contributes actual partition key values, inactive tables contribute
            // nulls
            int totalPartitionKeyCount = 0;
            for (ArgumentInfo arg : arguments) {
                if (arg instanceof TableArgumentInfo) {
                    TableArgumentInfo tableArg = (TableArgumentInfo) arg;
                    if (tableArg.isSetSemantic() && tableArg.partitionColumnNames != null) {
                        totalPartitionKeyCount += tableArg.partitionColumnNames.length;
                    }
                }
            }

            int ptfOutputArity = ptfRow.getArity();
            int totalArity = totalPartitionKeyCount + ptfOutputArity;

            Row result = new Row(ptfRow.getKind(), totalArity);

            // Prepend partition key values from all SET_SEMANTIC_TABLE arguments
            int resultIndex = 0;
            for (ArgumentInfo arg : arguments) {
                if (arg instanceof TableArgumentInfo) {
                    TableArgumentInfo tableArg = (TableArgumentInfo) arg;
                    if (tableArg.isSetSemantic() && tableArg.partitionColumnNames != null) {
                        // Check if this is the active table
                        boolean isActive = tableArg.name.equals(activeTableArg.name);

                        for (String columnName : tableArg.partitionColumnNames) {
                            if (isActive) {
                                // Active table: extract partition key value from input row
                                // Convert column name to position index
                                int columnIndex = getFieldIndex(tableArg.dataType, columnName);
                                result.setField(resultIndex++, activeRow.getField(columnIndex));
                            } else {
                                // Inactive table: use null
                                result.setField(resultIndex++, null);
                            }
                        }
                    }
                }
            }

            // Append PTF output
            for (int i = 0; i < ptfOutputArity; i++) {
                result.setField(resultIndex++, ptfRow.getField(i));
            }

            return (OUT) result;
        }

        /** Helper to get field index by name from a DataType. */
        private int getFieldIndex(DataType dataType, String fieldName) {
            org.apache.flink.table.types.logical.RowType rowType =
                    (org.apache.flink.table.types.logical.RowType) dataType.getLogicalType();
            int index = 0;
            for (org.apache.flink.table.types.logical.RowType.RowField field :
                    rowType.getFields()) {
                if (field.getName().equals(fieldName)) {
                    return index;
                }
                index++;
            }
            throw new IllegalStateException(
                    String.format("Field '%s' not found in type %s", fieldName, dataType));
        }

        @SuppressWarnings("unchecked")
        private OUT prependAllColumns(OUT ptfOutput) {
            if (!(ptfOutput instanceof Row)) {
                throw new IllegalStateException(
                        "Cannot prepend columns to non-Row output type: " + ptfOutput.getClass());
            }

            Row ptfRow = (Row) ptfOutput;
            int inputArity = activeRow.getArity();
            int ptfOutputArity = ptfRow.getArity();
            int totalArity = inputArity + ptfOutputArity;

            Row result = new Row(ptfRow.getKind(), totalArity);

            // Prepend ALL input columns
            for (int i = 0; i < inputArity; i++) {
                result.setField(i, activeRow.getField(i));
            }

            // Append PTF output
            for (int i = 0; i < ptfOutputArity; i++) {
                result.setField(inputArity + i, ptfRow.getField(i));
            }

            return (OUT) result;
        }

        @Override
        public void close() {}
    }

    /**
     * Builder for {@link ProcessTableFunctionTestHarness}.
     *
     * @param <OUT> The output type of the ProcessTableFunction
     */
    @PublicEvolving
    public static class Builder<OUT> {
        private final Class<? extends ProcessTableFunction<OUT>> functionClass;

        // Position counter for positional-only arguments (those without @ArgumentHint names)
        // Tracks eval() signature position for consistent dual-lookup
        private int nextPosition = 0;
        private final LinkedHashMap<String, ScalarArgumentConfiguration> scalarArgs =
                new LinkedHashMap<>();
        private final LinkedHashMap<String, TableArgumentConfiguration> tableArgs =
                new LinkedHashMap<>();
        private final Map<String, PartitionConfiguration> partitionConfigs = new HashMap<>();

        // Initial state: stateArgumentName -> (partitionKey -> stateValue)
        private final Map<String, Map<Row, Object>> initialState = new HashMap<>();

        private Builder(Class<? extends ProcessTableFunction<OUT>> functionClass) {
            this.functionClass = checkNotNull(functionClass, "functionClass must not be null");
        }

        // ---------------------------------------------------------------------
        // Table & Scalar Arguments
        // ---------------------------------------------------------------------

        /**
         * Configures a table argument with its schema (named argument).
         *
         * <p>Use this for dynamic tables that receive elements during the test. Elements are
         * provided via {@link #processElement(Row)} or {@link #processElementForTable(String,
         * Row)}.
         *
         * @param argumentName The table argument name
         * @param dataType The schema/structure of the table
         */
        public Builder<OUT> withTableArgument(String argumentName, AbstractDataType<?> dataType) {
            checkNotNull(argumentName, "argumentName must not be null");
            checkNotNull(dataType, "dataType must not be null");

            if (scalarArgs.containsKey(argumentName)) {
                throw new IllegalArgumentException(
                        "Argument already configured as scalar: " + argumentName);
            }

            if (tableArgs.containsKey(argumentName)) {
                throw new IllegalArgumentException(
                        "Table argument already configured: " + argumentName);
            }

            TableArgumentConfiguration config = new TableArgumentConfiguration(argumentName);
            config.explicitType = dataType;
            tableArgs.put(argumentName, config);
            return this;
        }

        /**
         * Configures a scalar (non-table) argument for the PTF's eval() method.
         *
         * <p>Scalar arguments are constant values passed to every eval() invocation, such as
         * thresholds, multipliers, or configuration parameters.
         *
         * @param argumentName Must match the parameter name in eval() or the @ArgumentHint name
         * @param value The value to pass for this argument in all eval() calls
         */
        public Builder<OUT> withScalarArgument(String argumentName, Object value) {
            checkNotNull(argumentName, "argumentName must not be null");
            checkNotNull(value, "value must not be null");

            if (scalarArgs.containsKey(argumentName) || tableArgs.containsKey(argumentName)) {
                throw new IllegalArgumentException("Argument already configured: " + argumentName);
            }

            ScalarArgumentConfiguration config =
                    new ScalarArgumentConfiguration(argumentName, value);
            scalarArgs.put(argumentName, config);
            return this;
        }

        // ---------------------------------------------------------------------
        // Partitioning
        // ---------------------------------------------------------------------

        /**
         * Specifies partition columns for a set semantic table.
         *
         * @param argumentName The table argument name
         * @param columnNames The partition column names
         * @return This builder
         */
        public Builder<OUT> withPartitionBy(String argumentName, String... columnNames) {
            checkNotNull(argumentName, "argumentName must not be null");
            checkNotNull(columnNames, "columnNames must not be null");
            checkArgument(columnNames.length > 0, "Must specify at least one column");

            if (partitionConfigs.containsKey(argumentName)) {
                throw new IllegalArgumentException(
                        "Partition config already exists for: " + argumentName);
            }

            PartitionConfiguration config = new PartitionConfiguration(argumentName, columnNames);
            partitionConfigs.put(argumentName, config);
            return this;
        }

        // ---------------------------------------------------------------------
        // State Initialization
        // ---------------------------------------------------------------------

        /**
         * Sets initial state for a specific state argument and partition key.
         *
         * <p>This is useful for testing recovery scenarios or resuming from checkpoints.
         *
         * @param stateArgument The state argument name (from eval() parameters)
         * @param key The partition key
         * @param state The initial state value (POJO, ListView, MapView, etc.)
         * @return This builder
         */
        public <K, S> Builder<OUT> withInitialStateArgument(String stateArgument, K key, S state) {
            checkNotNull(stateArgument, "stateArgument must not be null");
            checkNotNull(key, "key must not be null");
            checkNotNull(state, "state must not be null");

            // Convert key to Row if it isn't already
            Row partitionKey;
            if (key instanceof Row) {
                partitionKey = (Row) key;
            } else {
                partitionKey = Row.of(key);
            }

            initialState
                    .computeIfAbsent(stateArgument, k -> new HashMap<>())
                    .put(partitionKey, state);
            return this;
        }

        // ---------------------------------------------------------------------
        // Build
        // ---------------------------------------------------------------------

        /**
         * Builds the test harness.
         *
         * <p>This instantiates the PTF, validates configuration via type inference, creates the
         * FunctionContext, and opens the function.
         *
         * @return The configured test harness
         * @throws Exception If instantiation or opening fails
         */
        public ProcessTableFunctionTestHarness<OUT> build() throws Exception {
            ProcessTableFunction<OUT> function = instantiateFunction();

            java.lang.reflect.Method evalMethod = findEvalMethod();

            List<ArgumentInfo> arguments = extractAndValidateTypeInference(function, evalMethod);

            FunctionContext functionContext =
                    new FunctionContext(null, Thread.currentThread().getContextClassLoader(), null);

            // Validate that the eval method does not have currently unsupported arguments
            validateEvalMethodSupported(evalMethod, arguments);

            // Validate partition consistency for multi-table PTFs
            validatePartitionConsistency(arguments);

            // In cases where PTFs have only a single table argument, set it as a default
            // and mark as a single table function so that processElement without specifying
            // a table argument can be used.
            String defaultTableArg = null;
            boolean isSingleTableFunction = false;

            // Count table arguments from actual signature (not just builder config)
            List<TableArgumentInfo> tableArguments = new ArrayList<>();
            for (ArgumentInfo arg : arguments) {
                if (arg instanceof TableArgumentInfo) {
                    tableArguments.add((TableArgumentInfo) arg);
                }
            }

            if (tableArguments.size() == 1) {
                defaultTableArg = tableArguments.get(0).name;
                isSingleTableFunction = true;
            }

            // Create input and output converters for table arguments.
            Map<String, DataStructureConverter<Object, Object>> inputConverters = new HashMap<>();
            Map<String, DataStructureConverter<Object, Object>> outputConverters = new HashMap<>();
            createConverters(arguments, inputConverters, outputConverters);

            // Create converters for state parameters (for serde to/from RowData)
            Map<String, DataStructureConverter<Object, Object>> stateConverters = new HashMap<>();
            createStateConverters(arguments, stateConverters);

            // Wrap ListView/MapView converters to inject TestListView/TestMapView
            for (ArgumentInfo arg : arguments) {
                if (arg instanceof StateArgumentInfo) {
                    StateArgumentInfo stateArg = (StateArgumentInfo) arg;
                    DataStructureConverter<Object, Object> originalConverter =
                            stateConverters.get(stateArg.name);

                    // Check if this is ListView or MapView state
                    if (ListView.class.isAssignableFrom(stateArg.stateClass)) {
                        // Wrap converter to produce TestListView instead of ListView
                        stateConverters.put(
                                stateArg.name, new ListViewConverterWrapper(originalConverter));
                    } else if (MapView.class.isAssignableFrom(stateArg.stateClass)) {
                        // Wrap converter to produce TestMapView instead of MapView
                        stateConverters.put(
                                stateArg.name, new MapViewConverterWrapper(originalConverter));
                    }
                    // For POJOs/Row, use original converter (no wrapping needed)
                }
            }

            // Build the map of named arguments for quick lookup.
            Map<String, ArgumentInfo> argumentsByName = new HashMap<>();
            for (ArgumentInfo arg : arguments) {
                if (arg.name != null) {
                    argumentsByName.put(arg.name, arg);
                }
            }

            ProcessTableFunctionTestHarness<OUT> harness =
                    new ProcessTableFunctionTestHarness<>(
                            function,
                            functionContext,
                            defaultTableArg,
                            evalMethod,
                            arguments,
                            argumentsByName,
                            isSingleTableFunction,
                            extractScalarValues(arguments),
                            inputConverters,
                            outputConverters,
                            stateConverters);

            // Populate initial state if provided
            for (Map.Entry<String, Map<Row, Object>> entry : initialState.entrySet()) {
                String stateArgument = entry.getKey();
                for (Map.Entry<Row, Object> stateEntry : entry.getValue().entrySet()) {
                    Row partitionKey = stateEntry.getKey();
                    Object stateValue = stateEntry.getValue();
                    harness.setStateForKey(stateArgument, partitionKey, stateValue);
                }
            }

            return harness;
        }

        /** Extracts scalar values from configs, creating a map keyed by argument name. */
        private Map<String, Object> extractScalarValues(List<ArgumentInfo> arguments) {
            Map<String, Object> values = new HashMap<>();
            for (ArgumentInfo arg : arguments) {
                if (arg instanceof ScalarArgumentInfo) {
                    ScalarArgumentConfiguration config = scalarArgs.get(arg.name);
                    if (config != null) {
                        values.put(arg.name, config.value);
                    }
                }
            }
            return values;
        }

        /**
         * Creates and initializes data structure converters for all table arguments.
         *
         * <p>For Row types, both input and output converters are the same (between Row and
         * RowData).
         *
         * <p>For structured types, input converter uses Row types (Row to RowData), and the output
         * converter uses the structured type.
         */
        private void createConverters(
                List<ArgumentInfo> arguments,
                Map<String, DataStructureConverter<Object, Object>> inputConverters,
                Map<String, DataStructureConverter<Object, Object>> outputConverters) {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

            for (ArgumentInfo arg : arguments) {
                if (arg instanceof TableArgumentInfo) {
                    TableArgumentInfo tableArg = (TableArgumentInfo) arg;
                    String converterKey = tableArg.name;

                    LogicalType logicalType = tableArg.dataType.getLogicalType();
                    boolean isStructuredType =
                            logicalType instanceof StructuredType
                                    && ((StructuredType) logicalType)
                                            .getImplementationClass()
                                            .isPresent();

                    if (isStructuredType) {
                        StructuredType structuredType = (StructuredType) logicalType;
                        List<RowType.RowField> rowFields = new ArrayList<>();
                        for (StructuredType.StructuredAttribute attr :
                                structuredType.getAttributes()) {
                            rowFields.add(new RowType.RowField(attr.getName(), attr.getType()));
                        }
                        RowType rowType = new RowType(logicalType.isNullable(), rowFields);
                        DataType rowDataType = TypeConversions.fromLogicalToDataType(rowType);

                        DataStructureConverter<Object, Object> inputConverter =
                                DataStructureConverters.getConverter(rowDataType);
                        inputConverter.open(classLoader);

                        DataStructureConverter<Object, Object> outputConverter =
                                DataStructureConverters.getConverter(tableArg.dataType);
                        outputConverter.open(classLoader);

                        inputConverters.put(converterKey, inputConverter);
                        outputConverters.put(converterKey, outputConverter);
                    } else {
                        DataStructureConverter<Object, Object> converter =
                                DataStructureConverters.getConverter(tableArg.dataType);
                        converter.open(classLoader);

                        inputConverters.put(converterKey, converter);
                        outputConverters.put(converterKey, converter);
                    }
                }
            }
        }

        private java.lang.reflect.Method findEvalMethod() throws NoSuchMethodException {
            java.lang.reflect.Method[] methods = functionClass.getMethods();
            java.lang.reflect.Method evalMethod = null;
            int evalMethodCount = 0;

            for (java.lang.reflect.Method method : methods) {
                if (method.getName().equals("eval")) {
                    evalMethod = method;
                    evalMethodCount++;
                }
            }

            if (evalMethodCount == 0) {
                throw new NoSuchMethodException(
                        "No eval() method found in " + functionClass.getSimpleName());
            } else if (evalMethodCount > 1) {
                throw new IllegalStateException(
                        "Multiple eval() methods found in "
                                + functionClass.getSimpleName()
                                + ". ProcessTableFunction must have exactly one eval() method.");
            } else {
                return evalMethod;
            }
        }

        /**
         * Validates that the eval() method doesn't use unsupported features. Temporary, until
         * Context is supported.
         */
        private void validateEvalMethodSupported(
                java.lang.reflect.Method evalMethod, List<ArgumentInfo> arguments) {
            java.lang.reflect.Parameter[] parameters = evalMethod.getParameters();

            for (int i = 0; i < parameters.length; i++) {
                java.lang.reflect.Parameter param = parameters[i];
                Class<?> paramType = param.getType();

                if (ProcessTableFunction.Context.class.isAssignableFrom(paramType)) {
                    throw new IllegalStateException(
                            String.format(
                                    "ProcessTableFunctionTestHarness does not yet support Context parameters. "
                                            + "Found Context parameter at position %d in eval() method. ",
                                    i));
                }
            }

            // Parameter count should match arguments list
            if (parameters.length != arguments.size()) {
                long stateCount =
                        arguments.stream().filter(arg -> arg instanceof StateArgumentInfo).count();
                long nonStateCount = arguments.size() - stateCount;
                throw new IllegalStateException(
                        String.format(
                                "Parameter count mismatch: eval() has %d parameters but expected %d (%d state + %d arguments). "
                                        + "This may indicate missing @StateHint or @ArgumentHint annotations.",
                                parameters.length, arguments.size(), stateCount, nonStateCount));
            }
        }

        /**
         * Creates data structure converters for state parameters.
         *
         * <p>State is stored internally as RowData and converted to/from the external state type.
         */
        private void createStateConverters(
                List<ArgumentInfo> arguments,
                Map<String, DataStructureConverter<Object, Object>> converters) {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            for (ArgumentInfo arg : arguments) {
                if (arg instanceof StateArgumentInfo) {
                    StateArgumentInfo stateArg = (StateArgumentInfo) arg;
                    DataStructureConverter<Object, Object> converter =
                            DataStructureConverters.getConverter(stateArg.dataType);
                    converter.open(classLoader);
                    converters.put(stateArg.name, converter);
                }
            }
        }

        /**
         * Validates that all SET_SEMANTIC_TABLE arguments with partitioning use consistent
         * partitioning. All such arguments must have the same number of partition columns with
         * matching data types.
         */
        private void validatePartitionConsistency(List<ArgumentInfo> arguments) {
            List<TableArgumentInfo> partitionedTables = new ArrayList<>();
            for (ArgumentInfo arg : arguments) {
                if (arg instanceof TableArgumentInfo) {
                    TableArgumentInfo tableArg = (TableArgumentInfo) arg;
                    if (tableArg.isSetSemantic() && tableArg.partitionColumnNames != null) {
                        partitionedTables.add(tableArg);
                    }
                }
            }

            if (partitionedTables.size() <= 1) {
                return;
            }

            TableArgumentInfo first = partitionedTables.get(0);
            int expectedPartitionColumnCount = first.partitionColumnNames.length;

            for (int i = 1; i < partitionedTables.size(); i++) {
                TableArgumentInfo current = partitionedTables.get(i);

                if (current.partitionColumnNames.length != expectedPartitionColumnCount) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Inconsistent partitioning: Table argument '%s' has %d partition column(s), "
                                            + "but table argument '%s' has %d partition column(s). "
                                            + "All SET_SEMANTIC_TABLE arguments must use consistent partitioning "
                                            + "(same number of columns and matching data types).",
                                    first.name,
                                    expectedPartitionColumnCount,
                                    current.name,
                                    current.partitionColumnNames.length));
                }

                // Check that partition column types match
                for (int colIdx = 0; colIdx < expectedPartitionColumnCount; colIdx++) {
                    String firstColName = first.partitionColumnNames[colIdx];
                    String currentColName = current.partitionColumnNames[colIdx];
                    DataType firstColType = extractPartitionColumnType(first, firstColName);
                    DataType currentColType = extractPartitionColumnType(current, currentColName);

                    if (!firstColType.equals(currentColType)) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Inconsistent partitioning: Partition column '%s' of table argument '%s' "
                                                + "has type %s, but partition column '%s' of table argument '%s' "
                                                + "has type %s. All SET_SEMANTIC_TABLE arguments must use "
                                                + "consistent partitioning (same number of columns and matching data types).",
                                        firstColName,
                                        first.name,
                                        firstColType,
                                        currentColName,
                                        current.name,
                                        currentColType));
                    }
                }
            }
        }

        private DataType extractPartitionColumnType(TableArgumentInfo tableArg, String columnName) {
            if (tableArg.dataType instanceof org.apache.flink.table.types.FieldsDataType) {
                org.apache.flink.table.types.FieldsDataType fieldsDataType =
                        (org.apache.flink.table.types.FieldsDataType) tableArg.dataType;

                // Get field names and types
                org.apache.flink.table.types.logical.RowType rowType =
                        (org.apache.flink.table.types.logical.RowType)
                                fieldsDataType.getLogicalType();
                List<DataType> fieldDataTypes = fieldsDataType.getChildren();

                // Find the field by name
                int fieldIndex = 0;
                for (org.apache.flink.table.types.logical.RowType.RowField field :
                        rowType.getFields()) {
                    if (field.getName().equals(columnName)) {
                        return fieldDataTypes.get(fieldIndex);
                    }
                    fieldIndex++;
                }
            }

            throw new IllegalStateException(
                    String.format(
                            "Cannot extract data type for partition column '%s' of argument '%s'",
                            columnName, tableArg.name));
        }

        // ---------------------------------------------------------------------
        // Type Inference
        // ---------------------------------------------------------------------

        /**
         * Extracts type inference from the PTF and validates builder configuration.
         *
         * <p>Uses SystemTypeInference to extract both state and non-state arguments, merging them
         * into a single ordered list matching the eval() signature.
         */
        private List<ArgumentInfo> extractAndValidateTypeInference(
                ProcessTableFunction<OUT> function, java.lang.reflect.Method evalMethod) {

            DataTypeFactory dataTypeFactory = createDataTypeFactory();
            TypeInference baseTypeInference = function.getTypeInference(dataTypeFactory);
            TypeInference systemTypeInference =
                    SystemTypeInference.of(FunctionKind.PROCESS_TABLE, baseTypeInference);

            // Extract state parameters (state args come first in eval() signature)
            List<ArgumentInfo> arguments = new ArrayList<>();
            LinkedHashMap<String, org.apache.flink.table.types.inference.StateTypeStrategy>
                    stateStrategies = systemTypeInference.getStateTypeStrategies();

            java.lang.reflect.Parameter[] parameters = evalMethod.getParameters();
            int paramIndex = 0;

            // Process state parameters first
            for (Map.Entry<String, org.apache.flink.table.types.inference.StateTypeStrategy> entry :
                    stateStrategies.entrySet()) {
                String stateName = entry.getKey();
                org.apache.flink.table.types.inference.StateTypeStrategy stateStrategy =
                        entry.getValue();

                if (paramIndex >= parameters.length) {
                    throw new IllegalStateException(
                            "State parameter count exceeds eval() parameter count");
                }

                java.lang.reflect.Parameter param = parameters[paramIndex];
                Class<?> stateClass = param.getType();

                // Infer data type using the StateTypeStrategy
                DataType dataType =
                        stateStrategy
                                .inferType(null)
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        "Could not infer data type for state parameter: "
                                                                + stateName));

                // Extract TTL if present
                java.time.Duration ttl = stateStrategy.getTimeToLive(null).orElse(null);

                arguments.add(new StateArgumentInfo(stateName, dataType, stateClass, ttl));
                paramIndex++;
            }

            // Extract non-state arguments
            Optional<List<StaticArgument>> staticArgsOpt = systemTypeInference.getStaticArguments();
            if (staticArgsOpt.isEmpty()) {
                throw new IllegalStateException(
                        "PTF does not provide static argument information. "
                                + "Ensure @ArgumentHint annotations are present on all eval() parameters.");
            }

            List<StaticArgument> allArgs = staticArgsOpt.get();
            List<StaticArgument> userArgs = new ArrayList<>();
            for (StaticArgument arg : allArgs) {
                if (!isSystemArgument(arg.getName())) {
                    userArgs.add(arg);
                }
            }

            // Build ArgumentInfo for non-state arguments
            for (StaticArgument staticArg : userArgs) {
                boolean isScalar =
                        staticArg
                                .getTraits()
                                .contains(
                                        org.apache.flink.table.types.inference.StaticArgumentTrait
                                                .SCALAR);
                boolean isTableArg =
                        staticArg
                                        .getTraits()
                                        .contains(
                                                org.apache.flink.table.types.inference
                                                        .StaticArgumentTrait.ROW_SEMANTIC_TABLE)
                                || staticArg
                                        .getTraits()
                                        .contains(
                                                org.apache.flink.table.types.inference
                                                        .StaticArgumentTrait.SET_SEMANTIC_TABLE);

                if (isScalar || isTableArg) {
                    ArgumentInfo argInfo = buildArgumentInfo(staticArg);
                    arguments.add(argInfo);
                } else {
                    throw new IllegalStateException(
                            "Unknown argument type for StaticArgument. "
                                    + "Expected SCALAR, ROW_SEMANTIC_TABLE, or SET_SEMANTIC_TABLE trait.");
                }
            }

            validateArgumentConfiguration(arguments);

            return arguments;
        }

        /** Checks if an argument name is a system-reserved argument. */
        private boolean isSystemArgument(String argName) {
            return SystemTypeInference.PROCESS_TABLE_FUNCTION_ARG_ON_TIME.equals(argName)
                    || SystemTypeInference.PROCESS_TABLE_FUNCTION_ARG_UID.equals(argName);
        }

        private DataTypeFactory createDataTypeFactory() {
            // Use DataTypeFactoryMock from flink-table-common test utilities
            return new DataTypeFactoryMock();
        }

        private ArgumentInfo buildArgumentInfo(StaticArgument staticArg) {

            String name = staticArg.getName();
            ArgumentTrait primaryTrait = extractPrimaryTrait(staticArg.getTraits());

            // Get data type
            DataType dataType;
            if (primaryTrait == ArgumentTrait.SCALAR) {
                Optional<DataType> dataTypeOpt = staticArg.getDataType();
                if (dataTypeOpt.isPresent()) {
                    dataType = dataTypeOpt.get();
                } else {
                    throw new IllegalStateException(
                            String.format(
                                    "Cannot determine data type for scalar argument '%s'", name));
                }
                return new ScalarArgumentInfo(name, dataType);
            } else {
                // For table arguments, check both annotation and builder config
                Optional<DataType> annotationTypeOpt = staticArg.getDataType();
                TableArgumentConfiguration config = tableArgs.get(name);

                if (annotationTypeOpt.isPresent()
                        && config != null
                        && config.explicitType != null) {
                    // Both specified - validate they match
                    DataTypeFactory factory = createDataTypeFactory();
                    DataType builderType = factory.createDataType(config.explicitType);
                    DataType annotationType = annotationTypeOpt.get();

                    if (!annotationType.equals(builderType)) {
                        throw new IllegalStateException(
                                String.format(
                                        "Type mismatch for table argument '%s': "
                                                + "annotation declares type %s but builder declares type %s. "
                                                + "Use either @ArgumentHint(type = ...) OR .withTableArgument(...), not both.",
                                        name, annotationType, builderType));
                    }
                    // Both match, use annotation type
                    dataType = annotationType;
                } else if (annotationTypeOpt.isPresent()) {
                    // Use type from @ArgumentHint(type = @DataTypeHint(...))
                    dataType = annotationTypeOpt.get();
                } else if (config != null && config.explicitType != null) {
                    // Use builder configuration
                    DataTypeFactory factory = createDataTypeFactory();
                    dataType = factory.createDataType(config.explicitType);
                } else {
                    // Neither annotation nor builder config provided
                    throw new IllegalStateException(
                            String.format(
                                    "Table argument '%s' requires explicit type configuration. "
                                            + "Use @ArgumentHint(type = @DataTypeHint(\"ROW<...>\")) or "
                                            + ".withTableArgument(\"%s\", DataTypes.of(\"ROW<...>\"))",
                                    name, name));
                }

                String[] partitionColumnNames = null;
                if (primaryTrait == ArgumentTrait.SET_SEMANTIC_TABLE) {
                    boolean hasOptionalPartitionBy =
                            staticArg
                                    .getTraits()
                                    .contains(StaticArgumentTrait.OPTIONAL_PARTITION_BY);
                    partitionColumnNames =
                            extractAndValidatePartitionColumns(
                                    name, dataType, hasOptionalPartitionBy);
                }

                boolean hasPassColumnsThrough =
                        staticArg.getTraits().contains(StaticArgumentTrait.PASS_COLUMNS_THROUGH);

                return new TableArgumentInfo(
                        name, dataType, primaryTrait, partitionColumnNames, hasPassColumnsThrough);
            }
        }

        private ArgumentTrait extractPrimaryTrait(EnumSet<StaticArgumentTrait> staticTraits) {
            if (staticTraits.contains(StaticArgumentTrait.SCALAR)) {
                return ArgumentTrait.SCALAR;
            }
            if (staticTraits.contains(StaticArgumentTrait.ROW_SEMANTIC_TABLE)) {
                return ArgumentTrait.ROW_SEMANTIC_TABLE;
            }
            if (staticTraits.contains(StaticArgumentTrait.SET_SEMANTIC_TABLE)) {
                return ArgumentTrait.SET_SEMANTIC_TABLE;
            }
            // Default to scalar
            return ArgumentTrait.SCALAR;
        }

        private String[] extractAndValidatePartitionColumns(
                String name, DataType dataType, boolean isOptionalPartitionBy) {
            PartitionConfiguration config = partitionConfigs.get(name);
            if (config == null) {
                if (isOptionalPartitionBy) {
                    return null;
                }
                throw new IllegalStateException(
                        String.format(
                                "No partition configuration found for table argument '%s'. "
                                        + "Use withPartitionBy(\"%s\", ...) to configure partitioning.",
                                name, name));
            }

            // Validate that all partition column names exist in the table schema
            RowType rowType = (RowType) dataType.getLogicalType();
            List<String> fieldNames = new ArrayList<>();
            for (RowType.RowField field : rowType.getFields()) {
                fieldNames.add(field.getName());
            }

            // Check each partition column exists
            for (String columnName : config.columnNames) {
                if (!fieldNames.contains(columnName)) {
                    throw new IllegalArgumentException(
                            "Partition column '"
                                    + columnName
                                    + "' not found. "
                                    + "Available columns: "
                                    + fieldNames);
                }
            }
            return config.columnNames;
        }

        private void validateArgumentConfiguration(List<ArgumentInfo> arguments) {
            // Check that all arguments have been configured in the builder.
            // Table arguments with inline types and state arguments can be elided from builder
            // config.
            for (ArgumentInfo arg : arguments) {
                if (arg instanceof ScalarArgumentInfo) {
                    if (!scalarArgs.containsKey(arg.name)) {
                        throw new IllegalStateException(
                                String.format(
                                        "Missing required scalar argument '%s'. "
                                                + "Use .withScalarArgument(\"%s\", ...)",
                                        arg.name, arg.name));
                    }
                } else if (arg instanceof TableArgumentInfo) {
                    // For table arguments: builder config is optional if type comes from annotation
                    boolean hasBuilderConfig = tableArgs.containsKey(arg.name);
                    boolean hasInlineType = arg.dataType != null;

                    if (!hasBuilderConfig && !hasInlineType) {
                        throw new IllegalStateException(
                                String.format(
                                        "Missing required table argument '%s'. "
                                                + "Either specify @ArgumentHint(type = @DataTypeHint(...)) "
                                                + "or use .withTableArgument(\"%s\", ...)",
                                        arg.name, arg.name));
                    }
                }
                // StateArgumentInfo doesn't need builder configuration - extracted from @StateHint
            }

            // Check for extra configured arguments not in signature
            java.util.Set<String> validNames = new java.util.HashSet<>();
            for (ArgumentInfo arg : arguments) {
                if (arg.name != null) {
                    validNames.add(arg.name);
                }
            }

            for (String configuredScalar : scalarArgs.keySet()) {
                if (!validNames.contains(configuredScalar)) {
                    throw new IllegalStateException(
                            "Unknown scalar argument: '"
                                    + configuredScalar
                                    + "'. Not found in PTF signature.");
                }
            }

            for (String configuredTable : tableArgs.keySet()) {
                if (!validNames.contains(configuredTable)) {
                    throw new IllegalStateException(
                            "Unknown table argument: '"
                                    + configuredTable
                                    + "'. Not found in PTF signature.");
                }
            }
        }

        private ProcessTableFunction<OUT> instantiateFunction() throws IllegalArgumentException {
            try {
                return functionClass.getDeclaredConstructor().newInstance();
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException(
                        "PTF class must have a no-arg constructor: " + functionClass.getName(), e);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Failed to instantiate PTF: " + functionClass.getName(), e);
            }
        }
    }

    /**
     * Base class for argument metadata extracted from type inference.
     *
     * <p>Represents validated argument information combining PTF signature, type inference results,
     * and builder configuration.
     *
     * <p>Position in eval() signature is implicit from the list order.
     */
    private abstract static class ArgumentInfo {
        final String name;
        final DataType dataType;

        ArgumentInfo(String name, DataType dataType) {
            this.name = name;
            this.dataType = dataType;
        }
    }

    /** Metadata for scalar arguments. */
    private static class ScalarArgumentInfo extends ArgumentInfo {
        ScalarArgumentInfo(String name, DataType dataType) {
            super(name, dataType);
        }
    }

    /** Metadata for table arguments (ROW_SEMANTIC_TABLE or SET_SEMANTIC_TABLE). */
    private static class TableArgumentInfo extends ArgumentInfo {
        final ArgumentTrait trait;
        final String[] partitionColumnNames; // nullable
        final boolean hasPassColumnsThrough;

        TableArgumentInfo(
                String name,
                DataType dataType,
                ArgumentTrait trait,
                String[] partitionColumnNames,
                boolean hasPassColumnsThrough) {
            super(name, dataType);
            this.trait = trait;
            this.partitionColumnNames = partitionColumnNames;
            this.hasPassColumnsThrough = hasPassColumnsThrough;
        }

        boolean isSetSemantic() {
            return trait == ArgumentTrait.SET_SEMANTIC_TABLE;
        }
    }

    /** Metadata for state arguments. */
    private static class StateArgumentInfo extends ArgumentInfo {
        final Class<?> stateClass;
        final java.time.Duration ttl; // nullable

        StateArgumentInfo(
                String name, DataType dataType, Class<?> stateClass, java.time.Duration ttl) {
            super(name, dataType);
            this.stateClass = stateClass;
            this.ttl = ttl;
        }
    }

    private static class TableArgumentConfiguration {
        final String name;
        AbstractDataType<?> explicitType; // from withTableArgument

        TableArgumentConfiguration(String name) {
            this.name = name;
        }
    }

    private static class ScalarArgumentConfiguration {
        final String name;
        Object value;

        ScalarArgumentConfiguration(String name, Object value) {
            this.name = name;
            this.value = value;
        }
    }

    /** Wrapper that converts RowData to TestListView instead of ListView. */
    private static class ListViewConverterWrapper
            implements DataStructureConverter<Object, Object> {
        private final DataStructureConverter<Object, Object> delegate;

        ListViewConverterWrapper(DataStructureConverter<Object, Object> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open(ClassLoader classLoader) {
            delegate.open(classLoader);
        }

        @Override
        public Object toInternal(Object external) {
            return delegate.toInternal(external);
        }

        @Override
        public Object toExternal(Object internal) {
            ListView<?> listView = (ListView<?>) delegate.toExternal(internal);
            // Create TestListView and copy data
            TestListView testListView = new TestListView<>();
            testListView.setList(listView.getList());
            return testListView;
        }

        @Override
        public Object toInternalOrNull(Object external) {
            return delegate.toInternalOrNull(external);
        }

        @Override
        public Object toExternalOrNull(Object internal) {
            if (internal == null) {
                return null;
            }
            return toExternal(internal);
        }
    }

    /** Wrapper that converts RowData to TestMapView instead of MapView. */
    private static class MapViewConverterWrapper implements DataStructureConverter<Object, Object> {
        private final DataStructureConverter<Object, Object> delegate;

        MapViewConverterWrapper(DataStructureConverter<Object, Object> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open(ClassLoader classLoader) {
            delegate.open(classLoader);
        }

        @Override
        public Object toInternal(Object external) {
            return delegate.toInternal(external);
        }

        @Override
        public Object toExternal(Object internal) {
            MapView<?, ?> mapView = (MapView<?, ?>) delegate.toExternal(internal);
            // Create TestMapView and copy data
            TestMapView testMapView = new TestMapView<>();
            testMapView.setMap(mapView.getMap());
            return testMapView;
        }

        @Override
        public Object toInternalOrNull(Object external) {
            return delegate.toInternalOrNull(external);
        }

        @Override
        public Object toExternalOrNull(Object internal) {
            if (internal == null) {
                return null;
            }
            return toExternal(internal);
        }
    }

    private static class PartitionConfiguration {
        final String tableName;
        final String[] columnNames;

        PartitionConfiguration(String tableName, String[] columnNames) {
            this.tableName = tableName;
            this.columnNames = columnNames;
        }
    }
}
