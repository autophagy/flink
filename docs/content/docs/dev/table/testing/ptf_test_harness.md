---
title: "Testing Process Table Functions"
weight: 10
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Testing Process Table Functions

The `ProcessTableFunctionTestHarness` provides a lightweight unit testing framework for Process Table
Functions (PTFs). It allows you to test PTF logic without starting a full Flink cluster. It is useful
for testing and validating PTF business logic, multi-table PTF behaviour and validating errors.

For end-to-end integration testing with the full Flink planner and runtime, use integration tests
instead.

{{< top >}}

## Quick Start

{{< tabs "quickstart" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.runtime.functions.ProcessTableFunctionTestHarness;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

// PTF under test
@DataTypeHint("ROW<doubled INT>")
public class DoublePTF extends ProcessTableFunction<Row> {
    public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
        int value = input.getFieldAs("value");
        collect(Row.of(value * 2));
    }
}

// Test
@Test
void testDoublePTF() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(DoublePTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                    .build()) {

        harness.processElement(Row.of(5));
        harness.processElement(Row.of(10));

        List<Row> output = harness.getOutput();
        assertThat(output).hasSize(2);
        assertThat(output.get(0)).isEqualTo(Row.of(10));
        assertThat(output.get(1)).isEqualTo(Row.of(20));
    }
}
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## Common Testing Scenarios

### Testing Row-Semantic Tables

Use `.withTableArgument()` to configure the input table schema:

{{< tabs "row-semantic" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<value INT>")
public class PassthroughPTF extends ProcessTableFunction<Row> {
    public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
        collect(input);
    }
}

@Test
void testPassthrough() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(PassthroughPTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                    .build()) {

        harness.processElement(Row.of(42));
        harness.processElement(Row.of(100));

        List<Row> output = harness.getOutput();
        assertThat(output).containsExactly(Row.of(42), Row.of(100));
    }
}
```
{{< /tab >}}
{{< /tabs >}}

### Testing Set-Semantic Tables with Partitioning

For `SET_SEMANTIC_TABLE`, use `.withPartitionBy()` to configure partition columns:

{{< tabs "set-semantic" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<doubled INT>")
public class PartitionedPTF extends ProcessTableFunction<Row> {
    public void eval(@ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
        int value = input.getFieldAs("value");
        collect(Row.of(value * 2));
    }
}

@Test
void testPartitionedPTF() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(PartitionedPTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<key STRING, value INT>"))
                    .withPartitionBy("input", "key")
                    .build()) {

        harness.processElement(Row.of("A", 10));
        harness.processElement(Row.of("B", 20));

        List<Row> output = harness.getOutput();
        assertThat(output.get(0)).isEqualTo(Row.of("A", 20));
        assertThat(output.get(1)).isEqualTo(Row.of("B", 40));
    }
}
```
{{< /tab >}}
{{< /tabs >}}

### Testing Multiple Table Arguments

Use `processElementForTable()` to specify which table receives each row:

{{< tabs "multi-table" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<output STRING>")
public class JoinPTF extends ProcessTableFunction<Row> {
    public void eval(
            @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row left,
            @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row right) {
        if (left != null) {
            collect(Row.of("LEFT: " + left));
        }
        if (right != null) {
            collect(Row.of("RIGHT: " + right));
        }
    }
}

@Test
void testMultiTable() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(JoinPTF.class)
                    .withTableArgument("left", DataTypes.of("ROW<id INT, name STRING>"))
                    .withPartitionBy("left", "id")
                    .withTableArgument("right", DataTypes.of("ROW<id INT, city STRING>"))
                    .withPartitionBy("right", "id")
                    .build()) {

        // Use processElementForTable() to target specific tables
        harness.processElementForTable("left", Row.of(1, "Alice"));
        harness.processElementForTable("right", Row.of(1, "Berlin"));

        List<Row> output = harness.getOutput();
        assertThat(output.get(0)).isEqualTo(Row.of(1, null, "LEFT: +I[1, Alice]"));
        assertThat(output.get(1)).isEqualTo(Row.of(null, 1, "RIGHT: +I[1, Berlin]"));
    }
}
```
{{< /tab >}}
{{< /tabs >}}

### Testing with Scalar Arguments

Use `.withScalarArgument()` to configure scalar parameter values:

{{< tabs "scalar-args" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<value INT>")
public class FilterPTF extends ProcessTableFunction<Row> {
    public void eval(
            @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input,
            @ArgumentHint(ArgumentTrait.SCALAR) int threshold) {
        int value = input.getFieldAs("value");
        if (value > threshold) {
            collect(Row.of(value));
        }
    }
}

@Test
void testFilter() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(FilterPTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                    .withScalarArgument("threshold", 50)  // Configure scalar value
                    .build()) {

        harness.processElement(Row.of(30));
        harness.processElement(Row.of(70));

        List<Row> output = harness.getOutput();
        assertThat(output).containsExactly(Row.of(70));
    }
}
```
{{< /tab >}}
{{< /tabs >}}

**Scalar-Only PTFs**: For PTFs with only scalar arguments, use `invoke()` to trigger evaluation:

{{< tabs "scalar-only" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<sum INT>")
public class AddPTF extends ProcessTableFunction<Row> {
    public void eval(
            @ArgumentHint(ArgumentTrait.SCALAR) int a,
            @ArgumentHint(ArgumentTrait.SCALAR) int b) {
        collect(Row.of(a + b));
    }
}

@Test
void testScalarOnly() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(AddPTF.class)
                    .withScalarArgument("a", 5)
                    .withScalarArgument("b", 7)
                    .build()) {

        harness.invoke();  // Use invoke() instead of processElement()

        List<Row> output = harness.getOutput();
        assertThat(output).containsExactly(Row.of(12));
    }
}
```
{{< /tab >}}
{{< /tabs >}}

### Configuring Table Argument Types

The harness supports two ways to specify table argument types:

{{< tabs "type-config" >}}
{{< tab "Java" >}}
```java
// Option 1: Inline type annotation using DataTypeHint
@DataTypeHint("ROW<doubled INT>")
public class InlineTypePTF extends ProcessTableFunction<Row> {
    public void eval(
            @ArgumentHint(
                value = ArgumentTrait.ROW_SEMANTIC_TABLE,
                type = @DataTypeHint("ROW<value INT>")
            ) Row input) {
        int value = input.getFieldAs("value");
        collect(Row.of(value * 2));
    }
}

@Test
void testInlineType() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(InlineTypePTF.class).build()) {

        harness.processElement(Row.of(5));
        assertThat(harness.getOutput()).containsExactly(Row.of(10));
    }
}

// Option 2: Builder configuration
@Test
void testBuilderType() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(DoublePTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                    .build()) {

        harness.processElement(Row.of(5));
        assertThat(harness.getOutput()).containsExactly(Row.of(10));
    }
}
```
{{< /tab >}}
{{< /tabs >}}

**Structured Types**: The harness supports structured POJO types in addition to `Row`, both as PTF inputs
and outputs:

{{< tabs "pojo-types" >}}
{{< tab "Java" >}}
```java
public static class Customer {
    public String name;
    public int age;
}

@DataTypeHint("ROW<name STRING, age INT>")
public class CustomerPTF extends ProcessTableFunction<Customer> {
    public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Customer c) {
        collect(c);
    }
}

@Test
void testPOJO() throws Exception {
    try (ProcessTableFunctionTestHarness<Customer> harness =
            ProcessTableFunctionTestHarness.ofClass(CustomerPTF.class)
                    .withTableArgument("input", DataTypes.of(Customer.class))
                    .build()) {

        harness.processElement(Row.of("Alice", 30));

        List<Customer> output = harness.getOutput();
        assertThat(output.get(0).name).isEqualTo("Alice");
        assertThat(output.get(0).age).isEqualTo(30);
    }
}
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## Testing State

Process Table Functions can maintain state across rows within a partition. The test harness fully supports state arguments including value state (POJOs/Row), ListView, MapView, and state with TTL.

### Basic State with POJOs

Use `@StateHint` to declare state arguments. The harness automatically manages state across partitions:

{{< tabs "basic-state" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<count BIGINT>")
public class CounterPTF extends ProcessTableFunction<Row> {

    public static class CounterState {
        public long count = 0;
    }

    public void eval(
            @StateHint(type = @DataTypeHint("ROW<count BIGINT>")) CounterState state,
            @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
        state.count++;
        collect(Row.of(state.count));
    }
}

@Test
void testCounter() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(CounterPTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<id INT>"))
                    .withPartitionBy("input", "id")
                    .build()) {

        // Partition 1: counter increments independently
        harness.processElement(Row.of(1));
        harness.processElement(Row.of(1));

        // Partition 2: separate counter
        harness.processElement(Row.of(2));

        List<Row> output = harness.getOutput();
        assertThat(output.get(0)).isEqualTo(Row.of(1L));  // First row for partition 1
        assertThat(output.get(1)).isEqualTo(Row.of(2L));  // Second row for partition 1
        assertThat(output.get(2)).isEqualTo(Row.of(1L));  // First row for partition 2

        // Inspect state directly
        CounterState state1 = harness.getStateForKey("state", Row.of(1), CounterState.class);
        assertThat(state1.count).isEqualTo(2L);

        CounterState state2 = harness.getStateForKey("state", Row.of(2), CounterState.class);
        assertThat(state2.count).isEqualTo(1L);
    }
}
```
{{< /tab >}}
{{< /tabs >}}

### ListView State

Use `ListView<T>` for state that stores lists of elements:

{{< tabs "listview-state" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<history ARRAY<STRING>>")
public class HistoryPTF extends ProcessTableFunction<Row> {

    public void eval(
            @StateHint(type = @DataTypeHint("ARRAY<STRING>")) ListView<String> history,
            @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) throws Exception {

        String event = input.getFieldAs(1);
        history.add(event);

        List<String> allEvents = new ArrayList<>();
        for (String e : history.get()) {
            allEvents.add(e);
        }
        collect(Row.of(allEvents.toArray(new String[0])));
    }
}

@Test
void testListView() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(HistoryPTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<id INT, event STRING>"))
                    .withPartitionBy("input", "id")
                    .build()) {

        harness.processElement(Row.of(1, "login"));
        harness.processElement(Row.of(1, "click"));

        List<Row> output = harness.getOutput();
        assertThat(output.get(1)).isEqualTo(Row.of((Object) new String[]{"login", "click"}));

        // Inspect state directly
        ListView<String> history = harness.getStateForKey("history", Row.of(1), ListView.class);
        assertThat(history.get()).containsExactly("login", "click");
    }
}
```
{{< /tab >}}
{{< /tabs >}}

### MapView State

Use `MapView<K, V>` for state that stores key-value pairs:

{{< tabs "mapview-state" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<total BIGINT>")
public class AggregatePTF extends ProcessTableFunction<Row> {

    public void eval(
            @StateHint MapView<String, Long> aggregates,
            @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) throws Exception {

        String category = input.getFieldAs(1);
        Long amount = input.getFieldAs(2);

        Long current = aggregates.get(category);
        aggregates.put(category, (current == null ? 0 : current) + amount);

        long total = 0;
        for (Long value : aggregates.getMap().values()) {
            total += value;
        }
        collect(Row.of(total));
    }
}

@Test
void testMapView() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(AggregatePTF.class)
                    .withTableArgument("input",
                        DataTypes.of("ROW<id INT, category STRING, amount BIGINT>"))
                    .withPartitionBy("input", "id")
                    .build()) {

        harness.processElement(Row.of(1, "food", 100L));
        harness.processElement(Row.of(1, "transport", 50L));

        List<Row> output = harness.getOutput();
        assertThat(output.get(0)).isEqualTo(Row.of(100L));
        assertThat(output.get(1)).isEqualTo(Row.of(150L));

        // Inspect state directly
        MapView<String, Long> aggregates =
            harness.getStateForKey("aggregates", Row.of(1), MapView.class);
        assertThat(aggregates.get("food")).isEqualTo(100L);
        assertThat(aggregates.get("transport")).isEqualTo(50L);
    }
}
```
{{< /tab >}}
{{< /tabs >}}

### Initial State Setup

You can initialize state before processing begins using `withInitialStateArgument()`. This is useful for testing recovery scenarios, checkpoint resumption, or specific state conditions:

{{< tabs "initial-state" >}}
{{< tab "Java" >}}
```java
@Test
void testInitialState() throws Exception {
    // Set up initial state before processing
    CounterPTF.CounterState initialState = new CounterPTF.CounterState();
    initialState.count = 100L;

    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(CounterPTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<id INT>"))
                    .withPartitionBy("input", "id")
                    .withInitialStateArgument("state", Row.of(1), initialState)
                    .build()) {

        // Counter starts at 100 for partition 1
        harness.processElement(Row.of(1));
        assertThat(harness.getOutput()).containsExactly(Row.of(1, 101L));

        // Partition 2 starts with fresh state (count = 0)
        harness.processElement(Row.of(2));
        assertThat(harness.getOutput().get(1)).isEqualTo(Row.of(2, 1L));
    }
}
```
{{< /tab >}}
{{< /tabs >}}

Initial state works with all state types (POJOs, ListView, MapView). You can set initial state for multiple partitions by calling `withInitialStateArgument()` multiple times with different partition keys.

### State with TTL

State can have a time-to-live (TTL) that automatically expires old data. TTL is specified using the `ttl` parameter in `@StateHint`:

{{< tabs "state-ttl" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<recent_count BIGINT>")
public class RecentCounterPTF extends ProcessTableFunction<Row> {

    public static class CounterState {
        public long count = 0;
    }

    public void eval(
            // State expires after 1 hour
            @StateHint(
                ttl = "1 hour",
                type = @DataTypeHint("ROW<count BIGINT>")
            ) CounterState state,
            @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
        state.count++;
        collect(Row.of(state.count));
    }
}

@Test
void testStateTTL() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(RecentCounterPTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<id INT>"))
                    .withPartitionBy("input", "id")
                    .build()) {

        // Process at time 0
        harness.processElement(Row.of(1));
        assertThat(harness.getOutput()).containsExactly(Row.of(1L));
        harness.clearOutput();

        // Advance time by 30 minutes (state still valid)
        harness.advanceSystemClock(Duration.ofMinutes(30));
        harness.processElement(Row.of(1));
        assertThat(harness.getOutput()).containsExactly(Row.of(2L));
        harness.clearOutput();

        // Advance time by 45 minutes more (total: 75 minutes - state expired)
        harness.advanceSystemClock(Duration.ofMinutes(45));

        // Verify state was expired
        CounterState state = harness.getStateForKey("state", Row.of(1), CounterState.class);
        assertThat(state).isNull();

        // Process element - counter resets
        harness.processElement(Row.of(1));
        assertThat(harness.getOutput()).containsExactly(Row.of(1L));
    }
}
```
{{< /tab >}}
{{< /tabs >}}

**TTL Granularity**: TTL semantics vary by state type:
- **Value State (POJO/Row)**: TTL applies to the entire state object
- **ListView**: TTL applies per list element - elements expire independently
- **MapView**: TTL applies per map entry - entries expire independently

{{< tabs "ttl-granularity" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<values ARRAY<INT>>")
public class ElementTTLPTF extends ProcessTableFunction<Row> {

    public void eval(
            // Each list element has independent 1-hour TTL
            @StateHint(ttl = "1 hour", type = @DataTypeHint("ARRAY<INT>"))
            ListView<Integer> values,
            @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) throws Exception {

        values.add(input.getFieldAs(1));

        List<Integer> current = new ArrayList<>();
        for (Integer v : values.get()) {
            current.add(v);
        }
        collect(Row.of(current.toArray(new Integer[0])));
    }
}

@Test
void testElementTTL() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(ElementTTLPTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<id INT, value INT>"))
                    .withPartitionBy("input", "id")
                    .build()) {

        // Add element 1 at time 0
        harness.processElement(Row.of(1, 100));
        harness.clearOutput();

        // Add element 2 at time +30 minutes
        harness.advanceSystemClock(Duration.ofMinutes(30));
        harness.processElement(Row.of(1, 200));
        assertThat(harness.getOutput()).containsExactly(
            Row.of((Object) new Integer[]{100, 200}));
        harness.clearOutput();

        // Advance time by 45 minutes more (total: 75 minutes)
        // Element 1 expires (added at 0, TTL=60 min), element 2 remains (added at 30 min)
        harness.advanceSystemClock(Duration.ofMinutes(45));

        // Verify element 1 was expired
        ListView<Integer> values = harness.getStateForKey("values", Row.of(1), ListView.class);
        assertThat(values.get()).containsExactly(200);  // Only element 2 remains

        harness.processElement(Row.of(1, 300));
        assertThat(harness.getOutput()).containsExactly(
            Row.of((Object) new Integer[]{200, 300}));
    }
}
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## Testing Time and Timers

Process Table Functions can work with event time, register timers, and respond to timer firings. The test harness fully supports these time-based features.

### Basic Timer Registration

Use the `Context` parameter to access time services and register timers:

{{< tabs "basic-timer" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<message STRING>")
public class TimeoutPTF extends ProcessTableFunction<Row> {

    public void eval(
            Context ctx,
            @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
        // Register a timer 5 seconds in the future
        Instant timeout = Instant.ofEpochMilli(input.getFieldAs("timestamp"))
            .plusSeconds(5);
        ctx.timeContext(Instant.class).registerOnTime("timeout", timeout);
        collect(Row.of("Timer registered"));
    }

    public void onTimer(OnTimerContext ctx) {
        collect(Row.of("Timer fired: " + ctx.currentTimer()));
    }
}

@Test
void testTimer() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(TimeoutPTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<id INT, timestamp BIGINT>"))
                    .withPartitionBy("input", "id")
                    .withOnTimeColumn("input", "timestamp")
                    .build()) {

        // Process element with timestamp 1000ms
        harness.processElement(Row.of(1, 1000L));
        assertThat(harness.getOutput()).containsExactly(Row.of("Timer registered"));
        harness.clearOutput();

        // Advance watermark past timer timestamp (1000 + 5000 = 6000)
        harness.advanceWatermark(Instant.ofEpochMilli(6000));

        // Timer fires
        assertThat(harness.getOutput()).containsExactly(Row.of("Timer fired: timeout"));
    }
}
```
{{< /tab >}}
{{< /tabs >}}

### Named vs Unnamed Timers

Timers can be **named** or **unnamed**. Named timers are identified by a string name, while unnamed timers are identified only by their timestamp:

{{< tabs "timer-types" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<type STRING>")
public class MultiTimerPTF extends ProcessTableFunction<Row> {

    public void eval(Context ctx, @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
        Instant time = Instant.ofEpochMilli(input.getFieldAs("timestamp"));

        // Named timer
        ctx.timeContext(Instant.class).registerOnTime("named-timer", time.plusSeconds(10));

        // Unnamed timer
        ctx.timeContext(Instant.class).registerOnTime(time.plusSeconds(20));

        collect(Row.of("Timers registered"));
    }

    public void onTimer(OnTimerContext ctx) {
        String name = ctx.currentTimer();
        if (name != null) {
            collect(Row.of("Named: " + name));
        } else {
            collect(Row.of("Unnamed timer"));
        }
    }
}
```
{{< /tab >}}
{{< /tabs >}}

**Timer Semantics**:
- **Named timers** have replacement semantics - registering a timer with an existing name replaces the old timer
- **Unnamed timers** are independent - multiple unnamed timers can exist for different timestamps
- **Partition scoping** - timers are scoped to partition keys, just like state

### Accessing Time in eval()

Extract event time from the on-time column using `timeContext().time()`:

{{< tabs "time-access" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<event_time TIMESTAMP(3)>")
public class TimeExtractorPTF extends ProcessTableFunction<Row> {

    public void eval(
            Context ctx,
            @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {

        // Extract event time from the on-time column
        Instant eventTime = ctx.timeContext(Instant.class).time();
        collect(Row.of(eventTime));
    }
}

@Test
void testTimeExtraction() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(TimeExtractorPTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<id INT, ts BIGINT>"))
                    .withPartitionBy("input", "id")
                    .withOnTimeColumn("input", "ts")  // Designate ts as on-time column
                    .build()) {

        harness.processElement(Row.of(1, 1000L));
        assertThat(harness.getOutput()).containsExactly(
            Row.of(Instant.ofEpochMilli(1000)));
    }
}
```
{{< /tab >}}
{{< /tabs >}}

### Watermark Management

Watermarks control when timers fire. Advance watermarks using `advanceWatermark()` or `advanceWatermarkForTable()`:

{{< tabs "watermarks" >}}
{{< tab "Java" >}}
```java
@Test
void testWatermarks() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(TimeoutPTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<id INT, ts BIGINT>"))
                    .withPartitionBy("input", "id")
                    .withOnTimeColumn("input", "ts")
                    .withInitialWatermark(Instant.ofEpochMilli(0))  // Set initial watermark
                    .build()) {

        harness.processElement(Row.of(1, 1000L));

        // Advance all table watermarks
        harness.advanceWatermark(Instant.ofEpochMilli(6000));

        // Or advance specific table watermark (useful for multi-table PTFs)
        harness.advanceWatermarkForTable("input", Instant.ofEpochMilli(7000));

        // Query current watermark
        Instant current = harness.getCurrentWatermarkForTable("input", Instant.class);
        assertThat(current).isEqualTo(Instant.ofEpochMilli(7000));
    }
}
```
{{< /tab >}}
{{< /tabs >}}

**Watermark Semantics**:
- **Global watermark** = minimum watermark across all table inputs
- Timers fire when `watermark >= timer.timestamp`
- Watermarks cannot move backward (throws exception)
- For multi-input PTFs, use `advanceWatermarkForTable()` to control per-table watermarks

### Timer Introspection

Inspect pending and fired timers during tests:

{{< tabs "timer-introspection" >}}
{{< tab "Java" >}}
```java
@Test
void testTimerIntrospection() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(TimeoutPTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<id INT, ts BIGINT>"))
                    .withPartitionBy("input", "id")
                    .withOnTimeColumn("input", "ts")
                    .build()) {

        harness.processElement(Row.of(1, 1000L));

        // Check pending timers
        List<Timer> pending = harness.getPendingTimers();
        assertThat(pending).hasSize(1);
        assertThat(pending.get(0).getName()).isEqualTo("timeout");
        assertThat(pending.get(0).getTimestamp(Instant.class))
            .isEqualTo(Instant.ofEpochMilli(6000));
        assertThat(pending.get(0).hasFired()).isFalse();

        // Fire the timer
        harness.advanceWatermark(Instant.ofEpochMilli(7000));

        // Check fired timers
        List<Timer> fired = harness.getFiredTimers();
        assertThat(fired).hasSize(1);
        assertThat(fired.get(0).hasFired()).isTrue();

        // Pending timers are now empty
        assertThat(harness.getPendingTimers()).isEmpty();

        // Clear fired timer history
        harness.clearFiredTimers();
        assertThat(harness.getFiredTimers()).isEmpty();
    }
}
```
{{< /tab >}}
{{< /tabs >}}

### State Access in onTimer

Access and mutate state when timers fire:

{{< tabs "timer-state" >}}
{{< tab "Java" >}}
```java
@DataTypeHint("ROW<timeout_count BIGINT>")
public class TimeoutCounterPTF extends ProcessTableFunction<Row> {

    public static class CounterState {
        public long timeouts = 0;
    }

    public void eval(
            Context ctx,
            @StateHint(type = @DataTypeHint("ROW<timeouts BIGINT>")) CounterState state,
            @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {

        Instant timeout = Instant.ofEpochMilli(input.getFieldAs("timestamp"))
            .plusSeconds(5);
        ctx.timeContext(Instant.class).registerOnTime("timeout", timeout);
    }

    public void onTimer(
            OnTimerContext ctx,
            @StateHint(type = @DataTypeHint("ROW<timeouts BIGINT>")) CounterState state) {

        // State mutations in onTimer persist
        state.timeouts++;
        collect(Row.of(state.timeouts));
    }
}

@Test
void testTimerWithState() throws Exception {
    try (ProcessTableFunctionTestHarness<Row> harness =
            ProcessTableFunctionTestHarness.ofClass(TimeoutCounterPTF.class)
                    .withTableArgument("input", DataTypes.of("ROW<id INT, timestamp BIGINT>"))
                    .withPartitionBy("input", "id")
                    .withOnTimeColumn("input", "timestamp")
                    .build()) {

        // Register two timers
        harness.processElement(Row.of(1, 1000L));
        harness.processElement(Row.of(1, 2000L));

        // Fire first timer
        harness.advanceWatermark(Instant.ofEpochMilli(6000));
        assertThat(harness.getOutput()).containsExactly(Row.of(1L));
        harness.clearOutput();

        // Fire second timer - counter increments
        harness.advanceWatermark(Instant.ofEpochMilli(7000));
        assertThat(harness.getOutput()).containsExactly(Row.of(2L));

        // Verify state persisted
        CounterState state = harness.getStateForKey("state", Row.of(1), CounterState.class);
        assertThat(state.timeouts).isEqualTo(2L);
    }
}
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## Unimplemented Features

| Feature                                                     | Support                   |
|-------------------------------------------------------------|---------------------------|
| **Update traits (SUPPORTS_UPDATES, REQUIRE_UPDATE_BEFORE)** | ❌ Not currently supported |

{{< top >}}
