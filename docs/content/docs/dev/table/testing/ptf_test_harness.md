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

## Unimplemented Features

| Feature                                                     | Support                   |
|-------------------------------------------------------------|---------------------------|
| **Context parameter**                                       | ❌ Not currently supported |
| **State (@StateHint)**                                      | ❌ Not currently supported |
| **Timers (onTimer)**                                        | ❌ Not currently supported |
| **on_time / rowtime**                                       | ❌ Not currently supported |
| **Update traits (SUPPORTS_UPDATES, REQUIRE_UPDATE_BEFORE)** | ❌ Not currently supported |

{{< top >}}
