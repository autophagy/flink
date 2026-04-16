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

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProcessTableFunctionTestHarnessTest {

    @DataTypeHint("ROW<value INT>")
    public static class PassthroughPTF extends ProcessTableFunction<Row> {
        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
            collect(input);
        }
    }

    /** Filter PTF for testing scalar argument handling. */
    @DataTypeHint("ROW<value INT>")
    public static class FilterPTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input,
                @ArgumentHint(ArgumentTrait.SCALAR) Integer threshold) {
            // Use named field access - converter enriches Row with field names
            int value = input.getFieldAs("value");
            if (value >= threshold) {
                collect(input);
            }
        }
    }

    /** PTF for testing transformation of output types. */
    @DataTypeHint("ROW<doubled INT, original INT>")
    public static class DoublePTF extends ProcessTableFunction<Row> {
        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
            int value = (Integer) input.getField(0);
            collect(Row.of(value * 2, value));
        }
    }

    /** PTF with for testing table argument names set via argument hints. */
    @DataTypeHint("ROW<value INT>")
    public static class ExplicitNamePTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(value = ArgumentTrait.ROW_SEMANTIC_TABLE, name = "customName")
                        Row actualParamName) {
            collect(actualParamName);
        }
    }

    /** PTF with inline type annotation - no builder config needed. */
    @DataTypeHint("ROW<doubled INT>")
    public static class InlineTypePTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(
                                value = ArgumentTrait.ROW_SEMANTIC_TABLE,
                                type = @DataTypeHint("ROW<value INT>"))
                        Row input) {
            int value = (Integer) input.getField(0);
            collect(Row.of(value * 2));
        }
    }

    @DataTypeHint("ROW<value INT>")
    public static class PartitionedPTF extends ProcessTableFunction<Row> {
        public void eval(@ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
            collect(Row.of((Integer) input.getFieldAs("value")));
        }
    }

    /**
     * PTF with PASS_COLUMNS_THROUGH for validating that all input columns are prepended to output.
     */
    @DataTypeHint("ROW<doubled INT>")
    public static class PassColumnsThroughPTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.PASS_COLUMNS_THROUGH
                        })
                        Row input) {
            int value = (Integer) input.getField(1);
            collect(Row.of(value * 2));
        }
    }

    /** PTF with OPTIONAL_PARTITION_BY for validating that partition setup can be omitted. */
    @DataTypeHint("ROW<doubled INT>")
    public static class OptionalPartitionPTF extends ProcessTableFunction<Row> {

        public void eval(
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.OPTIONAL_PARTITION_BY
                        })
                        Row input) {
            int value = (Integer) input.getField(1);
            collect(Row.of(value * 2));
        }
    }

    /** Simple POJO for testing structured type input/output. */
    public static class User {
        public String name;
        public int age;

        public User() {}

        public User(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "User{name='" + name + "', age=" + age + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            User user = (User) o;
            return age == user.age && java.util.Objects.equals(name, user.name);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, age);
        }
    }

    /** PTF for testing structured type inputs. */
    @DataTypeHint("ROW<name STRING, age INT>")
    public static class UserPTF extends ProcessTableFunction<Row> {
        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) User user) {
            if (user.age >= 18) {
                collect(Row.of(user.name, user.age));
            }
        }
    }

    /** PTF that transforms structured type inputs and outputs. */
    public static class UserTransformPTF extends ProcessTableFunction<User> {
        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) User user) {
            User transformed = new User(user.name, user.age + 1);
            collect(transformed);
        }
    }

    /** Invalid PTF - uses reserved argument name "on_time". */
    @DataTypeHint("ROW<value INT>")
    public static class InvalidReservedArgOnTimePTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(value = ArgumentTrait.ROW_SEMANTIC_TABLE, name = "on_time")
                        Row input) {
            collect(input);
        }
    }

    /** Invalid PTF - uses reserved argument name "uid". */
    @DataTypeHint("ROW<value INT>")
    public static class InvalidReservedArgUidPTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input,
                @ArgumentHint(ArgumentTrait.SCALAR) String uid) {
            collect(input);
        }
    }

    /** Multi-table PTF for validating multi-input processing. */
    @DataTypeHint("ROW<output STRING>")
    public static class MultiTableJoinPTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row leftTable,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row rightTable) {
            if (leftTable != null) {
                collect(Row.of("LEFT: " + leftTable));
            }
            if (rightTable != null) {
                collect(Row.of("RIGHT: " + rightTable));
            }
        }
    }

    /**
     * Invalid PTF - uses PASS_COLUMNS_THROUGH with multiple table arguments (not allowed per Flink
     * docs).
     */
    @DataTypeHint("ROW<output STRING>")
    public static class InvalidPassColumnsThroughMultiTablePTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.PASS_COLUMNS_THROUGH
                        })
                        Row leftTable,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row rightTable) {
            if (leftTable != null) {
                collect(Row.of("LEFT: " + leftTable));
            }
            if (rightTable != null) {
                collect(Row.of("RIGHT: " + rightTable));
            }
        }
    }

    /** PTF with only scalar arguments, no tables. */
    @DataTypeHint("ROW<sum INT>")
    public static class ScalarOnlyPTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(ArgumentTrait.SCALAR) Integer a,
                @ArgumentHint(ArgumentTrait.SCALAR) Integer b) {
            collect(Row.of(a + b));
        }
    }

    /** PTF with Context parameter - should be rejected by test harness. */
    @DataTypeHint("ROW<value INT>")
    public static class PTFWithContext extends ProcessTableFunction<Row> {
        public void eval(Context ctx, @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
            collect(input);
        }
    }

    /** PTF with State parameter - should be rejected by test harness. */
    @DataTypeHint("ROW<value INT>")
    public static class PTFWithState extends ProcessTableFunction<Row> {
        public static class CountState {
            public long counter = 0L;
        }

        public void eval(
                @StateHint CountState state,
                @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
            collect(input);
        }
    }

    /** PTF with simple value state - counts rows per partition. */
    @DataTypeHint("ROW<count BIGINT>")
    public static class PTFWithValueState extends ProcessTableFunction<Row> {
        public static class CounterState {
            public long count = 0L;
        }

        public void eval(
                @StateHint CounterState state,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
            state.count++;
            collect(Row.of(state.count));
        }
    }

    /** PTF with ListView state - accumulates values in a list. */
    @DataTypeHint("ROW<values ARRAY<INT>>")
    public static class PTFWithListViewState extends ProcessTableFunction<Row> {
        public void eval(
                @StateHint(type = @DataTypeHint("ARRAY<INT>")) ListView<Integer> listState,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input)
                throws Exception {
            Integer value = input.getFieldAs("value");
            listState.add(value);

            // Collect all values as an array
            java.util.List<Integer> values = new java.util.ArrayList<>();
            for (Integer v : listState.get()) {
                values.add(v);
            }
            collect(Row.of((Object) values.toArray(new Integer[0])));
        }
    }

    /** PTF with MapView state - counts occurrences of each key. */
    @DataTypeHint("ROW<key STRING, count INT>")
    public static class PTFWithMapViewState extends ProcessTableFunction<Row> {
        public void eval(
                @StateHint MapView<String, Integer> mapState,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input)
                throws Exception {
            String key = input.getFieldAs("key");
            Integer count = mapState.get(key);
            if (count == null) {
                mapState.put(key, 1);
            } else {
                mapState.put(key, count + 1);
            }
            collect(Row.of(key, mapState.get(key)));
        }
    }

    // -------------------------------------------------------------------------
    // Builder Configuration Tests
    // -------------------------------------------------------------------------

    @Test
    void testBuilderRejectsDuplicateScalarArguments() {
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(FilterPTF.class)
                                    .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                                    .withScalarArgument("threshold", 50)
                                    .withScalarArgument("threshold", 100);
                        });

        assertThat(exception.getMessage()).contains("threshold");
    }

    @Test
    void testBuilderRejectsDuplicateTableArguments() {
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(MultiTableJoinPTF.class)
                                    .withTableArgument(
                                            "leftTable", DataTypes.of("ROW<id INT, name STRING>"))
                                    .withTableArgument(
                                            "leftTable", DataTypes.of("ROW<id INT, value INT>"));
                        });

        assertThat(exception.getMessage()).contains("leftTable");
    }

    @Test
    void testBuilderRejectsMixedDuplicateArguments() {
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(FilterPTF.class)
                                    .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                                    .withScalarArgument("input", 42);
                        });

        assertThat(exception.getMessage()).contains("input");
    }

    @Test
    void testBuilderRejectsReservedArgumentOnTime() {
        // We should reject PTFs that use reserved argument name "on_time"
        ProcessTableFunctionTestHarness.Builder harnessBuilder =
                ProcessTableFunctionTestHarness.ofClass(InvalidReservedArgOnTimePTF.class)
                        .withTableArgument("on_time", DataTypes.of("ROW<id INT>"));

        ValidationException exception =
                assertThrows(
                        ValidationException.class,
                        () -> {
                            harnessBuilder.build();
                        });

        assertThat(exception.getMessage())
                .contains("Function signature must not declare system arguments")
                .contains("on_time");
    }

    @Test
    void testBuilderRejectsReservedArgumentUid() {
        // We should reject PTFs that use reserved argument name "uid"
        ProcessTableFunctionTestHarness.Builder harnessBuilder =
                ProcessTableFunctionTestHarness.ofClass(InvalidReservedArgUidPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<id INT>"))
                        .withScalarArgument("uid", "my-id");

        ValidationException exception =
                assertThrows(
                        ValidationException.class,
                        () -> {
                            harnessBuilder.build();
                        });

        assertThat(exception.getMessage())
                .contains("Function signature must not declare system arguments")
                .contains("uid");
    }

    // -------------------------------------------------------------------------
    // Argument Configuration Tests
    // -------------------------------------------------------------------------

    @Test
    void testExplicitNameTakesPrecedence() throws Exception {
        // Verify that @ArgumentHint(name="customName") takes precedence over actual parameter
        // name when processing elements and calling eval.

        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(ExplicitNamePTF.class)
                        .withTableArgument("customName", DataTypes.of("ROW<value INT>"))
                        .build()) {

            harness.processElement(Row.of(42));
            harness.processElement(Row.of(100));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(2);
            assertThat(output.get(0).getField(0)).isEqualTo(42);
            assertThat(output.get(1).getField(0)).isEqualTo(100);
        }
    }

    @Test
    void testScalarOnlyPTF() throws Exception {
        // Test scalar-only PTF with no table arguments
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(ScalarOnlyPTF.class)
                        .withScalarArgument("a", 10)
                        .withScalarArgument("b", 20)
                        .build()) {

            harness.invoke();

            List<Row> output = harness.getOutput();

            assertThat(output).hasSize(1);
            assertThat(output.get(0).getField(0)).isEqualTo(30);
        }
    }

    @Test
    void testInvokeRejectsTableArguments() throws Exception {
        // Verify that invoke() rejects PTFs with table arguments
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(FilterPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                        .withScalarArgument("threshold", 50)
                        .build()) {

            Exception exception =
                    assertThrows(
                            IllegalStateException.class,
                            () -> {
                                harness.invoke();
                            });

            assertThat(exception.getMessage()).contains("invoke() is only for scalar-only PTFs");
        }
    }

    @Test
    void testTableProcessingWithScalarArgument() throws Exception {
        // Test a PTF that uses a scalar parameter
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(FilterPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                        .withScalarArgument("threshold", 50) // Scalar argument: threshold = 50
                        .build()) {

            harness.processElement(Row.of(25));
            harness.processElement(Row.of(75));
            harness.processElement(Row.of(50));
            harness.processElement(Row.of(10));
            harness.processElement(Row.of(100));

            List<Row> output = harness.getOutput();

            assertThat(output).hasSize(3);
            assertThat(output.get(0).getField(0)).isEqualTo(75);
            assertThat(output.get(1).getField(0)).isEqualTo(50);
            assertThat(output.get(2).getField(0)).isEqualTo(100);
        }
    }

    // -------------------------------------------------------------------------
    // Argument Trait Tests
    // -------------------------------------------------------------------------

    @Test
    void testProcessElementWithRowKind() throws Exception {
        // Verify RowKind is preserved through processing (ROW_SEMANTIC_TABLE)
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PassthroughPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                        .build()) {

            harness.processElement(RowKind.INSERT, 10);
            harness.processElement(RowKind.UPDATE_BEFORE, 15);
            harness.processElement(RowKind.UPDATE_AFTER, 20);
            harness.processElement(RowKind.DELETE, 30);

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(4);
            assertThat(output.get(0).getKind()).isEqualTo(RowKind.INSERT);
            assertThat(output.get(1).getKind()).isEqualTo(RowKind.UPDATE_BEFORE);
            assertThat(output.get(2).getKind()).isEqualTo(RowKind.UPDATE_AFTER);
            assertThat(output.get(3).getKind()).isEqualTo(RowKind.DELETE);
        }
    }

    @Test
    void testPassColumnsThroughTrait() throws Exception {
        // Verify PASS_COLUMNS_THROUGH prepends ALL input columns (not just partition keys)
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PassColumnsThroughPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<key STRING, value INT>"))
                        .withPartitionBy("input", "key")
                        .build()) {

            harness.processElement(Row.of("A", 10));
            harness.processElement(Row.of("B", 20));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(2);

            assertThat(output.get(0)).isEqualTo(Row.of("A", 10, 20));
            assertThat(output.get(1)).isEqualTo(Row.of("B", 20, 40));
        }
    }

    @Test
    void testOptionalPartitionByWithoutPartition() throws Exception {
        // Verify OPTIONAL_PARTITION_BY allows omitting partition configuration
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(OptionalPartitionPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<key STRING, value INT>"))
                        .build()) {

            harness.processElement(Row.of("A", 10));
            harness.processElement(Row.of("B", 20));
            harness.processElement(Row.of("C", 30));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(3);

            assertThat(output.get(0)).isEqualTo(Row.of(20));
            assertThat(output.get(1)).isEqualTo(Row.of(40));
            assertThat(output.get(2)).isEqualTo(Row.of(60));
        }
    }

    @Test
    void testOptionalPartitionByWithPartition() throws Exception {
        // Verify OPTIONAL_PARTITION_BY still works when partition is configured
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(OptionalPartitionPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<key STRING, value INT>"))
                        .withPartitionBy("input", "key")
                        .build()) {

            harness.processElement(Row.of("A", 10));
            harness.processElement(Row.of("A", 5));
            harness.processElement(Row.of("B", 20));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(3);

            assertThat(output.get(0)).isEqualTo(Row.of("A", 20));
            assertThat(output.get(1)).isEqualTo(Row.of("A", 10));
            assertThat(output.get(2)).isEqualTo(Row.of("B", 40));
        }
    }

    // -------------------------------------------------------------------------
    // Data Type Conversion Tests
    // -------------------------------------------------------------------------

    @Test
    void testNamedRowFieldOrdering() throws Exception {
        // Test what happens when Row field order differs from DataType schema order
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PassthroughPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<user STRING, value INT>"))
                        .build()) {

            Row rowA = Row.withNames();
            rowA.setField("value", 100);
            rowA.setField("user", "Alice");

            harness.processElement(rowA);

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(1);

            Row result = output.get(0);

            // Positional access follows schema order
            assertThat(result.getField(0)).isEqualTo("Alice");
            assertThat(result.getField(1)).isEqualTo(100);
        }
    }

    @Test
    void testPositionalRowWithWrongTypeOrder() throws Exception {
        // Verify that type mismatches are caught when Row values don't match schema types
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PassthroughPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<user STRING, value INT>"))
                        .build()) {

            Row wrongOrderRow = Row.of(10, "Alice");

            assertThrows(ClassCastException.class, () -> harness.processElement(wrongOrderRow));
        }
    }

    @Test
    void testStructuredTypeInput() throws Exception {
        // Test PTF that accepts structured types instead of Row
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(UserPTF.class)
                        .withTableArgument("user", DataTypes.of(User.class))
                        .build()) {

            harness.processElement(Row.of("Alice", 25));
            harness.processElement(Row.of("Bob", 17));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(1);

            Row result = output.get(0);
            assertThat(result.getField(0)).isEqualTo("Alice");
            assertThat(result.getField(1)).isEqualTo(25);
        }
    }

    @Test
    void testStructuredTypeInputAndOutput() throws Exception {
        // Test PTF with structured type inputs and outputs
        try (ProcessTableFunctionTestHarness<User> harness =
                ProcessTableFunctionTestHarness.ofClass(UserTransformPTF.class)
                        .withTableArgument("user", DataTypes.of(User.class))
                        .build()) {

            harness.processElement(Row.of("Alice", 25));

            List<User> output = harness.getOutput();
            assertThat(output).hasSize(1);

            User result = output.get(0);
            assertThat(result.getClass()).isEqualTo(User.class);
            assertThat(result.name).isEqualTo("Alice");
            assertThat(result.age).isEqualTo(26);
        }
    }

    // -------------------------------------------------------------------------
    // Partitioning Tests
    // -------------------------------------------------------------------------

    @Test
    void testSetSemanticWithPartitionByName() throws Exception {
        // Verify set-semantic table with partition configuration by column name
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PartitionedPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<key STRING, value INT>"))
                        .withPartitionBy("input", "key") // Partition by "key" column name
                        .build()) {

            harness.processElement(Row.of("X", 10));
            harness.processElement(Row.of("Y", 20));
            harness.processElement(Row.of("X", 30));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(3);
            assertThat(output.get(0)).isEqualTo(Row.of("X", 10));
            assertThat(output.get(1)).isEqualTo(Row.of("Y", 20));
            assertThat(output.get(2)).isEqualTo(Row.of("X", 30));
        }
    }

    @Test
    void testSetSemanticWithMultiplePartitionColumns() throws Exception {
        // Verify composite partition key (multiple columns)
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PartitionedPTF.class)
                        .withTableArgument(
                                "input",
                                DataTypes.of("ROW<region STRING, country STRING, value INT>"))
                        .withPartitionBy("input", "region", "country")
                        .build()) {

            harness.processElement(Row.of("EU", "DE", 100));
            harness.processElement(Row.of("EU", "DE", 200));
            harness.processElement(Row.of("EU", "FR", 300));
            harness.processElement(Row.of("US", "NY", 400));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(4);
        }
    }

    @Test
    void testMultipleSetSemanticTablesWithMatchingPartitionKeys() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(MultiTableJoinPTF.class)
                        .withTableArgument("leftTable", DataTypes.of("ROW<name STRING, score INT>"))
                        .withPartitionBy("leftTable", "name")
                        .withTableArgument(
                                "rightTable", DataTypes.of("ROW<name STRING, city STRING>"))
                        .withPartitionBy("rightTable", "name")
                        .build()) {

            harness.processElementForTable("leftTable", Row.of("Alice", 100));
            harness.processElementForTable("leftTable", Row.of("Bob", 200));

            harness.processElementForTable("rightTable", Row.of("Alice", "Berlin"));
            harness.processElementForTable("rightTable", Row.of("Bob", "London"));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(4);

            assertThat(output.get(0).getField(0)).isEqualTo("Alice");
            assertThat(output.get(0).getField(1)).isNull();
            assertThat(output.get(0).getField(2)).isEqualTo("LEFT: +I[Alice, 100]");

            assertThat(output.get(1).getField(0)).isEqualTo("Bob");
            assertThat(output.get(1).getField(1)).isNull();
            assertThat(output.get(1).getField(2)).isEqualTo("LEFT: +I[Bob, 200]");

            assertThat(output.get(2).getField(0)).isNull();
            assertThat(output.get(2).getField(1)).isEqualTo("Alice");
            assertThat(output.get(2).getField(2)).isEqualTo("RIGHT: +I[Alice, Berlin]");

            assertThat(output.get(3).getField(0)).isNull();
            assertThat(output.get(3).getField(1)).isEqualTo("Bob");
            assertThat(output.get(3).getField(2)).isEqualTo("RIGHT: +I[Bob, London]");
        }
    }

    @Test
    void testMultipleSetSemanticTablesWithMismatchedPartitionTypes() {
        // Verify that multi-table PTFs with inconsistent partition types are rejected
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(MultiTableJoinPTF.class)
                                    .withTableArgument(
                                            "leftTable", DataTypes.of("ROW<id INT, name STRING>"))
                                    .withPartitionBy("leftTable", "id")
                                    .withTableArgument(
                                            "rightTable",
                                            DataTypes.of("ROW<key STRING, city STRING>"))
                                    .withPartitionBy("rightTable", "key")
                                    .build();
                        });

        assertThat(exception.getMessage()).contains("Inconsistent partitioning");
    }

    @Test
    void testMultipleSetSemanticTablesWithMismatchedPartitionColumnCount() {
        // Verify that multi-table PTFs with different partition column counts are rejected
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(MultiTableJoinPTF.class)
                                    .withTableArgument(
                                            "leftTable",
                                            DataTypes.of("ROW<id INT, region STRING, name STRING>"))
                                    .withPartitionBy("leftTable", "id", "region")
                                    .withTableArgument(
                                            "rightTable", DataTypes.of("ROW<id INT, city STRING>"))
                                    .withPartitionBy("rightTable", "id")
                                    .build();
                        });

        assertThat(exception.getMessage()).contains("Inconsistent partitioning");
    }

    @Test
    void testPassColumnsThroughWithMultipleTablesRejected() {
        // Verify that PASS_COLUMNS_THROUGH is rejected when used with multiple table arguments
        Exception exception =
                assertThrows(
                        org.apache.flink.table.api.ValidationException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(
                                            InvalidPassColumnsThroughMultiTablePTF.class)
                                    .withTableArgument("leftTable", DataTypes.of("ROW<a INT>"))
                                    .withTableArgument("rightTable", DataTypes.of("ROW<b INT>"))
                                    .build();
                        });

        assertThat(exception.getMessage())
                .contains("Pass-through columns")
                .contains("multiple table arguments");
    }

    @Test
    void testInlineTypeAnnotation() throws Exception {
        // Verify that PTFs can declare table argument types via @ArgumentHint(type = ...)
        // without needing .withTableArgument() configuration
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(InlineTypePTF.class).build()) {

            harness.processElement(Row.of(5));
            harness.processElement(Row.of(10));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(2);
            assertThat(output.get(0)).isEqualTo(Row.of(10));
            assertThat(output.get(1)).isEqualTo(Row.of(20));
        }
    }

    @Test
    void testInlineTypeMatchesBuilderConfig() throws Exception {
        // Verify that when both inline annotation and builder config are provided with matching
        // types, the harness builds successfully
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(InlineTypePTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                        .build()) {

            harness.processElement(Row.of(7));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(1);
            assertThat(output.get(0)).isEqualTo(Row.of(14));
        }
    }

    @Test
    void testInlineTypeMismatchWithBuilderConfigRejected() {
        // Verify that when inline annotation and builder config specify different types,
        // build() throws an exception
        Exception exception =
                assertThrows(
                        IllegalStateException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(InlineTypePTF.class)
                                    .withTableArgument(
                                            "input", DataTypes.of("ROW<value BIGINT>")) // Mismatch!
                                    .build();
                        });

        assertThat(exception.getMessage())
                .contains("Type mismatch")
                .contains("input")
                .contains("INT")
                .contains("BIGINT");
    }

    // -------------------------------------------------------------------------
    // Element Processing Tests
    // -------------------------------------------------------------------------

    @Test
    void testProcessElementOnMultiTableThrows() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(MultiTableJoinPTF.class)
                        .withTableArgument("leftTable", DataTypes.of("ROW<id INT, name STRING>"))
                        .withTableArgument("rightTable", DataTypes.of("ROW<id INT, value STRING>"))
                        .withPartitionBy("leftTable", "id")
                        .withPartitionBy("rightTable", "id")
                        .build()) {

            Exception exception =
                    assertThrows(
                            IllegalStateException.class,
                            () -> harness.processElement(Row.of(1, "Alice")));
            assertThat(exception.getMessage())
                    .contains("multiple table arguments")
                    .contains("processElementForTable");
        }
    }

    @Test
    void testProcessElementForTableMultipleTables() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(MultiTableJoinPTF.class)
                        .withTableArgument("leftTable", DataTypes.of("ROW<id INT, name STRING>"))
                        .withTableArgument("rightTable", DataTypes.of("ROW<id INT, value STRING>"))
                        .withPartitionBy("leftTable", "id")
                        .withPartitionBy("rightTable", "id")
                        .build()) {

            harness.processElementForTable("leftTable", Row.of(1, "Alice"));
            harness.processElementForTable("leftTable", Row.of(2, "Bob"));

            harness.processElementForTable("rightTable", Row.of(1, "value1"));
            harness.processElementForTable("rightTable", Row.of(2, "value2"));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(4);

            assertThat(output.get(0).getField(0)).isEqualTo(1);
            assertThat(output.get(0).getField(1)).isNull();
            assertThat(output.get(0).getField(2)).asString().startsWith("LEFT:");

            assertThat(output.get(1).getField(0)).isEqualTo(2);
            assertThat(output.get(1).getField(1)).isNull();
            assertThat(output.get(1).getField(2)).asString().startsWith("LEFT:");

            assertThat(output.get(2).getField(0)).isNull();
            assertThat(output.get(2).getField(1)).isEqualTo(1);
            assertThat(output.get(2).getField(2)).asString().startsWith("RIGHT:");

            assertThat(output.get(3).getField(0)).isNull();
            assertThat(output.get(3).getField(1)).isEqualTo(2);
            assertThat(output.get(3).getField(2)).asString().startsWith("RIGHT:");
        }
    }

    @Test
    void testPassthroughEndToEnd() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PassthroughPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                        .build()) {

            harness.processElement(Row.of(42));
            harness.processElement(Row.of(100));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(2);
            assertThat(output.get(0).getField(0)).isEqualTo(42);
            assertThat(output.get(1).getField(0)).isEqualTo(100);
        }
    }

    // -------------------------------------------------------------------------
    // Output Collection Tests
    // -------------------------------------------------------------------------

    @Test
    void testClearOutput() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PassthroughPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                        .build()) {

            harness.processElement(Row.of(10));
            harness.processElement(Row.of(20));
            assertThat(harness.getOutput()).hasSize(2);

            harness.clearOutput();
            assertThat(harness.getOutput()).isEmpty();

            harness.processElement(Row.of(30));
            assertThat(harness.getOutput()).hasSize(1);
        }
    }

    // -------------------------------------------------------------------------
    // Error Cases Tests
    // -------------------------------------------------------------------------

    @Test
    void testProcessElementForTableWithInvalidName() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PassthroughPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                        .build()) {

            Exception exception =
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> harness.processElementForTable("nonexistent", Row.of(42)));
            assertThat(exception.getMessage()).contains("nonexistent");
        }
    }

    @Test
    void testContextParameterRejected() {
        Exception exception =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                ProcessTableFunctionTestHarness.ofClass(PTFWithContext.class)
                                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                                        .build());

        assertThat(exception.getMessage())
                .contains("does not yet support Context parameters")
                .contains("Context parameter")
                .contains("position 0");
    }

    @Test
    void testSetSemanticMissingPartitionConfigThrows() {
        Exception exception =
                assertThrows(
                        IllegalStateException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(PartitionedPTF.class)
                                    .withTableArgument(
                                            "input", DataTypes.of("ROW<key STRING, value INT>"))
                                    .build();
                        });

        assertThat(exception.getMessage()).contains("No partition configuration found");
        assertThat(exception.getMessage()).contains("withPartitionBy");
    }

    @Test
    void testPartitionByInvalidColumnName() {
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(PartitionedPTF.class)
                                    .withTableArgument(
                                            "input", DataTypes.of("ROW<key STRING, value INT>"))
                                    .withPartitionBy("input", "nonexistent")
                                    .build();
                        });

        assertThat(exception.getMessage()).contains("not found");
        assertThat(exception.getMessage()).contains("Available columns");
    }

    @Test
    void testPartitionByDuplicateConfigThrows() {
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(PartitionedPTF.class)
                                    .withTableArgument(
                                            "input", DataTypes.of("ROW<key STRING, value INT>"))
                                    .withPartitionBy("input", "key") // First config
                                    .withPartitionBy("input", "key"); // Duplicate - should fail
                        });

        assertThat(exception.getMessage()).contains("Partition config already exists");
    }

    // -------------------------------------------------------------------------
    // State Tests
    // -------------------------------------------------------------------------

    @Test
    void testSimpleValueState() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithValueState.class)
                        .withTableArgument("input", DataTypes.of("ROW<name STRING, value INT>"))
                        .withPartitionBy("input", "name")
                        .build();

        // Partition "Alice" - first row
        harness.processElementForTable("input", Row.of("Alice", 10));
        assertThat(harness.getOutput()).containsExactly(Row.of("Alice", 1L));
        PTFWithValueState.CounterState aliceState =
                harness.getStateForKey(
                        "state", Row.of("Alice"), PTFWithValueState.CounterState.class);
        assertThat(aliceState.count).isEqualTo(1L);
        harness.clearOutput();

        // Partition "Bob" - first row
        harness.processElementForTable("input", Row.of("Bob", 20));
        assertThat(harness.getOutput()).containsExactly(Row.of("Bob", 1L));
        PTFWithValueState.CounterState bobState =
                harness.getStateForKey(
                        "state", Row.of("Bob"), PTFWithValueState.CounterState.class);
        assertThat(bobState.count).isEqualTo(1L);
        harness.clearOutput();

        // Partition "Alice" - second row (state should persist)
        harness.processElementForTable("input", Row.of("Alice", 15));
        assertThat(harness.getOutput()).containsExactly(Row.of("Alice", 2L));
        aliceState =
                harness.getStateForKey(
                        "state", Row.of("Alice"), PTFWithValueState.CounterState.class);
        assertThat(aliceState.count).isEqualTo(2L);
        harness.clearOutput();

        // Partition "Bob" - second row (state should persist)
        harness.processElementForTable("input", Row.of("Bob", 25));
        assertThat(harness.getOutput()).containsExactly(Row.of("Bob", 2L));
        bobState =
                harness.getStateForKey(
                        "state", Row.of("Bob"), PTFWithValueState.CounterState.class);
        assertThat(bobState.count).isEqualTo(2L);
        harness.clearOutput();

        // Partition "Alice" - third row
        harness.processElementForTable("input", Row.of("Alice", 30));
        assertThat(harness.getOutput()).containsExactly(Row.of("Alice", 3L));
        aliceState =
                harness.getStateForKey(
                        "state", Row.of("Alice"), PTFWithValueState.CounterState.class);
        assertThat(aliceState.count).isEqualTo(3L);

        // Verify state keys
        java.util.Set<Row> keys = harness.getStateKeys("state");
        assertThat(keys).containsExactlyInAnyOrder(Row.of("Alice"), Row.of("Bob"));

        // Verify getAllState
        java.util.Map<Row, PTFWithValueState.CounterState> allState =
                harness.getAllState("state", PTFWithValueState.CounterState.class);
        assertThat(allState).hasSize(2);
        assertThat(allState.get(Row.of("Alice")).count).isEqualTo(3L);
        assertThat(allState.get(Row.of("Bob")).count).isEqualTo(2L);

        harness.close();
    }

    @Test
    void testListViewState() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithListViewState.class)
                        .withTableArgument("input", DataTypes.of("ROW<key STRING, value INT>"))
                        .withPartitionBy("input", "key")
                        .build();

        // Partition "A" - add first value
        harness.processElementForTable("input", Row.of("A", 1));
        assertThat(harness.getOutput()).containsExactly(Row.of("A", new Integer[] {1}));
        org.apache.flink.table.api.dataview.ListView<Integer> listStateA =
                harness.getStateForKey(
                        "listState",
                        Row.of("A"),
                        org.apache.flink.table.api.dataview.ListView.class);
        assertThat(listStateA.get()).containsExactly(1);
        harness.clearOutput();

        // Partition "A" - add second value (list should grow)
        harness.processElementForTable("input", Row.of("A", 2));
        assertThat(harness.getOutput()).containsExactly(Row.of("A", new Integer[] {1, 2}));
        listStateA =
                harness.getStateForKey(
                        "listState",
                        Row.of("A"),
                        org.apache.flink.table.api.dataview.ListView.class);
        assertThat(listStateA.get()).containsExactly(1, 2);
        harness.clearOutput();

        // Partition "B" - separate state
        harness.processElementForTable("input", Row.of("B", 10));
        assertThat(harness.getOutput()).containsExactly(Row.of("B", new Integer[] {10}));
        org.apache.flink.table.api.dataview.ListView<Integer> listStateB =
                harness.getStateForKey(
                        "listState",
                        Row.of("B"),
                        org.apache.flink.table.api.dataview.ListView.class);
        assertThat(listStateB.get()).containsExactly(10);
        harness.clearOutput();

        // Partition "A" - add third value
        harness.processElementForTable("input", Row.of("A", 3));
        assertThat(harness.getOutput()).containsExactly(Row.of("A", new Integer[] {1, 2, 3}));
        listStateA =
                harness.getStateForKey(
                        "listState",
                        Row.of("A"),
                        org.apache.flink.table.api.dataview.ListView.class);
        assertThat(listStateA.get()).containsExactly(1, 2, 3);

        harness.close();
    }

    @Test
    void testMapViewState() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithMapViewState.class)
                        .withTableArgument(
                                "input", DataTypes.of("ROW<partition STRING, key STRING>"))
                        .withPartitionBy("input", "partition")
                        .build();

        // Partition "P1" - first occurrence of key "foo"
        harness.processElementForTable("input", Row.of("P1", "foo"));
        assertThat(harness.getOutput()).containsExactly(Row.of("P1", "foo", 1));
        org.apache.flink.table.api.dataview.MapView<String, Integer> mapStateP1 =
                harness.getStateForKey(
                        "mapState",
                        Row.of("P1"),
                        org.apache.flink.table.api.dataview.MapView.class);
        assertThat(mapStateP1.get("foo")).isEqualTo(1);
        harness.clearOutput();

        // Partition "P1" - second occurrence of key "foo"
        harness.processElementForTable("input", Row.of("P1", "foo"));
        assertThat(harness.getOutput()).containsExactly(Row.of("P1", "foo", 2));
        mapStateP1 =
                harness.getStateForKey(
                        "mapState",
                        Row.of("P1"),
                        org.apache.flink.table.api.dataview.MapView.class);
        assertThat(mapStateP1.get("foo")).isEqualTo(2);
        harness.clearOutput();

        // Partition "P1" - first occurrence of key "bar"
        harness.processElementForTable("input", Row.of("P1", "bar"));
        assertThat(harness.getOutput()).containsExactly(Row.of("P1", "bar", 1));
        mapStateP1 =
                harness.getStateForKey(
                        "mapState",
                        Row.of("P1"),
                        org.apache.flink.table.api.dataview.MapView.class);
        assertThat(mapStateP1.get("foo")).isEqualTo(2);
        assertThat(mapStateP1.get("bar")).isEqualTo(1);
        harness.clearOutput();

        // Partition "P2" - separate state, same key "foo"
        harness.processElementForTable("input", Row.of("P2", "foo"));
        assertThat(harness.getOutput()).containsExactly(Row.of("P2", "foo", 1));
        org.apache.flink.table.api.dataview.MapView<String, Integer> mapStateP2 =
                harness.getStateForKey(
                        "mapState",
                        Row.of("P2"),
                        org.apache.flink.table.api.dataview.MapView.class);
        assertThat(mapStateP2.get("foo")).isEqualTo(1);
        harness.clearOutput();

        // Partition "P1" - third occurrence of key "foo"
        harness.processElementForTable("input", Row.of("P1", "foo"));
        assertThat(harness.getOutput()).containsExactly(Row.of("P1", "foo", 3));
        mapStateP1 =
                harness.getStateForKey(
                        "mapState",
                        Row.of("P1"),
                        org.apache.flink.table.api.dataview.MapView.class);
        assertThat(mapStateP1.get("foo")).isEqualTo(3);
        assertThat(mapStateP1.get("bar")).isEqualTo(1);

        harness.close();
    }

    @Test
    void testInitialStateSetup() throws Exception {
        // Create initial state
        PTFWithValueState.CounterState initialState = new PTFWithValueState.CounterState();
        initialState.count = 100L;

        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithValueState.class)
                        .withTableArgument("input", DataTypes.of("ROW<id INT>"))
                        .withPartitionBy("input", "id")
                        .withInitialStateArgument("state", Row.of(1), initialState)
                        .build();

        // Verify initial state was set
        PTFWithValueState.CounterState state =
                harness.getStateForKey("state", Row.of(1), PTFWithValueState.CounterState.class);
        assertThat(state).isNotNull();
        assertThat(state.count).isEqualTo(100L);

        // Process element - counter should start at 100
        harness.processElement(Row.of(1));
        assertThat(harness.getOutput()).containsExactly(Row.of(1, 101L));

        // Partition 2 should start with fresh state
        harness.processElement(Row.of(2));
        assertThat(harness.getOutput().get(1)).isEqualTo(Row.of(2, 1L));

        harness.close();
    }

    @Test
    void testInitialStateWithListView() throws Exception {
        // Create initial ListView state
        org.apache.flink.table.api.dataview.ListView<Integer> initialList =
                new org.apache.flink.table.api.dataview.ListView<>();
        initialList.add(10);
        initialList.add(20);
        initialList.add(30);

        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithListViewState.class)
                        .withTableArgument("input", DataTypes.of("ROW<id STRING, value INT>"))
                        .withPartitionBy("input", "id")
                        .withInitialStateArgument("listState", Row.of("P1"), initialList)
                        .build();

        // Verify initial state
        org.apache.flink.table.api.dataview.ListView<Integer> listState =
                harness.getStateForKey(
                        "listState",
                        Row.of("P1"),
                        org.apache.flink.table.api.dataview.ListView.class);
        assertThat(listState.get()).containsExactly(10, 20, 30);

        // Add another element - should append to existing list
        harness.processElement(Row.of("P1", 40));
        assertThat(harness.getOutput())
                .containsExactly(Row.of("P1", (Object) new Integer[] {10, 20, 30, 40}));

        listState =
                harness.getStateForKey(
                        "listState",
                        Row.of("P1"),
                        org.apache.flink.table.api.dataview.ListView.class);
        assertThat(listState.get()).containsExactly(10, 20, 30, 40);

        harness.close();
    }

    @Test
    void testInitialStateWithMapView() throws Exception {
        // Create initial MapView state
        org.apache.flink.table.api.dataview.MapView<String, Integer> initialMap =
                new org.apache.flink.table.api.dataview.MapView<>();
        initialMap.put("apple", 5);
        initialMap.put("banana", 10);

        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithMapViewState.class)
                        .withTableArgument(
                                "input", DataTypes.of("ROW<id STRING, key STRING, value INT>"))
                        .withPartitionBy("input", "id")
                        .withInitialStateArgument("mapState", Row.of("P1"), initialMap)
                        .build();

        // Verify initial state
        org.apache.flink.table.api.dataview.MapView<String, Integer> mapState =
                harness.getStateForKey(
                        "mapState",
                        Row.of("P1"),
                        org.apache.flink.table.api.dataview.MapView.class);
        assertThat(mapState.get("apple")).isEqualTo(5);
        assertThat(mapState.get("banana")).isEqualTo(10);

        // Add another entry - should merge with existing map
        harness.processElement(Row.of("P1", "cherry", 999));
        assertThat(harness.getOutput()).containsExactly(Row.of("P1", "cherry", 1));

        mapState =
                harness.getStateForKey(
                        "mapState",
                        Row.of("P1"),
                        org.apache.flink.table.api.dataview.MapView.class);
        assertThat(mapState.get("apple")).isEqualTo(5);
        assertThat(mapState.get("banana")).isEqualTo(10);
        assertThat(mapState.get("cherry")).isEqualTo(1);

        harness.close();
    }

    // -------------------------------------------------------------------------
    // State TTL Tests
    // -------------------------------------------------------------------------

    /** PTF with value state (POJO) that has TTL. */
    @DataTypeHint("ROW<count BIGINT>")
    public static class PTFWithValueStateTTL extends ProcessTableFunction<Row> {
        public static class CounterState {
            public long count = 0L;
        }

        public void eval(
                @StateHint(ttl = "1 s") CounterState state,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
            state.count++;
            collect(Row.of(state.count));
        }
    }

    @Test
    void testValueStateTtlExpiration() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithValueStateTTL.class)
                        .withTableArgument("input", DataTypes.of("ROW<id INT>"))
                        .withPartitionBy("input", "id")
                        .build();

        // Process element at time 0
        harness.processElement(Row.of(1));

        PTFWithValueStateTTL.CounterState state =
                harness.getStateForKey("state", Row.of(1), PTFWithValueStateTTL.CounterState.class);
        assertThat(state).isNotNull();
        assertThat(state.count).isEqualTo(1L);

        // Advance past TTL (1000ms)
        harness.advanceSystemClock(1001);

        // State should be expired
        state = harness.getStateForKey("state", Row.of(1), PTFWithValueStateTTL.CounterState.class);
        assertThat(state).isNull();

        harness.close();
    }

    /** PTF with ListView state that has per-element TTL. */
    @DataTypeHint("ROW<values ARRAY<INT>>")
    public static class PTFWithListViewTTL extends ProcessTableFunction<Row> {
        public void eval(
                @StateHint(ttl = "1 s")
                        org.apache.flink.table.api.dataview.ListView<Integer> listState,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input)
                throws Exception {
            int value = input.getFieldAs(1); // value is at index 1 (after id at index 0)
            listState.add(value);

            java.util.List<Integer> collected = new ArrayList<>();
            for (Integer v : listState.get()) {
                collected.add(v);
            }
            collect(Row.of((Object) collected.toArray(new Integer[0])));
        }
    }

    @Test
    void testListViewPerElementTtl() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithListViewTTL.class)
                        .withTableArgument("input", DataTypes.of("ROW<id INT, value INT>"))
                        .withPartitionBy("input", "id")
                        .build();

        // Add element 1 at time 0 (using partition key 0 for all rows to simulate global state)
        harness.processElement(Row.of(0, 1));
        assertThat(harness.getOutput()).containsExactly(Row.of(0, (Object) new Integer[] {1}));
        harness.clearOutput();

        // Advance 500ms, add element 2
        harness.advanceSystemClock(500);
        harness.processElement(Row.of(0, 2));
        assertThat(harness.getOutput()).containsExactly(Row.of(0, (Object) new Integer[] {1, 2}));
        harness.clearOutput();

        // Advance 600ms more (total 1100ms)
        // Element 1 expires (1100 > 1000), element 2 remains (600 < 1000)
        harness.advanceSystemClock(600);

        org.apache.flink.table.api.dataview.ListView<Integer> listState =
                harness.getStateForKey(
                        "listState", Row.of(0), org.apache.flink.table.api.dataview.ListView.class);
        assertThat(listState.get()).containsExactly(2); // Only element 2 remains

        harness.close();
    }

    /** PTF with MapView state that has per-entry TTL. */
    @DataTypeHint("ROW<size INT>")
    public static class PTFWithMapViewTTL extends ProcessTableFunction<Row> {
        public void eval(
                @StateHint(ttl = "1 s")
                        org.apache.flink.table.api.dataview.MapView<String, Integer> mapState,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input)
                throws Exception {
            String key = input.getFieldAs(1); // key is at index 1 (after id at index 0)
            Integer value = input.getFieldAs(2); // value is at index 2
            mapState.put(key, value);
            collect(Row.of(mapState.getMap().size()));
        }
    }

    @Test
    void testMapViewPerEntryTtl() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithMapViewTTL.class)
                        .withTableArgument(
                                "input", DataTypes.of("ROW<id INT, key STRING, value INT>"))
                        .withPartitionBy("input", "id")
                        .build();

        // Insert entry A at time 0
        harness.processElement(Row.of(0, "A", 1));
        harness.clearOutput();

        // Advance 500ms, insert entry B
        harness.advanceSystemClock(500);
        harness.processElement(Row.of(0, "B", 2));
        harness.clearOutput();

        // Advance 600ms more (total 1100ms)
        // Entry A expires (1100 > 1000), entry B remains (600 < 1000)
        harness.advanceSystemClock(600);

        org.apache.flink.table.api.dataview.MapView<String, Integer> mapState =
                harness.getStateForKey(
                        "mapState", Row.of(0), org.apache.flink.table.api.dataview.MapView.class);
        assertThat(mapState.getMap()).containsOnly(java.util.Map.entry("B", 2));

        harness.close();
    }

    /** PTF with zero TTL state. */
    @DataTypeHint("ROW<value INT>")
    public static class PTFWithZeroTTL extends ProcessTableFunction<Row> {
        public static class ZeroTtlState {
            public int value = 0;
        }

        public void eval(
                @StateHint(ttl = "0") ZeroTtlState state,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
            state.value++;
            collect(Row.of(state.value));
        }
    }

    @Test
    void testZeroTtl() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithZeroTTL.class)
                        .withTableArgument("input", DataTypes.of("ROW<id INT>"))
                        .withPartitionBy("input", "id")
                        .build();

        harness.processElement(Row.of(1));

        PTFWithZeroTTL.ZeroTtlState state =
                harness.getStateForKey("state", Row.of(1), PTFWithZeroTTL.ZeroTtlState.class);
        assertThat(state).isNotNull();

        // After advancing even 1ms, state expires
        harness.advanceSystemClock(1);

        state = harness.getStateForKey("state", Row.of(1), PTFWithZeroTTL.ZeroTtlState.class);
        assertThat(state).isNull();

        harness.close();
    }

    /** PTF with mixed state types having different TTLs. */
    @DataTypeHint("ROW<count INT>")
    public static class PTFWithMixedStateTTL extends ProcessTableFunction<Row> {
        public static class ValueState {
            public int count = 0;
        }

        public void eval(
                @StateHint(ttl = "500 ms") ValueState valueState,
                @StateHint(ttl = "1 s")
                        org.apache.flink.table.api.dataview.ListView<Integer> listState,
                @StateHint(ttl = "1500 ms")
                        org.apache.flink.table.api.dataview.MapView<String, Integer> mapState,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input)
                throws Exception {
            valueState.count++;
            listState.add(1);
            mapState.put("key", 1);
            collect(Row.of(valueState.count));
        }
    }

    @Test
    void testMixedStateTtls() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithMixedStateTTL.class)
                        .withTableArgument("input", DataTypes.of("ROW<id INT>"))
                        .withPartitionBy("input", "id")
                        .build();

        harness.processElement(Row.of(1));

        // After 600ms: valueState expired, listState and mapState remain
        harness.advanceSystemClock(600);

        PTFWithMixedStateTTL.ValueState vs =
                harness.getStateForKey(
                        "valueState", Row.of(1), PTFWithMixedStateTTL.ValueState.class);
        org.apache.flink.table.api.dataview.ListView<Integer> ls =
                harness.getStateForKey(
                        "listState", Row.of(1), org.apache.flink.table.api.dataview.ListView.class);
        org.apache.flink.table.api.dataview.MapView<String, Integer> ms =
                harness.getStateForKey(
                        "mapState", Row.of(1), org.apache.flink.table.api.dataview.MapView.class);

        assertThat(vs).isNull(); // Expired
        assertThat(ls.get()).hasSize(1); // Still valid
        assertThat(ms.getMap()).hasSize(1); // Still valid

        // After 1100ms total: listState expired, mapState remains
        harness.advanceSystemClock(500);

        ls =
                harness.getStateForKey(
                        "listState", Row.of(1), org.apache.flink.table.api.dataview.ListView.class);
        ms =
                harness.getStateForKey(
                        "mapState", Row.of(1), org.apache.flink.table.api.dataview.MapView.class);

        assertThat(ls.get()).isEmpty(); // Expired (entries removed)
        assertThat(ms.getMap()).hasSize(1); // Still valid

        // After 1600ms total: all expired
        harness.advanceSystemClock(500);

        ms =
                harness.getStateForKey(
                        "mapState", Row.of(1), org.apache.flink.table.api.dataview.MapView.class);
        assertThat(ms.getMap()).isEmpty(); // Expired

        harness.close();
    }

    @Test
    void testTimeOverloadMethods() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithValueStateTTL.class)
                        .withTableArgument("input", DataTypes.of("ROW<id INT>"))
                        .withPartitionBy("input", "id")
                        .build();

        // Create state at time 0
        harness.processElement(Row.of(1));
        PTFWithValueStateTTL.CounterState state =
                harness.getStateForKey("state", Row.of(1), PTFWithValueStateTTL.CounterState.class);
        assertThat(state).isNotNull();

        // Test long millis - advance by 1001ms (state with 1s TTL should expire)
        harness.advanceSystemClock(1001L);
        state = harness.getStateForKey("state", Row.of(1), PTFWithValueStateTTL.CounterState.class);
        assertThat(state).isNull();

        // Create new state at 1001ms
        harness.processElement(Row.of(2));

        // Test Instant overload - advance to 2002ms (1001ms elapsed since creation)
        harness.advanceSystemClock(java.time.Instant.ofEpochMilli(2002));
        state = harness.getStateForKey("state", Row.of(2), PTFWithValueStateTTL.CounterState.class);
        assertThat(state).isNull();

        harness.close();
    }

    @Test
    void testSetStateForKeyWithListViewTtl() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithListViewTTL.class)
                        .withTableArgument("input", DataTypes.of("ROW<id INT, value INT>"))
                        .withPartitionBy("input", "id")
                        .build();

        // Add element 1 at time 0
        harness.processElement(Row.of(0, 1));
        harness.clearOutput();

        // Advance 500ms, add element 2
        harness.advanceSystemClock(500);
        harness.processElement(Row.of(0, 2));
        harness.clearOutput();

        // Advance 300ms (total 800ms)
        harness.advanceSystemClock(300);

        // Now use setStateForKey to replace the list entirely
        org.apache.flink.table.api.dataview.ListView<Integer> newList =
                new org.apache.flink.table.api.dataview.ListView<>();
        newList.add(10);
        newList.add(20);
        harness.setStateForKey("listState", Row.of(0), newList);

        // Advance 300ms more (total 1100ms from start)
        // Original elements (1, 2) would have expired by now (added at 0ms and 500ms)
        // But the new elements (10, 20) were added at 800ms, so they should survive
        harness.advanceSystemClock(300);

        org.apache.flink.table.api.dataview.ListView<Integer> listState =
                harness.getStateForKey(
                        "listState", Row.of(0), org.apache.flink.table.api.dataview.ListView.class);

        // Expected: Elements 10 and 20 should still be present (added at 800ms, TTL=1000ms,
        // current=1100ms)
        // Bug: If setList() assigns currentTimeMillis, elements get timestamp 800ms and should
        // survive
        // But original implementation may lose timestamps entirely
        assertThat(listState.get()).containsExactly(10, 20);

        // Advance past TTL for the new elements (800ms + 1000ms = 1800ms)
        harness.advanceSystemClock(800); // Now at 1900ms

        listState =
                harness.getStateForKey(
                        "listState", Row.of(0), org.apache.flink.table.api.dataview.ListView.class);
        assertThat(listState.get()).isEmpty(); // Should be expired now

        harness.close();
    }

    @Test
    void testSetStateForKeyWithMapViewTtl() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithMapViewTTL.class)
                        .withTableArgument(
                                "input", DataTypes.of("ROW<id INT, key STRING, value INT>"))
                        .withPartitionBy("input", "id")
                        .build();

        // Insert entry A at time 0
        harness.processElement(Row.of(0, "A", 1));
        harness.clearOutput();

        // Advance 500ms, insert entry B
        harness.advanceSystemClock(500);
        harness.processElement(Row.of(0, "B", 2));
        harness.clearOutput();

        // Advance 300ms (total 800ms)
        harness.advanceSystemClock(300);

        // Use setStateForKey to replace the map entirely
        org.apache.flink.table.api.dataview.MapView<String, Integer> newMap =
                new org.apache.flink.table.api.dataview.MapView<>();
        newMap.put("X", 10);
        newMap.put("Y", 20);
        harness.setStateForKey("mapState", Row.of(0), newMap);

        // Advance 300ms more (total 1100ms from start)
        harness.advanceSystemClock(300);

        org.apache.flink.table.api.dataview.MapView<String, Integer> mapState =
                harness.getStateForKey(
                        "mapState", Row.of(0), org.apache.flink.table.api.dataview.MapView.class);

        // Expected: Entries X and Y should still be present (added at 800ms)
        assertThat(mapState.getMap()).containsOnly(Map.entry("X", 10), Map.entry("Y", 20));

        // Advance past TTL for the new entries
        harness.advanceSystemClock(800); // Now at 1900ms

        mapState =
                harness.getStateForKey(
                        "mapState", Row.of(0), org.apache.flink.table.api.dataview.MapView.class);
        assertThat(mapState.getMap()).isEmpty(); // Should be expired now

        harness.close();
    }
}
