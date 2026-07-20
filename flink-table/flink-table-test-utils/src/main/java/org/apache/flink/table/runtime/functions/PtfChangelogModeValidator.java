/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validates the changelog-mode configuration of a {@link ProcessTableFunction} under test: the
 * per-argument input modes and upsert keys against the arguments' declared traits, and the output
 * mode against changelog deliverability rules.
 */
@Internal
final class PtfChangelogModeValidator {

    private final List<ProcessTableFunctionTestHarness.TableArgumentInfo> tableArguments;
    private final Map<String, ChangelogMode> tableArgumentChangelogModes;
    private final Map<String, String[]> tableArgumentUpsertKeys;
    @Nullable private final String onTimeColumnName;

    PtfChangelogModeValidator(
            List<ProcessTableFunctionTestHarness.TableArgumentInfo> tableArguments,
            Map<String, ChangelogMode> tableArgumentChangelogModes,
            Map<String, String[]> tableArgumentUpsertKeys,
            @Nullable String onTimeColumnName) {
        this.tableArguments = tableArguments;
        this.tableArgumentChangelogModes = tableArgumentChangelogModes;
        this.tableArgumentUpsertKeys = tableArgumentUpsertKeys;
        this.onTimeColumnName = onTimeColumnName;
    }

    /**
     * Validates the per-argument changelog configuration: unknown argument names, SUPPORT_UPDATES
     * requiring an explicit mode, REQUIRE_UPDATE_BEFORE/REQUIRE_FULL_DELETE trait compatibility,
     * upsert mode requiring set semantics with matching partition columns, upsert-key column
     * validity, and on-time column existence.
     *
     * @throws IllegalArgumentException if configuration references unknown arguments
     * @throws IllegalStateException if configuration violates documented constraints
     */
    void validateConfiguration() {
        Set<String> validTableArgNames =
                tableArguments.stream().map(t -> t.name).collect(Collectors.toSet());
        for (String argName : tableArgumentChangelogModes.keySet()) {
            if (!validTableArgNames.contains(argName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Unknown table argument: '%s'. Available table arguments: %s",
                                argName, validTableArgNames));
            }
        }

        for (Map.Entry<String, String[]> entry : tableArgumentUpsertKeys.entrySet()) {
            String argName = entry.getKey();
            String[] upsertKeyColumns = entry.getValue();

            ProcessTableFunctionTestHarness.TableArgumentInfo tableArg =
                    tableArguments.stream()
                            .filter(t -> t.name.equals(argName))
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    String.format(
                                                            "Unknown table argument for upsert key: '%s'. "
                                                                    + "Available table arguments: %s",
                                                            argName, validTableArgNames)));

            validateUpsertKeyColumnNames(tableArg, upsertKeyColumns);
        }

        // Check on-time column existence first so a missing column reports "does not exist"
        // rather than a misleading "not supported for PTFs that consume or produce updates" later.
        if (onTimeColumnName != null) {
            boolean foundInAnyTable =
                    tableArguments.stream()
                            .anyMatch(
                                    t ->
                                            ProcessTableFunctionTestHarness.getFieldNames(
                                                            t.dataType)
                                                    .contains(onTimeColumnName));
            if (!foundInAnyTable) {
                throw new IllegalArgumentException(
                        String.format(
                                "withOnTimeColumn references column '%s' which does not exist in any "
                                        + "table argument. Available table arguments and their columns: %s",
                                onTimeColumnName,
                                tableArguments.stream()
                                        .collect(
                                                Collectors.toMap(
                                                        t -> t.name,
                                                        t ->
                                                                ProcessTableFunctionTestHarness
                                                                        .getFieldNames(
                                                                                t.dataType)))));
            }
        }

        for (ProcessTableFunctionTestHarness.TableArgumentInfo tableArg : tableArguments) {
            if (tableArg.is(StaticArgumentTrait.SUPPORT_UPDATES)) {
                if (tableArg.changelogMode == null) {
                    throw new IllegalStateException(
                            String.format(
                                    "Table argument '%s' declares SUPPORT_UPDATES but no changelog mode "
                                            + "was configured. Use .withTableArgumentChangelogMode(\"%s\", ...) "
                                            + "to specify what changelog mode this argument receives.",
                                    tableArg.name, tableArg.name));
                }

                ChangelogMode mode = tableArg.changelogMode;

                String violation = getDeliverabilityViolation(mode);
                if (violation != null) {
                    throw new IllegalStateException(
                            String.format(
                                    "Table argument '%s' is configured with changelog mode %s, which violates a deliverability constraint: %s",
                                    tableArg.name, mode, violation));
                }

                // Insert-only mode is legal even with REQUIRE_UPDATE_BEFORE since it has no
                // updates to encode
                if (tableArg.is(StaticArgumentTrait.REQUIRE_UPDATE_BEFORE)
                        && !mode.containsOnly(RowKind.INSERT)) {
                    if (!mode.contains(RowKind.UPDATE_BEFORE)) {
                        throw new IllegalStateException(
                                String.format(
                                        "Table argument '%s' declares REQUIRE_UPDATE_BEFORE but "
                                                + "configured mode %s does not include UPDATE_BEFORE.",
                                        tableArg.name, mode));
                    }
                }

                if (tableArg.is(StaticArgumentTrait.REQUIRE_FULL_DELETE)) {
                    if (mode.keyOnlyDeletes()) {
                        throw new IllegalStateException(
                                String.format(
                                        "Table argument '%s' declares REQUIRE_FULL_DELETE but "
                                                + "configured mode %s has keyOnlyDeletes=true.",
                                        tableArg.name, mode));
                    }
                }

                // An update without UPDATE_BEFORE is upsert-style and needs a co-located key; a
                // retract (with UPDATE_BEFORE) carries its own before-image and needs none.
                boolean isUpsertStyleInput =
                        !mode.containsOnly(RowKind.INSERT) && !mode.contains(RowKind.UPDATE_BEFORE);
                if (isUpsertStyleInput) {
                    if (!tableArg.isPartitioned()) {
                        throw new IllegalStateException(
                                String.format(
                                        "Table argument '%s' is configured with upsert mode %s, "
                                                + "but this is only possible for SET_SEMANTIC_TABLE arguments "
                                                + "with non-empty PARTITION BY columns. "
                                                + "ROW_SEMANTIC_TABLE arguments or arguments with no "
                                                + "partitioning can only use insertOnly() or all() modes.",
                                        tableArg.name, mode));
                    }
                    String[] upsertKey = tableArg.upsertKey;
                    if (upsertKey == null) {
                        throw new IllegalStateException(
                                String.format(
                                        "Table argument '%s' is configured with upsert mode %s, "
                                                + "but no upsert key was configured. "
                                                + "Use .withTableArgumentUpsertKey(\"%s\", ...) to specify "
                                                + "the upsert key columns.",
                                        tableArg.name, mode, tableArg.name));
                    }

                    Set<String> partitionCols =
                            new HashSet<>(Arrays.asList(tableArg.partitionColumnNames));
                    Set<String> upsertKeyCols = new HashSet<>(Arrays.asList(upsertKey));
                    if (!partitionCols.equals(upsertKeyCols)) {
                        throw new IllegalStateException(
                                String.format(
                                        "Table argument '%s' partition columns %s do not cover the "
                                                + "same set of columns as the configured upsert key %s. "
                                                + "For upsert input modes, partition columns must contain "
                                                + "the exact same set of columns as the upsert key "
                                                + "(order-independent).",
                                        tableArg.name,
                                        Arrays.toString(tableArg.partitionColumnNames),
                                        Arrays.toString(upsertKey)));
                    }
                }
            } else {
                // A non-SUPPORT_UPDATES argument is contractually insert-only, so any other
                // configured mode describes a stream the planner could never deliver here.
                ChangelogMode configured = tableArg.changelogMode;
                if (configured != null && !configured.equals(ChangelogMode.insertOnly())) {
                    throw new IllegalStateException(
                            String.format(
                                    "Table argument '%s' does not declare SUPPORT_UPDATES, so "
                                            + "its changelog mode must be insertOnly(). "
                                            + "Configured mode: %s",
                                    tableArg.name, configured));
                }
            }
        }
    }

    /**
     * Validates resolved output changelog mode against deliverability rules and on-time
     * compatibility.
     *
     * @param outputMode the resolved output changelog mode
     * @param sourceDescription where the mode came from, used to attribute error messages
     * @param isChangelogFunction whether the PTF implements ChangelogFunction
     * @throws IllegalStateException if output mode is not deliverable or violates constraints
     */
    void validateResolvedOutputMode(
            ChangelogMode outputMode, String sourceDescription, boolean isChangelogFunction) {
        if (!isDeliverableChangelogMode(outputMode)) {
            throw new IllegalStateException(
                    String.format(
                            "Output changelog mode %s (%s) violates deliverability: %s",
                            outputMode, sourceDescription, getDeliverabilityViolation(outputMode)));
        }

        if (!isChangelogFunction && !outputMode.equals(ChangelogMode.insertOnly())) {
            throw new IllegalStateException(
                    "Function does not implement ChangelogFunction, so output mode must be "
                            + "insertOnly(). Configured mode: "
                            + outputMode);
        }

        validateOnTimeCompatibleWithChangelogModes(outputMode);
        validateUpsertOutputRequiresSetSemantics(outputMode);
    }

    private void validateOnTimeCompatibleWithChangelogModes(ChangelogMode outputMode) {
        if (onTimeColumnName == null) {
            return;
        }
        boolean anyUpdatingInput =
                tableArguments.stream()
                        .anyMatch(t -> !t.effectiveChangelogMode().containsOnly(RowKind.INSERT));
        boolean updatingOutput = !outputMode.containsOnly(RowKind.INSERT);
        if (anyUpdatingInput || updatingOutput) {
            throw new IllegalStateException(
                    "Time operations using the `on_time` argument are currently not "
                            + "supported for PTFs that consume or produce updates.");
        }
    }

    private void validateUpsertOutputRequiresSetSemantics(ChangelogMode outputMode) {
        if (outputMode.containsOnly(RowKind.INSERT) || outputMode.contains(RowKind.UPDATE_BEFORE)) {
            return;
        }
        for (ProcessTableFunctionTestHarness.TableArgumentInfo tableArg : tableArguments) {
            if (!tableArg.isSetSemantic()) {
                throw new IllegalStateException(
                        String.format(
                                "PTFs that take table arguments with row semantics don't "
                                        + "support upsert output. Table argument '%s' must "
                                        + "use set semantics.",
                                tableArg.name));
            }
        }
    }

    private void validateUpsertKeyColumnNames(
            ProcessTableFunctionTestHarness.TableArgumentInfo tableArg, String[] upsertKeyColumns) {
        List<String> fieldNames = ProcessTableFunctionTestHarness.getFieldNames(tableArg.dataType);
        for (String columnName : upsertKeyColumns) {
            if (!fieldNames.contains(columnName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Upsert key column '%s' not found in table argument '%s'. "
                                        + "Available fields: %s",
                                columnName, tableArg.name, fieldNames));
            }
        }
    }

    private static boolean isDeliverableChangelogMode(ChangelogMode mode) {
        return getDeliverabilityViolation(mode) == null;
    }

    /**
     * Identifies which structural constraint a changelog mode violates, if any.
     *
     * <p>Returns a constraint-specific error message if the mode violates one of: (1) UPDATE_BEFORE
     * present without UPDATE_AFTER, (2) keyOnlyDeletes set without DELETE, or (3) keyOnlyDeletes
     * set together with UPDATE_BEFORE. Returns null if the mode is deliverable.
     */
    private static String getDeliverabilityViolation(ChangelogMode mode) {
        if (mode.contains(RowKind.UPDATE_BEFORE) && !mode.contains(RowKind.UPDATE_AFTER)) {
            return "UPDATE_BEFORE requires UPDATE_AFTER to be present";
        }
        if (mode.keyOnlyDeletes() && !mode.contains(RowKind.DELETE)) {
            return "keyOnlyDeletes requires DELETE to be present";
        }
        if (mode.keyOnlyDeletes() && mode.contains(RowKind.UPDATE_BEFORE)) {
            return "keyOnlyDeletes cannot combine with UPDATE_BEFORE (a retract stream always carries full deletes)";
        }
        return null;
    }
}
