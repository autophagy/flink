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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PtfChangelogModeResolver}, exercising it directly against mock {@link
 * ChangelogFunction} implementations rather than through {@link
 * ProcessTableFunctionTestHarness.Builder}.
 *
 * <p>The resolver probes the function in up to three phases, each carrying a different required
 * ("hint") mode: phase-1 asks for the broadest mode, phase-2 settles whether UPDATE_BEFORE is
 * needed, and phase-3 settles the delete shape. Tests assert both the resolved mode and, where the
 * probe sequence is the point, the hints received.
 */
class PtfChangelogModeResolverTest {

    private static final ChangelogMode HINT_PHASE_1 = ChangelogMode.upsert(false);

    /**
     * A {@link ChangelogFunction} whose answer is driven by a supplied behavior function, and which
     * records every call's received {@link ChangelogFunction.ChangelogContext} for later
     * assertions.
     */
    private static class RecordingChangelogFunction implements ChangelogFunction {
        private final Function<ChangelogContext, ChangelogMode> behavior;
        private final List<ChangelogMode> receivedHints = new ArrayList<>();
        private int callCount = 0;

        RecordingChangelogFunction(Function<ChangelogContext, ChangelogMode> behavior) {
            this.behavior = behavior;
        }

        /**
         * Always returns the same {@link ChangelogMode}, regardless of context — a valid pattern
         * per {@link ChangelogFunction}'s own Javadoc.
         */
        static RecordingChangelogFunction fixed(ChangelogMode mode) {
            return new RecordingChangelogFunction(ctx -> mode);
        }

        /** Answers based on the call index (1-based), for phase-dependent behavior. */
        static RecordingChangelogFunction perCall(Function<Integer, ChangelogMode> byCallIndex) {
            int[] calls = {0};
            return new RecordingChangelogFunction(ctx -> byCallIndex.apply(++calls[0]));
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogContext ctx) {
            callCount++;
            receivedHints.add(ctx.getRequiredChangelogMode());
            return behavior.apply(ctx);
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            throw new UnsupportedOperationException("Not used by these tests.");
        }

        @Override
        public FunctionKind getKind() {
            throw new UnsupportedOperationException("Not used by these tests.");
        }
    }

    // -------------------------------------------------------------------------
    // Probe sequencing
    // -------------------------------------------------------------------------

    @Test
    void testInsertOnlyAnswerStopsAtAnyProbe() {
        RecordingChangelogFunction fn =
                RecordingChangelogFunction.fixed(ChangelogMode.insertOnly());

        assertThat(resolve(fn)).isEqualTo(ChangelogMode.insertOnly());
        assertThat(fn.callCount).isEqualTo(1);
        assertThat(fn.receivedHints).containsExactly(HINT_PHASE_1);
    }

    @Test
    void testUpsertAnswerReachesAndIsAcceptedAtDeleteShapeProbe() {
        // A fixed answer of upsert(true) satisfies phase-1 (not insert-only) and phase-2
        // (doesn't contain UPDATE_BEFORE), so the resolver reaches phase-3 to probe delete shape
        // and accepts this answer as final. A fixed mock can't demonstrate that phase-3's hint is
        // honored (it ignores hints by definition); that's covered separately, below, by a
        // hint-sensitive mock.
        RecordingChangelogFunction fn =
                RecordingChangelogFunction.fixed(ChangelogMode.upsert(true));

        assertThat(resolve(fn)).isEqualTo(ChangelogMode.upsert(true));
        assertThat(fn.callCount).isEqualTo(3);
        assertThat(fn.receivedHints)
                .containsExactly(
                        HINT_PHASE_1, ChangelogMode.upsert(false), ChangelogMode.upsert(true));
    }

    @Test
    void testDeleteShapeProbeAcceptsFullDeleteWhenFunctionIgnoresKeyOnlyHint() {
        // A fixed answer of upsert(false) satisfies phase-1 and phase-2 probes the same way,
        // but ignores phase-3's key-only-deletes ask (still reports full deletes) — the resolver
        // must accept that as final rather than fail or fall back further, since full delete is a
        // strictly compatible superset of key-only.
        RecordingChangelogFunction fn =
                RecordingChangelogFunction.fixed(ChangelogMode.upsert(false));

        assertThat(resolve(fn)).isEqualTo(ChangelogMode.upsert(false));
        assertThat(fn.callCount).isEqualTo(3);
    }

    @Test
    void testUpsertOrRetractProbeEscalatesToRetractWhenFunctionInsistsOnUpdateBefore() {
        // Phase-2 insists on UPDATE_BEFORE, so phase-3 is skipped (UPDATE_BEFORE mandates full
        // deletes anyway). Resolved mode contains UPDATE_BEFORE from phase-2 answer.
        RecordingChangelogFunction fn = RecordingChangelogFunction.fixed(ChangelogMode.all());

        assertThat(resolve(fn)).isEqualTo(ChangelogMode.all());
        assertThat(fn.callCount).isEqualTo(2);
        assertThat(fn.receivedHints).containsExactly(HINT_PHASE_1, ChangelogMode.upsert(false));
    }

    @Test
    void testPhase3ProbeSkippedWhenPhase1HasNoDelete() {
        // When phase-1 result doesn't contain DELETE, phase-3 probe is skipped even if phase-2
        // says no UPDATE_BEFORE. Modes without DELETE never need key-only-deletes distinction.
        RecordingChangelogFunction fn =
                new RecordingChangelogFunction(
                        ctx -> {
                            if (ctx.getRequiredChangelogMode().keyOnlyDeletes()) {
                                throw new AssertionError(
                                        "Phase-3 probe should not run when phase-1 lacks DELETE");
                            }
                            return ChangelogMode.newBuilder()
                                    .addContainedKind(RowKind.INSERT)
                                    .addContainedKind(RowKind.UPDATE_AFTER)
                                    .build();
                        });

        ChangelogMode result = resolve(fn);

        assertThat(result.contains(RowKind.DELETE)).isFalse();
        assertThat(result.keyOnlyDeletes()).isFalse();
        assertThat(fn.callCount).isEqualTo(2);
    }

    // -------------------------------------------------------------------------
    // Assembling the resolved mode from phase answers
    // -------------------------------------------------------------------------

    @Test
    void testUpdateBeforeAddedWhenOnlyPhase2AsksForIt() {
        // Phase-1 returns upsert(true) (lacks UPDATE_BEFORE); phase-2 returns all(). The resolved
        // mode must include UPDATE_BEFORE from the phase-2 answer even though phase-1 omitted it.
        RecordingChangelogFunction fn =
                RecordingChangelogFunction.perCall(
                        call -> call == 1 ? ChangelogMode.upsert(true) : ChangelogMode.all());

        assertThat(resolve(fn)).isEqualTo(ChangelogMode.all());
    }

    @Test
    void testUpdateBeforeNotLeakedFromHintWhenFunctionDoesNotAskForIt() {
        // The reverse direction: the function returns all() only when UPDATE_BEFORE is requested,
        // and the derived hints never request it. Without proper filtering, UPDATE_BEFORE would
        // leak into the result.
        RecordingChangelogFunction fn =
                new RecordingChangelogFunction(
                        ctx ->
                                ctx.getRequiredChangelogMode().contains(RowKind.UPDATE_BEFORE)
                                        ? ChangelogMode.all()
                                        : ChangelogMode.upsert(true));

        ChangelogMode result = resolve(fn);

        assertThat(result).isEqualTo(ChangelogMode.upsert(true));
        assertThat(result.contains(RowKind.UPDATE_BEFORE)).isFalse();
    }

    @Test
    void testUpdateBeforeNotAddedWhenPhase1LacksUpdateAfter() {
        // UPDATE_BEFORE is only valid alongside UPDATE_AFTER, so a phase-2 answer of all() must
        // not introduce it when phase-1 produced [INSERT, DELETE].
        RecordingChangelogFunction fn =
                RecordingChangelogFunction.perCall(
                        call ->
                                call == 1
                                        ? ChangelogMode.newBuilder()
                                                .addContainedKind(RowKind.INSERT)
                                                .addContainedKind(RowKind.DELETE)
                                                .build()
                                        : ChangelogMode.all());

        ChangelogMode result = resolve(fn);

        assertThat(result.contains(RowKind.UPDATE_AFTER)).isFalse();
        assertThat(result.contains(RowKind.UPDATE_BEFORE)).isFalse();
        assertThat(result.contains(RowKind.DELETE)).isTrue();
    }

    @Test
    void testDeleteShapeAnswerDoesNotOverrideAnyProbesKindSet() {
        // Phase-3 must not override the kind set from phase-1, only the keyOnlyDeletes bit. Here
        // phase-3 (hint keyOnlyDeletes=true) degrades to insertOnly(); the upsert kind set from
        // phase-1 must survive with keyOnlyDeletes=false.
        RecordingChangelogFunction fn =
                new RecordingChangelogFunction(
                        ctx -> {
                            ChangelogMode hint = ctx.getRequiredChangelogMode();
                            if (hint.contains(RowKind.UPDATE_BEFORE) || !hint.keyOnlyDeletes()) {
                                return ChangelogMode.upsert(false);
                            }
                            return ChangelogMode.insertOnly();
                        });

        assertThat(resolve(fn)).isEqualTo(ChangelogMode.upsert(false));
    }

    @Test
    void testEarlyReturnInsertOnlyPhaseFiltersStrayKeyOnlyDeletesFlag() {
        // Phase-1 answer is [INSERT] with keyOnlyDeletes=true set (an impossible combination that
        // can be constructed programmatically but never expressed by production). Early return must
        // route through assembler to filter out the stray flag.
        RecordingChangelogFunction fn =
                RecordingChangelogFunction.fixed(
                        ChangelogMode.newBuilder()
                                .addContainedKind(RowKind.INSERT)
                                .keyOnlyDeletes(true)
                                .build());

        ChangelogMode result = resolve(fn);

        assertThat(result.contains(RowKind.INSERT)).isTrue();
        assertThat(result.keyOnlyDeletes()).isFalse();
        assertThat(fn.callCount).isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // ChangelogContext exposed to the function
    // -------------------------------------------------------------------------

    @Test
    void testContextTableChangelogModesReflectConfiguredInputModes() {
        // A multi-argument PTF: table "t1" (configured to all()), a scalar in between (must report
        // null per ChangelogContext#getTableChangelogMode's own Javadoc), and table "t2" (left at
        // its insertOnly() default). Position 0 also confirms that the SQL-operand-ordered list the
        // resolver receives maps positionally onto configured modes.
        List<ProcessTableFunctionTestHarness.ArgumentInfo> arguments =
                Arrays.asList(
                        tableArgWithChangelogMode(
                                "t1",
                                DataTypes.ROW(DataTypes.FIELD("v", DataTypes.INT())),
                                ChangelogMode.all(),
                                StaticArgumentTrait.ROW_SEMANTIC_TABLE,
                                StaticArgumentTrait.SUPPORT_UPDATES),
                        new ProcessTableFunctionTestHarness.ScalarArgumentInfo(
                                "s", DataTypes.INT(), 1),
                        tableArg(
                                "t2",
                                DataTypes.ROW(DataTypes.FIELD("n", DataTypes.STRING())),
                                null,
                                StaticArgumentTrait.ROW_SEMANTIC_TABLE));

        Map<Integer, ChangelogMode> observed = new HashMap<>();
        RecordingChangelogFunction fn =
                new RecordingChangelogFunction(
                        ctx -> {
                            for (int pos = 0; pos < arguments.size(); pos++) {
                                observed.put(pos, ctx.getTableChangelogMode(pos));
                            }
                            return ChangelogMode.insertOnly();
                        });

        resolve(fn, arguments);

        assertThat(observed.get(0)).isEqualTo(ChangelogMode.all());
        assertThat(observed.get(1)).isNull();
        assertThat(observed.get(2)).isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void testContextTableSemanticsMatchesConfiguredPartitioning() {
        // The recorded TableSemantics should reflect partitioning on "k" (field index 0), matching
        // what TestContext#tableSemanticsFor() would compute for the same argument.
        List<ProcessTableFunctionTestHarness.ArgumentInfo> arguments =
                Collections.singletonList(
                        tableArg(
                                "p",
                                DataTypes.ROW(
                                        DataTypes.FIELD("k", DataTypes.STRING()),
                                        DataTypes.FIELD("v", DataTypes.INT())),
                                new String[] {"k"},
                                StaticArgumentTrait.SET_SEMANTIC_TABLE));

        Map<Integer, TableSemantics> observed = new HashMap<>();
        RecordingChangelogFunction fn =
                new RecordingChangelogFunction(
                        ctx -> {
                            ctx.getTableSemantics(0).ifPresent(sem -> observed.put(0, sem));
                            return ChangelogMode.insertOnly();
                        });

        resolve(fn, arguments);

        assertThat(observed.get(0)).isNotNull();
        assertThat(observed.get(0).partitionByColumns()).containsExactly(0);
    }

    @Test
    void testContextTableSemanticsExposesConfiguredUpsertKeys() {
        // Upsert-key column names configured on the resolver must reach the function as resolved
        // field indices, mirroring what production TableSemantics provides.
        List<ProcessTableFunctionTestHarness.ArgumentInfo> arguments =
                Collections.singletonList(
                        tableArgWithUpsertKey(
                                "input",
                                DataTypes.ROW(
                                        DataTypes.FIELD("k", DataTypes.INT()),
                                        DataTypes.FIELD("v", DataTypes.STRING())),
                                new String[] {"k"},
                                new String[] {"k"},
                                StaticArgumentTrait.SET_SEMANTIC_TABLE));

        Map<Integer, List<int[]>> observed = new HashMap<>();
        RecordingChangelogFunction fn =
                new RecordingChangelogFunction(
                        ctx -> {
                            ctx.getTableSemantics(0)
                                    .ifPresent(sem -> observed.put(0, sem.upsertKeyColumns()));
                            return ChangelogMode.upsert(true);
                        });

        resolve(fn, arguments);

        assertThat(observed.get(0)).hasSize(1);
        assertThat(observed.get(0).get(0)).containsExactly(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("argumentValueCases")
    void testContextArgumentValue(
            String name,
            @Nullable Object configuredValue,
            Class<?> requestedClass,
            @Nullable Object expected) {
        List<ProcessTableFunctionTestHarness.ArgumentInfo> arguments =
                Collections.singletonList(
                        new ProcessTableFunctionTestHarness.ScalarArgumentInfo(
                                "arg", DataTypes.INT(), configuredValue));

        Object[] captured = new Object[1];
        RecordingChangelogFunction fn =
                new RecordingChangelogFunction(
                        ctx -> {
                            captured[0] = ctx.getArgumentValue(0, requestedClass).orElse(null);
                            return ChangelogMode.insertOnly();
                        });

        resolve(fn, arguments);

        assertThat(captured[0]).isEqualTo(expected);
    }

    private static Stream<Arguments> argumentValueCases() {
        return Stream.of(
                Arguments.of("configured value is returned", 42, Integer.class, 42),
                Arguments.of("null value yields empty", null, Integer.class, null),
                Arguments.of("incompatible class yields empty", 42, String.class, null),
                Arguments.of("primitive class matches boxed value", 5, int.class, 5));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static ChangelogMode resolve(ChangelogFunction fn) {
        return resolve(fn, Collections.emptyList());
    }

    private static ChangelogMode resolve(
            ChangelogFunction fn, List<ProcessTableFunctionTestHarness.ArgumentInfo> arguments) {
        return new PtfChangelogModeResolver(fn, arguments).resolve();
    }

    private static ProcessTableFunctionTestHarness.TableArgumentInfo tableArg(
            String name,
            DataType dataType,
            @Nullable String[] partitionColumnNames,
            StaticArgumentTrait... traits) {
        return new ProcessTableFunctionTestHarness.TableArgumentInfo(
                name,
                dataType,
                EnumSet.copyOf(Arrays.asList(traits)),
                partitionColumnNames,
                null,
                null);
    }

    private static ProcessTableFunctionTestHarness.TableArgumentInfo tableArgWithChangelogMode(
            String name,
            DataType dataType,
            ChangelogMode changelogMode,
            StaticArgumentTrait... traits) {
        return new ProcessTableFunctionTestHarness.TableArgumentInfo(
                name, dataType, EnumSet.copyOf(Arrays.asList(traits)), null, changelogMode, null);
    }

    private static ProcessTableFunctionTestHarness.TableArgumentInfo tableArgWithUpsertKey(
            String name,
            DataType dataType,
            @Nullable String[] partitionColumnNames,
            String[] upsertKey,
            StaticArgumentTrait... traits) {
        return new ProcessTableFunctionTestHarness.TableArgumentInfo(
                name,
                dataType,
                EnumSet.copyOf(Arrays.asList(traits)),
                partitionColumnNames,
                null,
                upsertKey);
    }
}
