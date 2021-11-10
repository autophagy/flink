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

package org.apache.flink.runtime.highavailability.nonha.embedded;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link EmbeddedJobResultStore}. */
public class EmbeddedJobResultStoreTest extends TestLogger {

    private EmbeddedJobResultStore embeddedJobResultStore;

    @Before
    public void setupTest() {
        embeddedJobResultStore = new EmbeddedJobResultStore();
    }

    @After
    public void teardownTest() throws Exception {
        embeddedJobResultStore = null;
    }

    /** Tests that adding a JobResult to the JobResultStore results in a Dirty JobResultEntry. */
    @Test
    public void testStoreDirtyJobResult() throws Exception {
        JobID jobId = new JobID();
        JobResult jobResult =
                new JobResult.Builder()
                        .applicationStatus(ApplicationStatus.UNKNOWN)
                        .jobId(jobId)
                        .netRuntime(Long.MAX_VALUE)
                        .accumulatorResults(
                                Collections.singletonMap(
                                        "test", new SerializedValue<>(OptionalFailure.of(1.0))))
                        .build();
        embeddedJobResultStore.createDirtyResult(jobResult);
        assertTrue(embeddedJobResultStore.hasJobResultEntry(jobId).get());

        JobResultEntry jobResultEntry = embeddedJobResultStore.getJobResultEntry(jobId).get();

        assertEquals(jobResultEntry.getState(), JobResultEntry.JobResultState.DIRTY);
    }

    /**
     * Tests that adding a JobResult to the JobResultsStore and then marking it as clean puts it
     * into a clean state.
     */
    @Test
    public void testCleanDirtyJobResult() throws Exception {
        JobID jobId = new JobID();
        JobResult jobResult =
                new JobResult.Builder()
                        .applicationStatus(ApplicationStatus.UNKNOWN)
                        .jobId(jobId)
                        .netRuntime(Long.MAX_VALUE)
                        .accumulatorResults(
                                Collections.singletonMap(
                                        "test", new SerializedValue<>(OptionalFailure.of(1.0))))
                        .build();
        embeddedJobResultStore.createDirtyResult(jobResult);
        embeddedJobResultStore.markResultAsClean(jobId);

        JobResultEntry jobResultEntry = embeddedJobResultStore.getJobResultEntry(jobId).get();
        assertEquals(jobResultEntry.getState(), JobResultEntry.JobResultState.CLEAN);
    }
}
