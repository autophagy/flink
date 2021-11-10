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
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link EmbeddedJobResultStore}. */
public class EmbeddedJobResultStoreTest extends TestLogger {

    private EmbeddedJobResultStore embeddedJobResultStore;

    private JobResult jobResult;

    @Before
    public void setupTest() {
        embeddedJobResultStore = new EmbeddedJobResultStore();
        jobResult =
                new JobResult.Builder()
                        .applicationStatus(ApplicationStatus.UNKNOWN)
                        .jobId(new JobID())
                        .netRuntime(Long.MAX_VALUE)
                        .build();
    }

    @After
    public void teardownTest() {
        embeddedJobResultStore = null;
        jobResult = null;
    }

    /** Tests that adding a JobResult to the JobResultStore results in a Dirty JobResultEntry. */
    @Test
    public void testStoreDirtyJobResult() throws Exception {
        embeddedJobResultStore.createDirtyResult(jobResult);
        assertTrue(embeddedJobResultStore.hasJobResultEntry(jobResult.getJobId()));

        JobResultEntry jobResultEntry =
                embeddedJobResultStore.getJobResultEntry(jobResult.getJobId());

        assertEquals(jobResultEntry.getState(), JobResultEntry.JobResultState.DIRTY);
    }

    /**
     * Tests that adding a JobResult to the JobResultsStore and then marking it as clean puts it
     * into a clean state.
     */
    @Test
    public void testCleanDirtyJobResult() throws Exception {
        embeddedJobResultStore.createDirtyResult(jobResult);
        embeddedJobResultStore.markResultAsClean(jobResult.getJobId());

        JobResultEntry jobResultEntry =
                embeddedJobResultStore.getJobResultEntry(jobResult.getJobId());
        assertEquals(jobResultEntry.getState(), JobResultEntry.JobResultState.CLEAN);
    }

    /** Tests that attempting to clean a nonexistent job result produces an exception. */
    @Test(expected = NoSuchElementException.class)
    public void testCleanNonexistentJobResult() throws Exception {
        embeddedJobResultStore.markResultAsClean(jobResult.getJobId());
    }
}
