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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for the {@link FileSystemJobResultStore}. */
@ExtendWith(TestLoggerExtension.class)
public class FileSystemJobResultStoreTest {

    private static final JobResultEntry DUMMY_JOB_RESULT_ENTRY = createDummyJobResultEntry();

    private FileSystemJobResultStore fileSystemJobResultStore;

    @TempDir File temporaryFolder;

    private static final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    public void setupTest() throws IOException {
        Path path = new Path(temporaryFolder.toURI());
        fileSystemJobResultStore = new FileSystemJobResultStore(path.getFileSystem(), path, false);
    }

    /** Tests that adding a JobResult to the JobResultStore results in a Dirty JobResultEntry. */
    @Test
    public void testStoreDirtyJobResult() throws Exception {
        fileSystemJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        assertThat(fileSystemJobResultStore.hasJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isTrue();

        Set<JobResult> dirtyResults = fileSystemJobResultStore.getDirtyResults();
        assertThat(dirtyResults.stream().map(JobResult::getJobId).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(DUMMY_JOB_RESULT_ENTRY.getJobId());
        assertThat(getCleanResultIds()).isEmpty();

        Path path = expectedDirtyPath(DUMMY_JOB_RESULT_ENTRY);
        assertThat(new File(path.getPath())).exists().isFile().isNotEmpty();
    }

    @Test
    public void testStoreDirtyJobResultTwice() throws IOException {
        fileSystemJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);

        assertThatThrownBy(() -> fileSystemJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testStoreDirtyJobResultAfterClean() throws IOException {
        fileSystemJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        fileSystemJobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());

        assertThatThrownBy(() -> fileSystemJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY))
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * Tests that adding a JobResult to the JobResultsStore and then marking it as clean puts it
     * into a clean state.
     */
    @Test
    public void testCleanDirtyJobResult() throws Exception {
        fileSystemJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        fileSystemJobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());

        assertThat(fileSystemJobResultStore.getDirtyResults()).isEmpty();
        assertThat(getCleanResultIds())
                .containsExactlyInAnyOrder(DUMMY_JOB_RESULT_ENTRY.getJobId());

        Path path = expectedCleanPath(DUMMY_JOB_RESULT_ENTRY);
        assertThat(new File(path.getPath())).exists().isFile().isNotEmpty();
    }

    /**
     * Tests that adding a JobResult to the JobResultsStore and then marking it as clean puts it
     * into a clean state.
     */
    @Test
    public void testCleanDirtyJobResultTwice() throws Exception {
        fileSystemJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        fileSystemJobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());

        assertThat(getCleanResultIds())
                .containsExactlyInAnyOrder(DUMMY_JOB_RESULT_ENTRY.getJobId());
    }

    /** Tests that attempting to clean a nonexistent job result produces an exception. */
    @Test
    public void testCleanNonExistentJobResult() throws Exception {
        assertThatThrownBy(() -> fileSystemJobResultStore.markResultAsClean(new JobID()))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testHasJobResultEntryWithNoEntry() throws IOException {
        assertThat(fileSystemJobResultStore.hasJobResultEntry(new JobID())).isFalse();
    }

    @Test
    public void testHasJobResultEntryWithDirtyEntry() throws IOException {
        fileSystemJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);

        assertThat(fileSystemJobResultStore.hasJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isTrue();
    }

    @Test
    public void testHasJobResultEntryWithCleanEntry() throws IOException {
        fileSystemJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        fileSystemJobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());

        assertThat(fileSystemJobResultStore.hasJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isTrue();
    }

    @Test
    public void testGetDirtyResultsWithNoEntry() throws IOException {
        assertThat(fileSystemJobResultStore.getDirtyResults()).isEmpty();
    }

    @Test
    public void testGetDirtyResultsWithDirtyEntry() throws IOException {
        fileSystemJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);

        assertThat(
                        fileSystemJobResultStore.getDirtyResults().stream()
                                .map(JobResult::getJobId)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(DUMMY_JOB_RESULT_ENTRY.getJobId());
    }

    @Test
    public void testGetDirtyResultsWithDirtyAndCleanEntry() throws IOException {
        fileSystemJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        fileSystemJobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());

        final JobResultEntry dirtyJobResultEntry = createDummyJobResultEntry();
        fileSystemJobResultStore.createDirtyResult(dirtyJobResultEntry);

        assertThat(fileSystemJobResultStore.getDirtyResults()).hasSize(1);
        assertThat(getCleanResultIds()).hasSize(1);

        assertThat(
                        fileSystemJobResultStore.getDirtyResults().stream()
                                .map(JobResult::getJobId)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(dirtyJobResultEntry.getJobId());
    }

    @Test
    public void testDeleteOnCommit() throws IOException {
        Path path = new Path(temporaryFolder.toURI());
        fileSystemJobResultStore = new FileSystemJobResultStore(path.getFileSystem(), path, true);

        fileSystemJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);

        File dirtyFile = new File(expectedDirtyPath(DUMMY_JOB_RESULT_ENTRY).getPath());
        File cleanFile = new File(expectedCleanPath(DUMMY_JOB_RESULT_ENTRY).getPath());

        assertThat(dirtyFile).exists().isFile().isNotEmpty();

        fileSystemJobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());
        assertThat(dirtyFile).doesNotExist();
        assertThat(cleanFile).doesNotExist();
    }

    private static JobResultEntry createDummyJobResultEntry() {
        return new JobResultEntry(
                new JobResult.Builder()
                        .applicationStatus(ApplicationStatus.SUCCEEDED)
                        .jobId(new JobID())
                        .netRuntime(Long.MAX_VALUE)
                        .build());
    }

    private List<JobID> getCleanResultIds() throws IOException {
        final List<JobID> cleanResults = new ArrayList<>();

        final File[] cleanFiles =
                temporaryFolder.listFiles((dir, name) -> !name.endsWith("_DIRTY.json"));
        for (File cleanFile : cleanFiles) {
            final FileSystemJobResultStore.JsonJobResultEntry entry =
                    mapper.readValue(cleanFile, FileSystemJobResultStore.JsonJobResultEntry.class);
            cleanResults.add(entry.getJobResult().getJobId());
        }

        return cleanResults;
    }

    /**
     * Generates the expected path for a dirty entry given a job entry.
     *
     * @param entry The job ID to construct the expected dirty path from.
     * @return The expected dirty path.
     */
    private Path expectedDirtyPath(JobResultEntry entry) {
        return new Path(
                temporaryFolder.toURI().getPath()
                        + "/"
                        + entry.getJobId().toString()
                        + "_DIRTY.json");
    }

    /**
     * Generates the expected path for a clean entry given a job entry.
     *
     * @param entry The job entry to construct the expected clean path from.
     * @return The expected clean path.
     */
    private Path expectedCleanPath(JobResultEntry entry) {
        return new Path(
                temporaryFolder.toURI().getPath() + "/" + entry.getJobId().toString() + ".json");
    }
}
