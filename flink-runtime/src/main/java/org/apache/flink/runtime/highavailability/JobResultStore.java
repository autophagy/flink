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
import org.apache.flink.runtime.jobmaster.JobResult;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * A persistent storage mechanism for the results of successfully and unsuccessfully completed jobs.
 * This storage should outlive the concrete jobs themselves, in order for solve possible recovery
 * scenarios in multi-master setups.
 */
public interface JobResultStore {

    /**
     * Create a job result of a completed job. The initial state of a job result is always marked as
     * DIRTY, which indicates that clean-up operations still need to be performed. Once the job
     * resources have been finalized, we can "commit" the job result as a CLEAN result using {@link
     * #markResultAsClean(JobID)}.
     *
     * @param jobResult The job result we wish to persist.
     * @return A future that completes when the job result is persisted.
     * @throws IOException
     */
    CompletableFuture<Void> createDirtyResult(JobResult jobResult) throws IOException;

    /**
     * u Marks an existing job result as CLEAN. This indicates that no more resource cleanup steps
     * need to be performed.
     *
     * @param jobId Ident of the job we wish to mark as clean.
     * @return A future that completes when the job result is marked as clean.
     * @throws IOException
     */
    CompletableFuture<Void> markResultAsClean(JobID jobId) throws IOException;

    /**
     * Returns whether the store already contains an entry for a job.
     *
     * @param jobId Ident of the job we wish to check the store for.
     * @return A future that contains a boolean as to whether the job result store contains an entry
     *     for the given {@link JobID}
     * @throws IOException
     */
    CompletableFuture<Boolean> hasJobResultEntry(JobID jobId) throws IOException;

    /**
     * Get a {@link JobResult} for a given {@link JobID}.
     *
     * @param jobId Ident of the job we wish to retrieve the JobResult for.
     * @return Future with a JobResult obtained from the store, which completes exceptionally in the
     *     event of no result being found.
     * @throws IOException
     */
    CompletableFuture<JobResultEntry> getJobResultEntry(JobID jobId) throws IOException;
}
