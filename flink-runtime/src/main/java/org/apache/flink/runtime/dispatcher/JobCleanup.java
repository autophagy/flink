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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.util.concurrent.CompletableFuture;

/**
 * {@code JobCleanup} is supposed to be used by any class that provides artifacts for a given job.
 */
public interface JobCleanup {

    /**
     * Cleans up all the artifacts related to the given {@link JobID} and referenced by the
     * implementing class.
     *
     * @param jobId The {@code JobID} representing the job that is subject to cleanup.
     * @returns A {@code CompletableFuture} with a {@code Boolean} result which is {@code true} if
     *     the cleanup was successful or {@code false} otherwise.
     * @throws Exception if cleaning up failed.
     */
    CompletableFuture<Boolean> cleanupJobData(JobID jobId) throws Exception;

    /**
     * Calls {@link #cleanupJobData(JobID)} in a retryable fashion.
     *
     * @param jobId The ID of the job for which the data shall be cleaned up.
     * @param retryStrategy The {@link RetryStrategy} that shall be applied.
     * @param scheduledExecutor The {@link ScheduledExecutor} that is used to execute the retries.
     * @return A {@code CompletableFuture} referring to the final result of the cleanup operation.
     */
    default CompletableFuture<Boolean> cleanupJobDataWithRetry(
            JobID jobId, RetryStrategy retryStrategy, ScheduledExecutor scheduledExecutor) {
        return FutureUtils.retryOperation(
                () -> {
                    try {
                        return cleanupJobData(jobId);
                    } catch (Exception e) {
                        return FutureUtils.completedExceptionally(e);
                    }
                },
                retryStrategy,
                // trigger retry in case false is returned by the cleanup
                cleanupSuccessful -> cleanupSuccessful ? true : null,
                // always retry on failure
                throwable -> true,
                scheduledExecutor);
    }
}
