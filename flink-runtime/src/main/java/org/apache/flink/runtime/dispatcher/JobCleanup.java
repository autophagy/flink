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
}
