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

import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.Preconditions;

/**
 * An entry in a {@link JobResultStore} that couples a completed {@link JobResult} to a state that
 * represents whether the resources of that JobResult have been finalized ({@link
 * JobResultState#CLEAN}) or have yet to be finalized ({@link JobResultState#DIRTY}).
 */
public class JobResultEntry {

    private final JobResult jobResult;
    private JobResultState state;

    public enum JobResultState {
        /** Job has finished, successfully or unsuccessfully, but not cleaned up. */
        DIRTY,

        /** Job has been finished, successfully or unsuccessfully, and has been cleaned up. */
        CLEAN
    }

    public static JobResultEntry createDirtyJobResultEntry(JobResult jobResult) {
        return new JobResultEntry(jobResult, JobResultState.DIRTY);
    }

    private JobResultEntry(JobResult result, JobResultState state) {
        this.jobResult = Preconditions.checkNotNull(result);
        this.state = state;
    }

    public JobResult getJobResult() {
        return jobResult;
    }

    public JobResultState getState() {
        return state;
    }

    public void markAsClean() {
        this.state = JobResultState.CLEAN;
    }
}
