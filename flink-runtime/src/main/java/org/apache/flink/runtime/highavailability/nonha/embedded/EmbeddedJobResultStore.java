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
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmaster.JobResult;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * An implementation of the {@link JobResultStore} which only persists the store to an in memory
 * map.
 */
public class EmbeddedJobResultStore implements JobResultStore {

    private final HashMap<JobID, JobResultEntry> inMemoryMap = new HashMap<>();

    @Override
    public void createDirtyResult(JobResult jobResult) throws IOException {
        final JobResultEntry jobResultEntry = JobResultEntry.createDirtyJobResultEntry(jobResult);
        inMemoryMap.put(jobResult.getJobId(), jobResultEntry);
    }

    @Override
    public void markResultAsClean(JobID jobId) throws IOException {
        if (internalHasJobResultEntry(jobId)) {
            inMemoryMap.get(jobId).markAsClean();
        }
    }

    @Override
    public boolean hasJobResultEntry(JobID jobId) throws IOException {
        return internalHasJobResultEntry(jobId);
    }

    @Override
    public JobResultEntry getJobResultEntry(JobID jobId) throws IOException {
        return inMemoryMap.get(jobId);
    }

    @Override
    public Collection<JobResult> getDirtyResults() throws IOException {
        return inMemoryMap.values().stream()
                .filter(a -> a.getState().equals(JobResultEntry.JobResultState.DIRTY))
                .map(JobResultEntry::getJobResult)
                .collect(Collectors.toList());
    }

    private boolean internalHasJobResultEntry(JobID jobId) {
        return inMemoryMap.containsKey(jobId);
    }
}
