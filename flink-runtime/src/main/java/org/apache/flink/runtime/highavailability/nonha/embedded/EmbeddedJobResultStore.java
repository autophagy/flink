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
import java.util.NoSuchElementException;
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
    public void markResultAsClean(JobID jobId) throws IOException, NoSuchElementException {
        JobResultEntry entry = inMemoryMap.get(jobId);
        if (entry != null) {
            inMemoryMap.get(jobId).markAsClean();
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public boolean hasJobResultEntry(JobID jobId) throws IOException {
        return inMemoryMap.containsKey(jobId);
    }

    /**
     * Get a {@link JobResult} for a given {@link JobID}.
     *
     * @param jobId Ident of the job we wish to retrieve the JobResult for.
     * @return A JobResult obtained from the store.
     * @throws IOException
     */
    @Override
    public JobResultEntry getJobResultEntry(JobID jobId)
            throws IOException, NoSuchElementException {
        JobResultEntry entry = inMemoryMap.get(jobId);
        if (entry != null) {
            return inMemoryMap.get(jobId);
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public Collection<JobResult> getDirtyResults() throws IOException {
        return inMemoryMap.values().stream()
                .filter(a -> a.getState().equals(JobResultEntry.JobResultState.DIRTY))
                .map(JobResultEntry::getJobResult)
                .collect(Collectors.toList());
    }
}
