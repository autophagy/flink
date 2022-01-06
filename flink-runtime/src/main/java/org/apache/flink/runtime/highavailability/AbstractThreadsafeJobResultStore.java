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
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** An abstract class for threadsafe implementations of the {@link JobResultStore}. */
public abstract class AbstractThreadsafeJobResultStore implements JobResultStore {

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    @Override
    public void createDirtyResult(JobResultEntry jobResultEntry) throws IOException {
        Preconditions.checkState(
                !hasJobResultEntry(jobResultEntry.getJobId()),
                "Job result store already contains an entry for for job %s",
                jobResultEntry.getJobId());

        readWriteLock.writeLock().lock();
        try {
            createDirtyResultInternal(jobResultEntry);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public abstract void createDirtyResultInternal(JobResultEntry jobResultEntry)
            throws IOException;

    @Override
    public void markResultAsClean(JobID jobId) throws IOException, NoSuchElementException {
        readWriteLock.writeLock().lock();
        try {
            markResultAsCleanInternal(jobId);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public abstract void markResultAsCleanInternal(JobID jobId)
            throws IOException, NoSuchElementException;

    @Override
    public boolean hasJobResultEntry(JobID jobId) throws IOException {
        readWriteLock.readLock().lock();
        try {
            return hasDirtyJobResultEntry(jobId) || hasCleanJobResultEntry(jobId);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public boolean hasDirtyJobResultEntry(JobID jobId) throws IOException {
        readWriteLock.readLock().lock();
        try {
            return hasDirtyJobResultEntryInternal(jobId);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public abstract boolean hasDirtyJobResultEntryInternal(JobID jobId) throws IOException;

    @Override
    public boolean hasCleanJobResultEntry(JobID jobId) throws IOException {
        readWriteLock.readLock().lock();
        try {
            return hasCleanJobResultEntryInternal(jobId);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public abstract boolean hasCleanJobResultEntryInternal(JobID jobId) throws IOException;

    @Override
    public Set<JobResult> getDirtyResults() throws IOException {
        readWriteLock.readLock().lock();
        try {
            return getDirtyResultsInternal();
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public abstract Set<JobResult> getDirtyResultsInternal() throws IOException;
}
