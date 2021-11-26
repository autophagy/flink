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
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.util.function.FunctionWithException;

import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** {@code JobCleanupTest} tests default implementations of {@link JobCleanup}. */
public class JobCleanupTest {

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> TEST_EXECUTOR_RESOURCE =
            new TestExecutorResource<>(Executors::newSingleThreadScheduledExecutor);

    @Test
    public void testSuccessfulCleanupWithNoRetries()
            throws ExecutionException, InterruptedException {
        final TestJobCleanup testInstance =
                new TestJobCleanup(jobId -> CompletableFuture.completedFuture(true));

        final CompletableFuture<Boolean> cleanupResult =
                testInstance.cleanupJobDataWithRetry(
                        new JobID(),
                        new FixedRetryStrategy(10, Duration.ZERO),
                        new ScheduledExecutorServiceAdapter(TEST_EXECUTOR_RESOURCE.getExecutor()));

        assertTrue(cleanupResult.get());
        assertEquals(1, testInstance.getCleanupCallCount());
    }

    @Test
    public void testRetryInCaseSingleUnsuccessfulCompletion()
            throws ExecutionException, InterruptedException {
        final AtomicBoolean expectedResult = new AtomicBoolean(false);
        final TestJobCleanup testInstance =
                new TestJobCleanup(
                        jobId -> {
                            if (expectedResult.get()) {
                                return CompletableFuture.completedFuture(true);
                            }

                            expectedResult.set(true);
                            return CompletableFuture.completedFuture(false);
                        });

        final CompletableFuture<Boolean> cleanupResult =
                testInstance.cleanupJobDataWithRetry(
                        new JobID(),
                        new FixedRetryStrategy(10, Duration.ZERO),
                        new ScheduledExecutorServiceAdapter(TEST_EXECUTOR_RESOURCE.getExecutor()));

        assertTrue(cleanupResult.get());
        assertEquals(2, testInstance.getCleanupCallCount());
    }

    @Test
    public void testRetryInCaseOfInfiniteNumberOfUnsuccessfulCompletions()
            throws ExecutionException, InterruptedException {
        final int maxRetryCount = 5;
        final TestJobCleanup testInstance =
                new TestJobCleanup(jobId -> CompletableFuture.completedFuture(false));

        final CompletableFuture<Boolean> cleanupResult =
                testInstance.cleanupJobDataWithRetry(
                        new JobID(),
                        new FixedRetryStrategy(maxRetryCount, Duration.ZERO),
                        new ScheduledExecutorServiceAdapter(TEST_EXECUTOR_RESOURCE.getExecutor()));

        try {
            cleanupResult.get();
            fail("A RetryException is expected.");
        } catch (Throwable actualException) {
            assertTrue(
                    ExceptionUtils.findThrowable(actualException, FutureUtils.RetryException.class)
                            .isPresent());
        }
        assertEquals(maxRetryCount + 1, testInstance.getCleanupCallCount());
    }

    @Test
    public void testRetryInCaseOfException() throws ExecutionException, InterruptedException {
        final Exception expectedException = new RuntimeException("Expected RuntimeException");
        final int maxRetryCount = 2;
        final TestJobCleanup testInstance =
                new TestJobCleanup(
                        jobId -> {
                            throw expectedException;
                        });

        final CompletableFuture<Boolean> cleanupResult =
                testInstance.cleanupJobDataWithRetry(
                        new JobID(),
                        new FixedRetryStrategy(maxRetryCount, Duration.ZERO),
                        new ScheduledExecutorServiceAdapter(TEST_EXECUTOR_RESOURCE.getExecutor()));

        try {
            cleanupResult.get();
            fail("A RuntimeException is expected.");
        } catch (Throwable actualException) {
            assertEquals(
                    expectedException,
                    ExceptionUtils.findThrowable(actualException, RuntimeException.class).get());
        }
        assertEquals(maxRetryCount + 1, testInstance.getCleanupCallCount());
    }

    private static class TestJobCleanup implements JobCleanup {

        private int cleanupCallCount = 0;
        private FunctionWithException<JobID, CompletableFuture<Boolean>, Exception> cleanupLogic;

        public TestJobCleanup(
                FunctionWithException<JobID, CompletableFuture<Boolean>, Exception> cleanupLogic) {
            this.cleanupLogic = cleanupLogic;
        }

        @Override
        public CompletableFuture<Boolean> cleanupJobData(JobID jobId) throws Exception {
            cleanupCallCount++;
            return cleanupLogic.apply(jobId);
        }

        public int getCleanupCallCount() {
            return cleanupCallCount;
        }
    }
}
