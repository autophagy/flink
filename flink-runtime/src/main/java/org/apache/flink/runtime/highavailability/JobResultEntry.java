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
import org.apache.flink.runtime.rest.messages.json.JobResultDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobResultSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * {@code JobResultEntry} is the entity managed by the {@link JobResultStore}. It collects
 * information about a globally terminated job (e.g. {@link JobResult}).
 */
public class JobResultEntry {

    private final JobResult jobResult;

    public JobResultEntry(JobResult jobResult) {
        this.jobResult = Preconditions.checkNotNull(jobResult);
    }

    public JobResult getJobResult() {
        return jobResult;
    }

    public JobID getJobId() {
        return jobResult.getJobId();
    }

    @JsonIgnoreProperties(
            value = {JsonJobResultEntry.FIELD_NAME_VERSION},
            allowGetters = true)
    /**
     * Wrapper class around {@link JobResultEntry} to allow for serialization of a schema version,
     * so that future schema changes can be handled in a backwards compatible manner.
     */
    public static class JsonJobResultEntry {
        public static final String FIELD_NAME_RESULT = "result";
        public static final String FIELD_NAME_VERSION = "version";

        @JsonProperty(FIELD_NAME_RESULT)
        @JsonSerialize(using = JobResultSerializer.class)
        @JsonDeserialize(using = JobResultDeserializer.class)
        private final JobResult jobResult;

        public JsonJobResultEntry(JobResultEntry entry) {
            this.jobResult = entry.getJobResult();
        }

        @JsonCreator
        public JsonJobResultEntry(@JsonProperty(FIELD_NAME_RESULT) JobResult jobResult) {
            this.jobResult = jobResult;
        }

        public JobResult getJobResult() {
            return jobResult;
        }

        public int getVersion() {
            return 1;
        }
    }
}
