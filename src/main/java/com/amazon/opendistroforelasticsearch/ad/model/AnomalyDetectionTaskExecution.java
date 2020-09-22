/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.model;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import com.amazon.opendistroforelasticsearch.ad.annotation.Generated;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;

/**
 * An AnomalyDetectionTaskExecution is used to represent anomaly detection task execution related parameters.
 */
public class AnomalyDetectionTaskExecution implements ToXContentObject {

    public static final String PARSE_FIELD_NAME = "AnomalyDetectionTaskExecution";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        AnomalyDetectionTaskExecution.class,
        new ParseField(PARSE_FIELD_NAME),
        it -> parse(it)
    );
    public static final String NO_ID = "";
    public static final String ANOMALY_DETECTION_TASK_EXECUTION_INDEX = ".opendistro-anomaly-detection-task-executions";
    public static final String TYPE = "_doc";

    private static final String TASK_EXECUTION_ID_FIELD = "task_execution_id";
    private static final String TASK_ID_FIELD = "task_id";
    private static final String TASK_FIELD = "task";
    private static final String DATA_START_TIME_FIELD = "data_start_time";
    public static final String DATA_END_TIME_FIELD = "data_end_time";
    private static final String EXECUTION_START_TIME_FIELD = "execution_start_time";
    public static final String EXECUTION_END_TIME_FIELD = "execution_end_time";
    public static final String CURRENT_DETECTION_INTERVAL_FIELD = "current_detection_interval";
    public static final String STATE_FIELD = "state";
    public static final String ERROR_FIELD = "error";
    private static final String SCHEMA_VERSION_FIELD = "schema_version";
    private static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    private static Logger logger = LogManager.getLogger(AnomalyDetectionTaskExecution.class);

    private final String taskExecutionId;
    private final String taskId;
    private final AnomalyDetectionTask task;
    private final Long version;
    private final Instant dataStartTime;
    private final Instant dataEndTime;
    private final Instant executionStartTime;
    private final Instant executionEndTime;
    private final Instant currentDetectionInterval;
    private final String state;
    private final String error;
    private final Integer schemaVersion;
    private final Instant lastUpdateTime;

    public AnomalyDetectionTaskExecution(
        String taskId,
        AnomalyDetectionTask task,
        Long version,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executionStartTime,
        Instant executionEndTime,
        Instant currentDetectionInterval,
        String state,
        String error,
        Integer schemaVersion,
        Instant lastUpdateTime
    ) {
        this(
            null,
            taskId,
            task,
            version,
            dataStartTime,
            dataEndTime,
            executionStartTime,
            executionEndTime,
            currentDetectionInterval,
            state,
            error,
            schemaVersion,
            lastUpdateTime
        );
    }

    public AnomalyDetectionTaskExecution(
        String taskExecutionId,
        String taskId,
        AnomalyDetectionTask task,
        Long version,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executionStartTime,
        Instant executionEndTime,
        Instant currentDetectionInterval,
        String state,
        String error,
        Integer schemaVersion,
        Instant lastUpdateTime
    ) {
        if (Strings.isBlank(taskId)) {
            throw new IllegalArgumentException("Detection task id should be set");
        }
        this.taskExecutionId = taskExecutionId;
        this.taskId = taskId;
        this.task = task;
        this.version = version;
        this.dataStartTime = dataStartTime;
        this.dataEndTime = dataEndTime;
        this.executionStartTime = executionStartTime;
        this.executionEndTime = executionEndTime;
        this.currentDetectionInterval = currentDetectionInterval;
        this.state = state;
        this.error = error;
        this.schemaVersion = schemaVersion;
        this.lastUpdateTime = lastUpdateTime;
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(TASK_ID_FIELD, taskId)
            .field(STATE_FIELD, state)
            .field(SCHEMA_VERSION_FIELD, schemaVersion)
            .field(TASK_FIELD, task);
        if (taskExecutionId != null) {
            xContentBuilder.field(TASK_EXECUTION_ID_FIELD, taskExecutionId);
        }
        if (dataStartTime != null) {
            xContentBuilder.field(DATA_START_TIME_FIELD, dataStartTime.toEpochMilli());
        }
        if (dataEndTime != null) {
            xContentBuilder.field(DATA_END_TIME_FIELD, dataEndTime.toEpochMilli());
        }
        if (executionStartTime != null) {
            xContentBuilder.field(EXECUTION_START_TIME_FIELD, executionStartTime.toEpochMilli());
        }
        if (executionEndTime != null) {
            xContentBuilder.field(EXECUTION_END_TIME_FIELD, executionEndTime.toEpochMilli());
        }
        if (currentDetectionInterval != null) {
            xContentBuilder.field(CURRENT_DETECTION_INTERVAL_FIELD, currentDetectionInterval.toEpochMilli());
        }
        if (error != null) {
            xContentBuilder.field(ERROR_FIELD, error);
        }
        if (lastUpdateTime != null) {
            xContentBuilder.timeField(LAST_UPDATE_TIME_FIELD, LAST_UPDATE_TIME_FIELD, lastUpdateTime.toEpochMilli());
        }
        return xContentBuilder.endObject();
    }

    public static AnomalyDetectionTaskExecution parse(XContentParser parser) throws IOException {
        return parse(parser, null, null);
    }

    public static AnomalyDetectionTaskExecution parse(XContentParser parser, String taskExecutionId) throws IOException {
        return parse(parser, null, taskExecutionId);
    }

    public static AnomalyDetectionTaskExecution parse(XContentParser parser, Long version, String taskExecutionId) throws IOException {
        String taskId = null;
        AnomalyDetectionTask task = null;
        String state = null;
        Instant dataStartTime = null;
        Instant dataEndTime = null;
        Instant executionStartTime = null;
        Instant executionEndTime = null;
        Instant currentDetectionInterval = null;
        int schemaVersion = 0;
        String error = null;
        Instant lastUpdateTime = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case TASK_ID_FIELD:
                    taskId = parser.text();
                    break;
                case TASK_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
                    task = AnomalyDetectionTask.parse(parser);
                    break;
                case STATE_FIELD:
                    state = parser.text();
                    break;
                case SCHEMA_VERSION_FIELD:
                    schemaVersion = parser.intValue();
                    break;
                case DATA_START_TIME_FIELD:
                    dataStartTime = ParseUtils.toInstant(parser);
                    break;
                case DATA_END_TIME_FIELD:
                    dataEndTime = ParseUtils.toInstant(parser);
                    break;
                case EXECUTION_START_TIME_FIELD:
                    executionStartTime = ParseUtils.toInstant(parser);
                    break;
                case EXECUTION_END_TIME_FIELD:
                    executionEndTime = ParseUtils.toInstant(parser);
                    break;
                case CURRENT_DETECTION_INTERVAL_FIELD:
                    currentDetectionInterval = ParseUtils.toInstant(parser);
                    break;
                case ERROR_FIELD:
                    error = parser.text();
                    break;
                case LAST_UPDATE_TIME_FIELD:
                    lastUpdateTime = ParseUtils.toInstant(parser);
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        return new AnomalyDetectionTaskExecution(
            taskExecutionId,
            taskId,
            task,
            version,
            dataStartTime,
            dataEndTime,
            executionStartTime,
            executionEndTime,
            currentDetectionInterval,
            state,
            error,
            schemaVersion,
            lastUpdateTime
        );
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AnomalyDetectionTaskExecution that = (AnomalyDetectionTaskExecution) o;
        return Objects.equals(taskId, that.taskId)
            && task.equals(that.task)
            && Objects.equals(version, that.version)
            && Objects.equals(dataStartTime, that.dataStartTime)
            && Objects.equals(dataEndTime, that.dataEndTime)
            && Objects.equals(executionStartTime, that.executionStartTime)
            && Objects.equals(executionEndTime, that.executionEndTime)
            && Objects.equals(currentDetectionInterval, that.currentDetectionInterval)
            && Objects.equals(state, that.state)
            && Objects.equals(error, that.error)
            && Objects.equals(schemaVersion, that.schemaVersion)
            && Objects.equals(lastUpdateTime, that.lastUpdateTime);
    }

    @Override
    public int hashCode() {
        return Objects
            .hash(
                taskId,
                version,
                dataStartTime,
                dataEndTime,
                executionStartTime,
                executionEndTime,
                currentDetectionInterval,
                state,
                error,
                schemaVersion,
                lastUpdateTime
            );
    }

    public String getTaskId() {
        return taskId;
    }

    public AnomalyDetectionTask getTask() {
        return task;
    }

    public Long getVersion() {
        return version;
    }

    public Instant getDataStartTime() {
        return dataStartTime;
    }

    public Instant getDataEndTime() {
        return dataEndTime;
    }

    public Instant getExecutionStartTime() {
        return executionStartTime;
    }

    public Instant getExecutionEndTime() {
        return executionEndTime;
    }

    public Instant getCurrentDetectionInterval() {
        return currentDetectionInterval;
    }

    public String getState() {
        return state;
    }

    public String getError() {
        return error;
    }

    public Integer getSchemaVersion() {
        return schemaVersion;
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public String toString() {
        return "AnomalyDetectionTaskExecution{"
            + "taskId='"
            + taskId
            + '\''
            + ", task='"
            + task.toString()
            + '\''
            + ", version="
            + version
            + ", dataStartTime="
            + dataStartTime
            + ", dataEndTime="
            + dataEndTime
            + ", executionStartTime="
            + executionStartTime
            + ", executionEndTime="
            + executionEndTime
            + ", currentDetectionInterval="
            + currentDetectionInterval
            + ", state='"
            + state
            + '\''
            + ", error='"
            + error
            + '\''
            + ", schemaVersion="
            + schemaVersion
            + ", lastUpdateTime="
            + lastUpdateTime
            + '}';
    }
}
