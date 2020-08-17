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

import com.amazon.opendistroforelasticsearch.ad.annotation.Generated;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

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

    private static final String TASK_ID_FIELD = "task_id";
    private static final String DETECTOR_ID_FIELD = "detector_id";
    private static final String DESCRIPTION_FIELD = "description";
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

    private final String taskId;
    private final String detectorId;
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
        String detectorId,
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
        if (Strings.isBlank(detectorId)) {
            throw new IllegalArgumentException("Detector id should be set");
        }
        this.taskId = taskId;
        this.detectorId = detectorId;
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
            .field(DETECTOR_ID_FIELD, detectorId)
            .field(STATE_FIELD, state)
            .field(SCHEMA_VERSION_FIELD, schemaVersion);
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
        return parse(parser, null);
    }

    public static AnomalyDetectionTaskExecution parse(
        XContentParser parser,
        Long version
    ) throws IOException {
        String taskId = null;
        String detectorId = null;
        String state = null;
        Instant dataStartTime = null;
        Instant dataEndTime = null;
        Instant executionStartTime = null;
        Instant executionEndTime = null;
        Instant currentDetectionInterval = null;
        int schemaVersion = 0;
        Map<String, Object> uiMetadata = null;
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
                case DETECTOR_ID_FIELD:
                    detectorId = parser.text();
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
                taskId,
                detectorId,
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnomalyDetectionTaskExecution that = (AnomalyDetectionTaskExecution) o;
        return Objects.equals(taskId, that.taskId) &&
                Objects.equals(detectorId, that.detectorId) &&
                Objects.equals(version, that.version) &&
                Objects.equals(dataStartTime, that.dataStartTime) &&
                Objects.equals(dataEndTime, that.dataEndTime) &&
                Objects.equals(executionStartTime, that.executionStartTime) &&
                Objects.equals(executionEndTime, that.executionEndTime) &&
                Objects.equals(currentDetectionInterval, that.currentDetectionInterval) &&
                Objects.equals(state, that.state) &&
                Objects.equals(error, that.error) &&
                Objects.equals(schemaVersion, that.schemaVersion) &&
                Objects.equals(lastUpdateTime, that.lastUpdateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, detectorId, version, dataStartTime, dataEndTime, executionStartTime, executionEndTime, currentDetectionInterval, state, error, schemaVersion, lastUpdateTime);
    }

    public String getTaskId() {
        return taskId;
    }

    public String getDetectorId() {
        return detectorId;
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
        return "AnomalyDetectionTaskExecution{" +
                "taskId='" + taskId + '\'' +
                ", detectorId='" + detectorId + '\'' +
                ", version=" + version +
                ", dataStartTime=" + dataStartTime +
                ", dataEndTime=" + dataEndTime +
                ", executionStartTime=" + executionStartTime +
                ", executionEndTime=" + executionEndTime +
                ", currentDetectionInterval=" + currentDetectionInterval +
                ", state='" + state + '\'' +
                ", error='" + error + '\'' +
                ", schemaVersion=" + schemaVersion +
                ", lastUpdateTime=" + lastUpdateTime +
                '}';
    }
}
