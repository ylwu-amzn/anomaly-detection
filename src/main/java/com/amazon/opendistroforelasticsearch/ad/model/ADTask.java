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
import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.google.common.base.Objects;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * One anomaly detection task means one detector starts to run until stopped.
 */
public class ADTask implements ToXContentObject, Writeable {

//    public static final String PARSE_FIELD_NAME = "DetectorInternalState";
//    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
//        AnomalyDetectionState.class,
//        new ParseField(PARSE_FIELD_NAME),
//        it -> parse(it)
//    );

    public static final String DETECTOR_STATE_INDEX = ".opendistro-anomaly-detection-state";

    public static final String TASK_ID_FIELD = "task_id";
    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String ERROR_FIELD = "error";
    public static final String STATE_FIELD = "state";
    public static final String DETECTOR_ID_FIELD = "detector_id";
    public static final String PROGRESS_FIELD = "progress";
    public static final String CURRENT_PIECE_FIELD = "current_piece";
    public static final String EXECUTION_START_TIME_FIELD = "execution_start_time";
    public static final String EXECUTION_END_TIME_FIELD = "execution_end_time";
    public static final String IS_LATEST_FIELD = "is_latest";
    public static final String TASK_TYPE_FIELD = "task_type";
    public static final String CHECKPOINT_ID_FIELD = "checkpoint_id";
    public static final String DETECTOR_FIELD = "detector";

    private String taskId = null;
    private Instant lastUpdateTime = null;
    private String error = null;
    private String state = null;
    private String detectorId = null;
    private Float progress = null;
    private Instant currentPiece = null;
    private Instant executionStartTime = null;
    private Instant executionEndTime = null;
    private Boolean isLatest = null;
    private String taskType = null;
    private String checkpointId = null;
    private AnomalyDetector detector = null;

    private ADTask() {}

    public ADTask(StreamInput input) throws IOException {
        this.taskId = input.readOptionalString();
        this.taskType = input.readOptionalString();
        this.detectorId = input.readOptionalString();
        if (input.readBoolean()) {
            this.detector = new AnomalyDetector(input);
        } else {
            this.detector = null;
        }
        this.state = input.readOptionalString();
        this.progress = input.readOptionalFloat();
        this.currentPiece = input.readOptionalInstant();
        this.executionStartTime = input.readOptionalInstant();
        this.executionEndTime = input.readOptionalInstant();
        this.isLatest = input.readOptionalBoolean();
        this.error = input.readOptionalString();
        this.checkpointId = input.readOptionalString();
        this.lastUpdateTime = input.readOptionalInstant();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(taskId);
        out.writeOptionalString(taskType);
        out.writeOptionalString(detectorId);
        if (detector !=  null) {
            out.writeBoolean(true);
            detector.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(state);
        out.writeOptionalFloat(progress);
        out.writeOptionalInstant(currentPiece);
        out.writeOptionalInstant(executionStartTime);
        out.writeOptionalInstant(executionEndTime);
        out.writeOptionalBoolean(isLatest);
        out.writeOptionalString(error);
        out.writeOptionalString(checkpointId);
        out.writeOptionalInstant(lastUpdateTime);
    }

    public static class Builder {
        private String taskId = null;
        private String taskType = null;
        private String detectorId = null;
        private AnomalyDetector detector = null;
        private String state = null;
        private Float progress = null;
        private Instant currentPiece = null;
        private Instant executionStartTime = null;
        private Instant executionEndTime = null;
        private Boolean isLatest = null;
        private String error = null;
        private String checkpointId = null;
        private Instant lastUpdateTime = null;

        public Builder() {}

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder lastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public Builder error(String error) {
            this.error = error;
            return this;
        }

        public Builder state(String state) {
            this.state = state;
            return this;
        }

        public Builder detectorId(String detectorId) {
            this.detectorId = detectorId;
            return this;
        }

        public Builder progress(Float progress) {
            this.progress = progress;
            return this;
        }

        public Builder currentPiece(Instant currentPiece) {
            this.currentPiece = currentPiece;
            return this;
        }

        public Builder executionStartTime(Instant executionStartTime) {
            this.executionStartTime = executionStartTime;
            return this;
        }

        public Builder executionEndTime(Instant executionEndTime) {
            this.executionEndTime = executionEndTime;
            return this;
        }

        public Builder isLatest(Boolean isLatest) {
            this.isLatest = isLatest;
            return this;
        }

        public Builder taskType(String taskType) {
            this.taskType = taskType;
            return this;
        }

        public Builder checkpointId(String checkpointId) {
            this.checkpointId = checkpointId;
            return this;
        }

        public Builder detector(AnomalyDetector detector) {
            this.detector = detector;
            return this;
        }

        public ADTask build() {
            ADTask task = new ADTask();
            task.taskId = this.taskId;
            task.lastUpdateTime = this.lastUpdateTime;
            task.error = this.error;
            task.state = this.state;
            task.detectorId = this.detectorId;
            task.progress = this.progress;
            task.currentPiece = this.currentPiece;
            task.executionStartTime = this.executionStartTime;
            task.executionEndTime = this.executionEndTime;
            task.isLatest = this.isLatest;
            task.taskType = this.taskType;
            task.checkpointId = this.checkpointId;
            task.detector = this.detector;

            return task;
        }

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        if (taskId != null) {
            xContentBuilder.field(TASK_ID_FIELD, taskId);
        }
        if (lastUpdateTime != null) {
            xContentBuilder.field(LAST_UPDATE_TIME_FIELD, lastUpdateTime.toEpochMilli());
        }
        if (error != null) {
            xContentBuilder.field(ERROR_FIELD, error);
        }
        if (state != null) {
            xContentBuilder.field(STATE_FIELD, state);
        }
        if (detectorId != null) {
            xContentBuilder.field(DETECTOR_ID_FIELD, detectorId);
        }
        if (progress != null) {
            xContentBuilder.field(PROGRESS_FIELD, progress);
        }
        if (currentPiece != null) {
            xContentBuilder.field(CURRENT_PIECE_FIELD, currentPiece);
        }
        if (executionStartTime != null) {
            xContentBuilder.field(EXECUTION_START_TIME_FIELD, executionStartTime);
        }
        if (executionEndTime != null) {
            xContentBuilder.field(EXECUTION_END_TIME_FIELD, executionEndTime);
        }
        if (isLatest != null) {
            xContentBuilder.field(IS_LATEST_FIELD, isLatest);
        }
        if (taskType != null) {
            xContentBuilder.field(TASK_TYPE_FIELD, taskType);
        }
        if (checkpointId != null) {
            xContentBuilder.field(CHECKPOINT_ID_FIELD, checkpointId);
        }
        if (detector != null) {
            xContentBuilder.field(DETECTOR_FIELD, detector);
        }
        return xContentBuilder.endObject();
    }

    public static ADTask parse(XContentParser parser) throws IOException {
        return parse(parser, null);
    }

    public static ADTask parse(XContentParser parser, String taskId) throws IOException {
        Instant lastUpdateTime = null;
        String error = null;
        String state = null;
        String detectorId = null;
        Float progress = null;
        Instant currentPiece = null;
        Instant executionStartTime = null;
        Instant executionEndTime = null;
        Boolean isLatest = null;
        String taskType = null;
        String checkpointId = null;
        AnomalyDetector detector = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case LAST_UPDATE_TIME_FIELD:
                    lastUpdateTime = ParseUtils.toInstant(parser);
                    break;
                case ERROR_FIELD:
                    error = parser.text();
                    break;
                case STATE_FIELD:
                    state = parser.text();
                    break;
                case DETECTOR_ID_FIELD:
                    detectorId = parser.text();
                    break;
                case PROGRESS_FIELD:
                    progress = parser.floatValue();
                    break;
                case CURRENT_PIECE_FIELD:
                    currentPiece = ParseUtils.toInstant(parser);
                    break;
                case EXECUTION_START_TIME_FIELD:
                    executionStartTime = ParseUtils.toInstant(parser);
                    break;
                case EXECUTION_END_TIME_FIELD:
                    executionEndTime = ParseUtils.toInstant(parser);
                    break;
                case IS_LATEST_FIELD:
                    isLatest = parser.booleanValue();
                    break;
                case TASK_TYPE_FIELD:
                    taskType = parser.text();
                    break;
                case CHECKPOINT_ID_FIELD:
                    checkpointId = parser.text();
                    break;
                case DETECTOR_FIELD:
                    detector = AnomalyDetector.parse(parser);
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new Builder()
                .taskId(taskId)
                .lastUpdateTime(lastUpdateTime)
                .error(error)
                .state(state)
                .detectorId(detectorId)
                .progress(progress)
                .currentPiece(currentPiece)
                .executionStartTime(executionStartTime)
                .executionEndTime(executionEndTime)
                .isLatest(isLatest)
                .taskType(taskType)
                .checkpointId(checkpointId)
                .detector(detector)
                .build();
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ADTask that = (ADTask) o;
        return Objects.equal(getTaskId(), that.getTaskId())
                && Objects.equal(getLastUpdateTime(), that.getLastUpdateTime())
                && Objects.equal(getError(), that.getError())
                && Objects.equal(getState(), that.getState())
                && Objects.equal(getDetectorId(), that.getDetectorId())
                && Objects.equal(getProgress(), that.getProgress())
                && Objects.equal(getCurrentPiece(), that.getCurrentPiece())
                && Objects.equal(getExecutionStartTime(), that.getExecutionStartTime())
                && Objects.equal(getExecutionEndTime(), that.getExecutionEndTime())
                && Objects.equal(getLatest(), that.getLatest())
                && Objects.equal(getTaskType(), that.getTaskType())
                && Objects.equal(getCheckpointId(), that.getCheckpointId())
                && Objects.equal(getDetector(), that.getDetector());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(taskId, lastUpdateTime, error, state, detectorId, progress, currentPiece, executionStartTime, executionEndTime,
                isLatest, taskType, checkpointId, detector);
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Instant lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public void setDetectorId(String detectorId) {
        this.detectorId = detectorId;
    }

    public Float getProgress() {
        return progress;
    }

    public void setProgress(Float progress) {
        this.progress = progress;
    }

    public Instant getCurrentPiece() {
        return currentPiece;
    }

    public void setCurrentPiece(Instant currentPiece) {
        this.currentPiece = currentPiece;
    }

    public Instant getExecutionStartTime() {
        return executionStartTime;
    }

    public void setExecutionStartTime(Instant executionStartTime) {
        this.executionStartTime = executionStartTime;
    }

    public Instant getExecutionEndTime() {
        return executionEndTime;
    }

    public void setExecutionEndTime(Instant executionEndTime) {
        this.executionEndTime = executionEndTime;
    }

    public Boolean getLatest() {
        return isLatest;
    }

    public void setLatest(Boolean latest) {
        isLatest = latest;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public String getCheckpointId() {
        return checkpointId;
    }

    public void setCheckpointId(String checkpointId) {
        this.checkpointId = checkpointId;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public void setDetector(AnomalyDetector detector) {
        this.detector = detector;
    }
}
