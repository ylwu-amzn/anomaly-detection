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

package com.amazon.opendistroforelasticsearch.ad.transport;

import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class BatchAnomalyResultRequest extends ActionRequest implements TaskAwareRequest {
    static final String INVALID_TIMESTAMP_ERR_MSG = "timestamp is invalid";
//    static final String TASK_ID_JSON_KEY = "taskId";
//    static final String TASK_EXECUTION_ID_JSON_KEY = "taskExecutionId";
//    static final String START_JSON_KEY = "start";
//    static final String END_JSON_KEY = "end";
//    static final String NODE_ID_JSON_KEY = "end";
    public static final long MAX_COUNT = 3;

    private ADTask task;

    public BatchAnomalyResultRequest(StreamInput in) throws IOException {
        super(in);
        task = new ADTask(in);
    }

    public BatchAnomalyResultRequest(ADTask task) {
        super();
        this.task = task;
    }

    public ADTask getTask() {
        return task;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        task.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(task.getTaskId())) {
            validationException = addValidationError("Task id is missing", validationException);
        }
        AnomalyDetector detector = task.getDetector();
        if (detector == null) {
            validationException = addValidationError("Detector is missing", validationException);
        } else if (detector.isRealTimeDetector()) {
            validationException = addValidationError("Can't run batch task for realtime detector", validationException);
        } else if (detector.getDetectionDateRange().getStartTime() == null
                || detector.getDetectionDateRange().getEndTime() == null
                || detector.getDetectionDateRange().getStartTime().isAfter(detector.getDetectionDateRange().getEndTime())) {
            validationException = addValidationError(
                    String.format(Locale.ROOT, "%s: start %d, end %d", INVALID_TIMESTAMP_ERR_MSG,
                            detector.getDetectionDateRange().getStartTime(),
                            detector.getDetectionDateRange().getEndTime()),
                    validationException
            );

        }
        return validationException;
    }

//    @Override
//    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
//        builder.startObject();
//        builder.field(TASK_ID_JSON_KEY, taskId);
//        builder.field(TASK_EXECUTION_ID_JSON_KEY, taskExecutionId);
//        builder.field(START_JSON_KEY, start);
//        builder.field(END_JSON_KEY, end);
//        builder.field(NODE_ID_JSON_KEY, nodeId);
//        builder.endObject();
//        return builder;
//    }

    public static BatchAnomalyResultRequest fromActionRequest(final ActionRequest actionRequest) {
        if (actionRequest instanceof BatchAnomalyResultRequest) {
            return (BatchAnomalyResultRequest) actionRequest;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionRequest.writeTo(osso);
            try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new BatchAnomalyResultRequest(input);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse ActionRequest into AnomalyResultRequest", e);
        }
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        StringBuilder descriptionBuilder = new StringBuilder();
        descriptionBuilder.append("task_id:[").append(task.getTaskId()).append("], ");
        if (task.getDetector() != null) {
            descriptionBuilder.append("detector_id:[").append(task.getDetectorId()).append("], ");
            descriptionBuilder.append("start_time:[").append(task.getDetector().getDetectionDateRange().getStartTime()).append("], ");
            descriptionBuilder.append("end_time:[").append(task.getDetector().getDetectionDateRange().getEndTime()).append("]");
        }

        Map<String, Object> taskInfo = new HashMap<>();
        taskInfo.put("task_id", task.getTaskId());
        if (task.getDetector() != null) {
            taskInfo.put("detector_id", task.getDetectorId());
            taskInfo.put("start_date", task.getDetector().getDetectionDateRange().getStartTime());
            taskInfo.put("end_date", task.getDetector().getDetectionDateRange().getEndTime());
        }

        return new AnomalyDetectionBatchTask(id, type, action, descriptionBuilder.toString(), parentTaskId, headers, taskInfo);
    }

    @Override
    public boolean getShouldStoreResult() {
        return true;
    }
}
