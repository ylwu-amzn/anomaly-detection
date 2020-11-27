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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager.AD_TASK_ID_HEADER;
import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ADBatchAnomalyResultRequest extends ActionRequest implements TaskAwareRequest {
    static final String INVALID_TIMESTAMP_ERR_MSG = "timestamp is invalid";

    private ADTask task;

    public ADBatchAnomalyResultRequest(StreamInput in) throws IOException {
        super(in);
        task = new ADTask(in);
    }

    public ADBatchAnomalyResultRequest(ADTask task) {
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
                String
                    .format(
                        Locale.ROOT,
                        "%s: start %d, end %d",
                        INVALID_TIMESTAMP_ERR_MSG,
                        detector.getDetectionDateRange().getStartTime(),
                        detector.getDetectionDateRange().getEndTime()
                    ),
                validationException
            );

        }
        return validationException;
    }

//    public static ADBatchAnomalyResultRequest fromActionRequest(final ActionRequest actionRequest) {
//        if (actionRequest instanceof ADBatchAnomalyResultRequest) {
//            return (ADBatchAnomalyResultRequest) actionRequest;
//        }
//
//        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
//            actionRequest.writeTo(osso);
//            try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
//                return new ADBatchAnomalyResultRequest(input);
//            }
//        } catch (IOException e) {
//            throw new IllegalArgumentException("failed to parse ActionRequest into AnomalyResultRequest", e);
//        }
//    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        StringBuilder descriptionBuilder = new StringBuilder();
        descriptionBuilder.append("task_id:[").append(task.getTaskId()).append("], ");
        if (task.getDetector() != null) {
            descriptionBuilder.append("detector_id:[").append(task.getDetectorId()).append("], ");
            descriptionBuilder.append("start_time:[").append(task.getDetector().getDetectionDateRange().getStartTime()).append("], ");
            descriptionBuilder.append("end_time:[").append(task.getDetector().getDetectionDateRange().getEndTime()).append("]");
        }

        headers.put(AD_TASK_ID_HEADER, task.getTaskId());

        return new ADTranspoertTask(id, type, action, descriptionBuilder.toString(), parentTaskId, headers);
    }

    @Override
    public boolean getShouldStoreResult() {
        return true;
    }
}
