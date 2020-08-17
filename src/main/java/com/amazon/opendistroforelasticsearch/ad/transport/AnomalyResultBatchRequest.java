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

import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonMessageAttributes;
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
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class AnomalyResultBatchRequest extends ActionRequest implements ToXContentObject, TaskAwareRequest {
    static final String INVALID_TIMESTAMP_ERR_MSG = "timestamp is invalid";
    static final String START_JSON_KEY = "start";
    static final String END_JSON_KEY = "end";

    private String detectorId;
    private String taskExecutionId;
    // time range start and end. Unit: epoch milliseconds
    private long start;
    private long end;

    public AnomalyResultBatchRequest(StreamInput in) throws IOException {
        super(in);
        detectorId = in.readString();
        taskExecutionId = in.readString();
        start = in.readLong();
        end = in.readLong();
    }

    public AnomalyResultBatchRequest(String detectorId, String taskExecutionId, long start, long end) {
        super();
        this.detectorId = detectorId;
        this.taskExecutionId = taskExecutionId;
        this.start = start;
        this.end = end;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public String getTaskExecutionId() {
        return taskExecutionId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(detectorId);
        out.writeString(taskExecutionId);
        out.writeLong(start);
        out.writeLong(end);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(detectorId)) {
            validationException = addValidationError(CommonErrorMessages.AD_ID_MISSING_MSG, validationException);
        }
        if (start <= 0 || end <= 0 || start > end) {
            validationException = addValidationError(
                String.format(Locale.ROOT, "%s: start %d, end %d", INVALID_TIMESTAMP_ERR_MSG, start, end),
                validationException
            );
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CommonMessageAttributes.ID_JSON_KEY, detectorId);
        builder.field(START_JSON_KEY, start);
        builder.field(END_JSON_KEY, end);
        builder.endObject();
        return builder;
    }

    public static AnomalyResultBatchRequest fromActionRequest(final ActionRequest actionRequest) {
        if (actionRequest instanceof AnomalyResultBatchRequest) {
            return (AnomalyResultBatchRequest) actionRequest;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionRequest.writeTo(osso);
            try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new AnomalyResultBatchRequest(input);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse ActionRequest into AnomalyResultRequest", e);
        }
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        StringBuilder descriptionBuilder = new StringBuilder();
        descriptionBuilder.append("task_execution_id[").append(this.taskExecutionId).append("], ");
        descriptionBuilder.append("detector_id[").append(this.detectorId).append("], ");
        descriptionBuilder.append("start_date[").append(this.start).append("], ");
        descriptionBuilder.append("end_date[").append(this.end).append("]");
        return new AnomalyDetectionBatchTask(id, type, action, descriptionBuilder.toString(), parentTaskId, headers);
    }
}
