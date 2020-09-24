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

import static org.elasticsearch.action.ValidateActions.addValidationError;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Locale;

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

public class ADTaskExecutionRequest extends ActionRequest implements ToXContentObject {
    static final String INVALID_TIMESTAMP_ERR_MSG = "timestamp is invalid";
    static final String TASK_ID_JSON_KEY = "taskId";
    static final String TASK_EXECUTION_ID_JSON_KEY = "taskExecutionId";
    static final String START_JSON_KEY = "start";
    static final String END_JSON_KEY = "end";
    static final String NODE_ID_JSON_KEY = "end";

    private String taskId;
    private String taskExecutionId;
    // time range start and end. Unit: epoch milliseconds
    private long start;
    private long end;
    private String nodeId;

    public ADTaskExecutionRequest(StreamInput in) throws IOException {
        super(in);
        taskId = in.readString();
        taskExecutionId = in.readString();
        start = in.readLong();
        end = in.readLong();
        nodeId = in.readString();
    }

    public ADTaskExecutionRequest(String taskId, String taskExecutionId, long start, long end, String nodeId) {
        super();
        this.taskId = taskId;
        this.taskExecutionId = taskExecutionId;
        this.start = start;
        this.end = end;
        this.nodeId = nodeId;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getTaskExecutionId() {
        return taskExecutionId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(taskId);
        out.writeString(taskExecutionId);
        out.writeLong(start);
        out.writeLong(end);
        out.writeString(nodeId);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(taskId)) {
            validationException = addValidationError("Task id is missing", validationException);
        }
        if (start <= 0 || end <= 0 || start > end) {
            validationException = addValidationError(
                String.format(Locale.ROOT, "%s: start %d, end %d", INVALID_TIMESTAMP_ERR_MSG, start, end),
                validationException
            );
        }
        if (StringUtils.isBlank(nodeId)) {
            validationException = addValidationError(String.format(Locale.ROOT, "Invalid node id"), validationException);
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TASK_ID_JSON_KEY, taskId);
        builder.field(TASK_EXECUTION_ID_JSON_KEY, taskExecutionId);
        builder.field(START_JSON_KEY, start);
        builder.field(END_JSON_KEY, end);
        builder.field(NODE_ID_JSON_KEY, nodeId);
        builder.endObject();
        return builder;
    }

    public static ADTaskExecutionRequest fromActionRequest(final ActionRequest actionRequest) {
        if (actionRequest instanceof ADTaskExecutionRequest) {
            return (ADTaskExecutionRequest) actionRequest;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionRequest.writeTo(osso);
            try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new ADTaskExecutionRequest(input);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse ActionRequest into AnomalyResultRequest", e);
        }
    }
}