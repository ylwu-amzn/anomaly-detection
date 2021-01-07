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

import java.io.IOException;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class ADCancelTaskRequest extends BaseNodesRequest<ADCancelTaskRequest> {

    private String detectorId;
    private String taskId;
    private String userName;

    public ADCancelTaskRequest(StreamInput in) throws IOException {
        super(in);
        this.detectorId = in.readOptionalString();
        this.taskId = in.readOptionalString();
        this.userName = in.readOptionalString();
    }

    public ADCancelTaskRequest(String detectorId, String userName, String... nodeIds) {
        this(detectorId, null, userName, nodeIds);
    }

    public ADCancelTaskRequest(String detectorId, String taskId, String userName, String... nodeIds) {
        super(nodeIds);
        this.detectorId = detectorId;
        this.taskId = taskId;
        this.userName = userName;
    }

    public ADCancelTaskRequest(String detectorId, String userName, DiscoveryNode... nodes) {
        this(detectorId, null, userName, nodes);
    }

    public ADCancelTaskRequest(String detectorId, String taskId, String userName, DiscoveryNode... nodes) {
        super(nodes);
        this.detectorId = detectorId;
        this.taskId = taskId;
        this.userName = userName;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(detectorId) && Strings.isEmpty(taskId)) {
            validationException = addValidationError("Both detector id and task id missing", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(detectorId);
        out.writeOptionalString(taskId);
        out.writeOptionalString(userName);
    }

    public String getDetectorId() {
        return detectorId;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getUserName() {
        return userName;
    }
}
