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
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ADTaskProfileRequest extends BaseNodesRequest<ADTaskProfileRequest> {

    private String adTaskId;

    public ADTaskProfileRequest(StreamInput in) throws IOException {
        super(in);
        this.adTaskId = in.readString();
    }

    public ADTaskProfileRequest(String adTaskId, String... nodeIds) {
        super(nodeIds);
        this.adTaskId = adTaskId;
    }

    public ADTaskProfileRequest(String adTaskId, DiscoveryNode... nodes) {
        super(nodes);
        this.adTaskId = adTaskId;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(adTaskId)) {
            validationException = addValidationError(CommonErrorMessages.AD_TASK_ID_MISSING_MSG, validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(adTaskId);
    }

    public String getAdTaskId() {
        return adTaskId;
    }
}
