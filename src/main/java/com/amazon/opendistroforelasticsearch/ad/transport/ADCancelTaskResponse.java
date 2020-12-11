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

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class ADCancelTaskResponse extends BaseNodesResponse<ADCancelTaskNodeResponse> implements ToXContentObject {

    private static final String NODES_KEY = "nodes";

    public ADCancelTaskResponse(StreamInput in) throws IOException {
        super(in);
    }

    public ADCancelTaskResponse(ClusterName clusterName, List<ADCancelTaskNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<ADCancelTaskNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public List<ADCancelTaskNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ADCancelTaskNodeResponse::readNodeResponse);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        String nodeId;
        DiscoveryNode node;
        builder.startObject(NODES_KEY);
        for (ADCancelTaskNodeResponse adTaskProfile : getNodes()) {
            node = adTaskProfile.getNode();
            nodeId = node.getId();
            builder.startObject(nodeId);
            adTaskProfile.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
