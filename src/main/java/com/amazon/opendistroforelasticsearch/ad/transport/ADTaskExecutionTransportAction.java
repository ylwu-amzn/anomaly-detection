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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;

public class ADTaskExecutionTransportAction extends HandledTransportAction<ActionRequest, AnomalyResultBatchResponse> {

    private static final Logger LOG = LogManager.getLogger(ADTaskExecutionTransportAction.class);

    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public ADTaskExecutionTransportAction(ActionFilters actionFilters, TransportService transportService, ClusterService clusterService) {
        super(ADTaskExecutionAction.NAME, transportService, actionFilters, AnomalyResultBatchRequest::new);
        this.transportService = transportService;
        this.clusterService = clusterService;
    }

    private DiscoveryNode getNode(String nodeId) {
        ClusterState state = this.clusterService.state();
        DiscoveryNode eligibleNode = null;
        for (DiscoveryNode node : state.nodes()) {
            if (node.getId().equals(nodeId)) {
                eligibleNode = node;
                break;
            }
        }
        return eligibleNode;
    }

    @Override
    protected void doExecute(Task task, ActionRequest actionRequest, ActionListener<AnomalyResultBatchResponse> actionListener) {
        ADTaskExecutionRequest request = ADTaskExecutionRequest.fromActionRequest(actionRequest);
        String nodeId = request.getNodeId();

        DiscoveryNode eligibleNode = getNode(nodeId);
        if (eligibleNode == null) {
            actionListener.onFailure(new ResourceNotFoundException("Can't find node " + nodeId));
            return;
        }
        AnomalyResultBatchRequest anomalyResultBatchRequest = new AnomalyResultBatchRequest(
            request.getTaskId(),
            request.getTaskExecutionId(),
            request.getStart(),
            request.getEnd(),
            nodeId
        );
        transportService
            .sendRequest(
                eligibleNode,
                AnomalyResultBatchAction.NAME,
                anomalyResultBatchRequest,
                new TransportResponseHandler<AnomalyResultBatchResponse>() {
                    @Override
                    public AnomalyResultBatchResponse read(StreamInput in) throws IOException {
                        return new AnomalyResultBatchResponse(in);
                    }

                    @Override
                    public void handleResponse(AnomalyResultBatchResponse response) {
                        actionListener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        actionListener.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                }
            );
    }

}
