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

import com.amazon.opendistroforelasticsearch.ad.task.ADTaskCancellationState;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class ADCancelTaskTransportAction extends
        TransportNodesAction<ADCancelTaskRequest, ADCancelTaskResponse, ADCancelTaskNodeRequest, ADCancelTaskNodeResponse> {

    private ADTaskManager adTaskManager;

    @Inject
    public ADCancelTaskTransportAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            ADTaskManager adTaskManager
    ) {
        super(
                ADCancelTaskAction.NAME,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                ADCancelTaskRequest::new,
                ADCancelTaskNodeRequest::new,
                ThreadPool.Names.MANAGEMENT,
                ADCancelTaskNodeResponse.class
        );
        this.adTaskManager = adTaskManager;
    }


    @Override
    protected ADCancelTaskResponse newResponse(ADCancelTaskRequest request, List<ADCancelTaskNodeResponse> responses, List<FailedNodeException> failures) {
        return new ADCancelTaskResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ADCancelTaskNodeRequest newNodeRequest(ADCancelTaskRequest request) {
        return new ADCancelTaskNodeRequest(request);
    }

    @Override
    protected ADCancelTaskNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ADCancelTaskNodeResponse(in);
    }

    @Override
    protected ADCancelTaskNodeResponse nodeOperation(ADCancelTaskNodeRequest request) {
        ADTaskCancellationState state = adTaskManager.cancelTask(request.getAdTaskId());
        return new ADCancelTaskNodeResponse(clusterService.localNode(), state);
    }
}
