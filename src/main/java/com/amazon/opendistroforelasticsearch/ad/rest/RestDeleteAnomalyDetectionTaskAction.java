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

package com.amazon.opendistroforelasticsearch.ad.rest;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTask;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.AnomalyDetectorActionHandler;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.TASK_ID;

/**
 * This class consists of the REST handler to delete anomaly detector.
 */
public class RestDeleteAnomalyDetectionTaskAction extends BaseRestHandler {

    public static final String DELETE_ANOMALY_DETECTION_TASK_ACTION = "delete_anomaly_detection_task";

    private static final Logger logger = LogManager.getLogger(RestDeleteAnomalyDetectionTaskAction.class);
    private final ClusterService clusterService;
    private final AnomalyDetectorActionHandler handler = new AnomalyDetectorActionHandler();

    public RestDeleteAnomalyDetectionTaskAction(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String getName() {
        return DELETE_ANOMALY_DETECTION_TASK_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String taskId = request.param(TASK_ID);

        //TODO: stop running task first
        return channel -> {
            logger.info("Delete anomaly detection task {}", taskId);
            handler
                .getDetectorJob(
                    clusterService,
                    client,
                    taskId,
                    channel,
                    () -> deleteAnomalyDetectionTask(client, taskId, channel)
                );
        };
    }

    private void deleteAnomalyDetectionTask(NodeClient client, String taskId, RestChannel channel) {
        logger.info("Delete anomaly detection task {}", taskId);
        DeleteRequest deleteRequest = new DeleteRequest(AnomalyDetectionTask.ANOMALY_DETECTION_TASK_INDEX, taskId)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.delete(deleteRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                // delete anomaly detection task document
                new Route(
                    RestRequest.Method.DELETE,
                    String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTION_TASKS_URI, TASK_ID)
                )
            );
    }
}
