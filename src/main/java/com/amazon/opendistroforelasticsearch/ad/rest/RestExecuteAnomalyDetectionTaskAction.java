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

import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.START_JOB;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.STOP_JOB;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.TASK_ID;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.task.AnomalyDetectionTaskManager;
import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to handle request to start/stop AD job.
 */
public class RestExecuteAnomalyDetectionTaskAction extends BaseRestHandler {
    private static final Logger log = LogManager.getLogger(RestExecuteAnomalyDetectionTaskAction.class);
    public static final String AD_JOB_ACTION = "anomaly_detection_task_action";
    private final AnomalyDetectionTaskManager anomalyDetectionTaskManager;

    public RestExecuteAnomalyDetectionTaskAction(AnomalyDetectionTaskManager anomalyDetectionTaskManager) {
        this.anomalyDetectionTaskManager = anomalyDetectionTaskManager;
    }

    @Override
    public String getName() {
        return AD_JOB_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String taskId = request.param(TASK_ID);
        String rawPath = request.rawPath();

        return channel -> {
            try {
                if (rawPath.endsWith(START_JOB)) {
                    anomalyDetectionTaskManager.startTask(taskId, ActionListener.wrap(taskExecutionId -> {
                        XContentBuilder builder = channel.newBuilder().startObject().field("taskExecutionId", taskExecutionId).endObject();
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                    }, e -> { onFailure(channel, e); }));
                } else if (rawPath.endsWith(STOP_JOB)) {
                    anomalyDetectionTaskManager
                        .stopTask(
                            taskId,
                            ActionListener
                                .wrap(r -> { channel.sendResponse(new BytesRestResponse(RestStatus.OK, r)); }, e -> onFailure(channel, e))
                        );
                }
            } catch (Exception e) {
                log.error("Fail to parse task " + taskId, e);
                onFailure(channel, e);
            }
        };
    }

    public void onFailure(RestChannel channel, Exception e) {
        if (e != null) {
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (IOException e1) {
                log.warn("Fail to send out failure message of exception", e);
            }
        }
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTION_TASKS_URI, TASK_ID, START_JOB)
                ),

                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTION_TASKS_URI, TASK_ID, STOP_JOB)
                )
            );
    }
}
