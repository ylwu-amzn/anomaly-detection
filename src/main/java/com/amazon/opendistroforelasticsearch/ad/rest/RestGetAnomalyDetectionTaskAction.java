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

import static com.amazon.opendistroforelasticsearch.ad.task.AnomalyDetectionTaskManager.GET_TASK_RESPONSE;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.TASK_ID;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.onFailure;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTask;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.task.AnomalyDetectionTaskManager;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to retrieve an anomaly detection task.
 */
public class RestGetAnomalyDetectionTaskAction extends BaseRestHandler {

    private static final String GET_ANOMALY_DETECTION_TASK_ACTION = "get_anomaly_detection_task";
    private static final Logger logger = LogManager.getLogger(RestGetAnomalyDetectionTaskAction.class);
    public static final String EXECUTION = "execution";
    private final AnomalyDetectionTaskManager anomalyDetectionTaskManager;

    public RestGetAnomalyDetectionTaskAction(AnomalyDetectionTaskManager anomalyDetectionTaskManager) {
        this.anomalyDetectionTaskManager = anomalyDetectionTaskManager;
    }

    @Override
    public String getName() {
        return GET_ANOMALY_DETECTION_TASK_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        String taskId = request.param(TASK_ID);
        boolean taskExecution = request.paramAsBoolean(EXECUTION, false);

        return channel -> {
            anomalyDetectionTaskManager.getTask(taskId, taskExecution, ActionListener.wrap(r -> {
                if (r.containsKey(RestHandlerUtils.ANOMALY_DETECTION_TASK)) {
                    AnomalyDetectionTask task = (AnomalyDetectionTask) r.get(RestHandlerUtils.ANOMALY_DETECTION_TASK);
                    GetResponse response = (GetResponse) r.get(GET_TASK_RESPONSE);
                    XContentBuilder builder = channel
                        .newBuilder()
                        .startObject()
                        .field(RestHandlerUtils._ID, response.getId())
                        .field(RestHandlerUtils._VERSION, response.getVersion())
                        .field(RestHandlerUtils._PRIMARY_TERM, response.getPrimaryTerm())
                        .field(RestHandlerUtils._SEQ_NO, response.getSeqNo())
                        .field(RestHandlerUtils.ANOMALY_DETECTION_TASK, task);
                    if (r.containsKey(RestHandlerUtils.ANOMALY_DETECTION_TASK_EXECUTION)) {
                        builder
                            .field(
                                RestHandlerUtils.ANOMALY_DETECTION_TASK_EXECUTION,
                                r.get(RestHandlerUtils.ANOMALY_DETECTION_TASK_EXECUTION)
                            );
                    }
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                } else {
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, "Task not found"));
                }
            }, e -> {
                logger.error("Fail to get anomaly detection task " + taskId, e);
                onFailure(channel, e);
            }));
        };

    }

    @Override
    public List<Route> routes() {
        String path = String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTION_TASKS_URI, TASK_ID);
        return ImmutableList.of(new Route(RestRequest.Method.GET, path), new Route(RestRequest.Method.HEAD, path));
    }
}
