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
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectionTaskActionHandler;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_ANOMALY_DETECTORS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.IF_PRIMARY_TERM;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.IF_SEQ_NO;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.REFRESH;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.TASK_ID;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Rest handlers to create and update anomaly detector.
 */
public class RestIndexAnomalyDetectionTaskAction extends BaseRestHandler {

    private static final String INDEX_ANOMALY_DETECTION_TASK_ACTION = "index_anomaly_detection_task_action";
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final Logger logger = LogManager.getLogger(RestIndexAnomalyDetectionTaskAction.class);
    private final ClusterService clusterService;
    private final Settings settings;

    private volatile TimeValue requestTimeout;
    private volatile Integer maxAnomalyDetectionTasks;

    public RestIndexAnomalyDetectionTaskAction(
        Settings settings,
        ClusterService clusterService,
        AnomalyDetectionIndices anomalyDetectionIndices
    ) {
        this.settings = settings;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.requestTimeout = REQUEST_TIMEOUT.get(settings);
        this.maxAnomalyDetectionTasks = MAX_ANOMALY_DETECTORS.get(settings);
        this.clusterService = clusterService;
        // TODO: will add more cluster setting consumer later
        // TODO: inject ClusterSettings only if clusterService is only used to get ClusterSettings
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REQUEST_TIMEOUT, it -> requestTimeout = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_ANOMALY_DETECTORS, it -> maxAnomalyDetectionTasks = it);
    }

    @Override
    public String getName() {
        return INDEX_ANOMALY_DETECTION_TASK_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String taskId = request.param(TASK_ID, AnomalyDetector.NO_ID);
        logger.info("AnomalyDetectionTasks {} action for taskId {}", request.method(), taskId);

        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        AnomalyDetectionTask task = AnomalyDetectionTask.parse(parser, taskId);
        logger.info("Prepared task {}", task.toString());

        long seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO);
        long primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        WriteRequest.RefreshPolicy refreshPolicy = request.hasParam(REFRESH)
            ? WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
            : WriteRequest.RefreshPolicy.IMMEDIATE;

        return channel -> new IndexAnomalyDetectionTaskActionHandler(
            clusterService,
            client,
            channel,
            anomalyDetectionIndices,
            maxAnomalyDetectionTasks,
            task,
            seqNo,
            primaryTerm,
            refreshPolicy,
            requestTimeout
        ).start();
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                // Create
                new Route(RestRequest.Method.POST, AnomalyDetectorPlugin.AD_BASE_DETECTION_TASKS_URI),
                // update
                new Route(
                    RestRequest.Method.PUT,
                    String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTION_TASKS_URI, TASK_ID)
                )
            );
    }
}
