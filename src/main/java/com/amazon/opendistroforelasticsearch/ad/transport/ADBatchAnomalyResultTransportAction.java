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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.task.ADBatchTaskRunner;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.AnomalyResultBulkIndexHandler;
import com.amazon.randomcutforest.RandomCutForest;
import com.google.common.util.concurrent.RateLimiter;

public class ADBatchAnomalyResultTransportAction extends HandledTransportAction<ADBatchAnomalyResultRequest, ADBatchAnomalyResultResponse> {

    // TODO: unify the logger name, we have LOG, log, logger currently
    private static final Logger LOG = LogManager.getLogger(ADBatchAnomalyResultTransportAction.class);
    static final String NODE_UNRESPONSIVE_ERR_MSG = "Model node is unresponsive.  Mute model";
    static final String READ_WRITE_BLOCKED = "Cannot read/write due to global block.";
    static final String INDEX_READ_BLOCKED = "Cannot read user index due to read block.";
    // TODO: test performance
    private final RateLimiter rateLimiter = RateLimiter.create(1);
    // TODO: limit how many running tasks

    private final Client client;
    private final TransportService transportService;
    private final FeatureManager featureManager;
    private final ModelManager modelManager;
    private final HashRing hashRing;
    private final TransportRequestOptions option;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ADStats adStats;
    private final ADCircuitBreakerService adCircuitBreakerService;
    private final Map<String, RandomCutForest> taskRcfMap;
    private final Map<String, ThresholdingModel> taskThresholdModelMap;
    private final Map<String, Boolean> taskThresholdModelTrainedMap;
    private final Map<String, List<Double>> taskTrainingDataMap; // TODO, check if this class is singleton or not
    private final AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler;
    // TODO: make the dynamic setting
    private final ThreadPool threadPool;
    private final ADTaskManager adTaskManager;
    private final ADBatchTaskRunner adBatchTaskRunner;

    @Inject
    public ADBatchAnomalyResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Client client,
        Settings settings,
        FeatureManager featureManager,
        ModelManager modelManager,
        HashRing hashRing,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ADCircuitBreakerService adCircuitBreakerService,
        ADStats adStats,
        AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler,
        ThreadPool threadPool,
        ADTaskManager adTaskManager,
        ADBatchTaskRunner adBatchTaskRunner
    ) {
        super(ADBatchAnomalyResultAction.NAME, transportService, actionFilters, ADBatchAnomalyResultRequest::new);
        this.client = client;
        this.transportService = transportService;
        this.featureManager = featureManager;
        this.modelManager = modelManager;
        this.hashRing = hashRing;
        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings))
            .build();
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.adStats = adStats;
        this.adTaskManager = adTaskManager;
        this.adBatchTaskRunner = adBatchTaskRunner;
        taskRcfMap = new ConcurrentHashMap<>();
        taskThresholdModelMap = new ConcurrentHashMap<>();
        this.anomalyResultBulkIndexHandler = anomalyResultBulkIndexHandler;
        taskThresholdModelTrainedMap = new ConcurrentHashMap<>();
        taskTrainingDataMap = new ConcurrentHashMap<>();
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, ADBatchAnomalyResultRequest request, ActionListener<ADBatchAnomalyResultResponse> actionListener) {
        adBatchTaskRunner.run(request.getAdTask(), task, transportService, actionListener);
    }
}
