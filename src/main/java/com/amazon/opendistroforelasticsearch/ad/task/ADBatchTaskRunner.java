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

package com.amazon.opendistroforelasticsearch.ad.task;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ADTaskCancelledException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.feature.SinglePointFeatures;
import com.amazon.opendistroforelasticsearch.ad.indices.ADIndex;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.DetectionDateRange;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchTaskRemoteExecutionAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsNodeResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsNodesAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.AnomalyResultBulkIndexHandler;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;
import com.amazon.randomcutforest.RandomCutForest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.InternalMin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin.AD_BATCH_TASK_THREAD_POOL_NAME;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.CURRENT_PIECE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.EXECUTION_END_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.INIT_PROGRESS_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STATE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.TASK_PROGRESS_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PIECE_SIZE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_MIN_SAMPLES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.THRESHOLD_MODEL_TRAINING_SIZE;

public class ADBatchTaskRunner {
    public static final String MIN_DATE = "min_date";
    public static final String MAX_DATE = "max_date";
    private final Logger logger = LogManager.getLogger(ADBatchTaskRunner.class);
    // TODO: tune threshold model training size
//    private final Integer THRESHOLD_MODEL_TRAINING_SIZE = 1000;

    // TODO: test performance
    private final RateLimiter rateLimiter = RateLimiter.create(1);

    private final ThreadPool threadPool;
    private final Client client;
    private final ADStats adStats;
    private final DiscoveryNodeFilterer nodeFilter;
    private final ClusterService clusterService;
    private final FeatureManager featureManager;
    private final ADCircuitBreakerService adCircuitBreakerService;
    private final ADTaskManager adTaskManager;
    private final AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private AnomalyDetectionIndices anomalyDetectionIndices;

    private final ADTaskCache adBatchTaskCache;
    private final TransportRequestOptions option; //TODO, test this config

    private volatile Integer maxAdBatchTaskPerNode;
    private volatile Integer pieceSize;
    private volatile Integer pieceIntervalSeconds;

    public ADBatchTaskRunner(
            Settings settings,
            ThreadPool threadPool,
            ClusterService clusterService,
            Client client,
            DiscoveryNodeFilterer nodeFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            ADCircuitBreakerService adCircuitBreakerService,
            FeatureManager featureManager,
            ADTaskManager adTaskManager,
            AnomalyDetectionIndices anomalyDetectionIndices,
            ADStats adStats,
            AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler,
            ADTaskCache adBatchTaskCache) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
        this.anomalyResultBulkIndexHandler = anomalyResultBulkIndexHandler;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.nodeFilter = nodeFilter;
        this.adStats = adStats;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.adTaskManager = adTaskManager;
        this.featureManager = featureManager;
        this.anomalyDetectionIndices = anomalyDetectionIndices;

        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings))
            .build();

        this.adBatchTaskCache = adBatchTaskCache;

        this.maxAdBatchTaskPerNode = MAX_BATCH_TASK_PER_NODE.get(settings);
        clusterService
                .getClusterSettings()
                .addSettingsUpdateConsumer(MAX_BATCH_TASK_PER_NODE, it -> maxAdBatchTaskPerNode = it);

        this.pieceSize = MAX_BATCH_TASK_PIECE_SIZE.get(settings);
        clusterService
                .getClusterSettings()
                .addSettingsUpdateConsumer(MAX_BATCH_TASK_PIECE_SIZE, it -> pieceSize = it);

        this.pieceIntervalSeconds = MAX_BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings);
        clusterService
                .getClusterSettings()
                .addSettingsUpdateConsumer(MAX_BATCH_TASK_PIECE_INTERVAL_SECONDS, it -> pieceIntervalSeconds = it);
    }

    public void run(ADTask adTask, Task task, TransportService transportService, ActionListener<ADBatchAnomalyResultResponse> listener) {
        Map<String, Object> updatedFields = new HashMap<>();
        updatedFields.put(STATE_FIELD, ADTaskState.INIT.name());
        updatedFields.put(INIT_PROGRESS_FIELD, 0.0f);
        adTaskManager.updateADTask(adTask.getTaskId(), updatedFields,
                ActionListener.wrap(r -> {
                    getNodeStats(adTask, ActionListener.wrap(node -> {
                        if (clusterService.localNode().getId().equals(node.getId())) {
                            // Execute batch task locally
                            logger.info("execute task {} locally on node {} for detector {}", adTask.getTaskId(), node.getId(), adTask.getDetectorId());
                            startADBatchTask(adTask, task, listener);
                        } else {
                            // Execute batch task remotely
                            logger.info("execute task {} remotely on node {} for detector {}", adTask.getTaskId(), node.getId(), adTask.getDetectorId());
                            transportService
                                    .sendRequest(
                                            node,
                                            ADBatchTaskRemoteExecutionAction.NAME,
                                            new ADBatchAnomalyResultRequest(adTask),
                                            option,
                                            new ActionListenerResponseHandler<>(listener, ADBatchAnomalyResultResponse::new)
                                    );
                        }
                    }, e -> listener.onFailure(e)));
                }, e -> {
                    logger.warn("Failed to move task to INIT state, task id " + adTask.getTaskId());
                    listener.onFailure(e);
                }));
    }

    private void getNodeStats(ADTask adTask, ActionListener<DiscoveryNode> listener) {
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        ADStatsRequest adStatsRequest = new ADStatsRequest(dataNodes);
        adStatsRequest.addAll(ImmutableSet.of(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName(),
                StatNames.JVM_HEAP_USAGE.getName()));

        client.execute(ADStatsNodesAction.INSTANCE, adStatsRequest, ActionListener.wrap(adStatsResponse -> {
            List<ADStatsNodeResponse> candidateNodeResponse = adStatsResponse
                .getNodes()
                .stream()
                // .filter(stat -> (short) stat.getStatsMap().get(StatNames.MEMORY_USAGE.name()) < 85 && (long)
                // stat.getStatsMap().get(StatNames.AD_EXECUTING_TASK_COUNT.name()) < 10)
                .filter(stat -> (Long) stat.getStatsMap().get(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()) < maxAdBatchTaskPerNode)
                // .sorted((ADStatsNodeResponse r1, ADStatsNodeResponse r2) -> ((Short)
                // r1.getStatsMap().get(StatNames.MEMORY_USAGE.name())).compareTo((short)
                // r2.getStatsMap().get(StatNames.MEMORY_USAGE.name())))
                .sorted(
                    (ADStatsNodeResponse r1, ADStatsNodeResponse r2) -> ((Long) r1
                        .getStatsMap()
                        .get(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()))
                            .compareTo((Long) r2.getStatsMap().get(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()))
                )
                .collect(Collectors.toList());
            candidateNodeResponse.stream().forEach(c -> {
                logger.info("Node AD stats: node id: {}, running_task: {}, jvm_heap_usage: {}",
                        c.getNode().getId(),
                        c.getStatsMap().get(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()),
                        c.getStatsMap().get(StatNames.JVM_HEAP_USAGE.getName()));
            });
            if (candidateNodeResponse.size() > 0) {
                if (candidateNodeResponse.size() == 1) {
                    logger.info("Dispatch AD task to node: {}", candidateNodeResponse.get(0).getNode().getId());
                    listener.onResponse(candidateNodeResponse.get(0).getNode());
                } else {
                    Long taskCount = (Long)candidateNodeResponse.get(0).getStatsMap().get(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName());
                    Optional<ADStatsNodeResponse> first = candidateNodeResponse.stream().filter(c -> taskCount.equals(c.getStatsMap().get(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName())))
                            .sorted((ADStatsNodeResponse r1, ADStatsNodeResponse r2) -> ((Long) r1
                                    .getStatsMap()
                                    .get(StatNames.JVM_HEAP_USAGE.getName()))
                                    .compareTo((Long) r2.getStatsMap().get(StatNames.JVM_HEAP_USAGE.getName()))).findFirst();
                    logger.info("Will Dispatch AD task to node: {}", first.get().getNode().getId());
                    listener.onResponse(first.get().getNode());
                }

            } else {
                String errorMessage = "No eligible node to run detector " + adTask.getDetectorId();
                logger.warn(errorMessage);
                listener.onFailure(new LimitExceededException(adTask.getDetectorId(), errorMessage));
            }
        }, exception -> {
            logger.error("Failed to get node's task stats", exception);
            listener.onFailure(exception);
        }));
    }

    public void startADBatchTask(ADTask adTask, Task task, ActionListener<ADBatchAnomalyResultResponse> listener) {
        try {
            if (!EnabledSetting.isADPluginEnabled()) {
                throw new EndRunException(adTask.getDetectorId(), CommonErrorMessages.DISABLED_ERR_MSG, true);
            }
            threadPool.executor(AD_BATCH_TASK_THREAD_POOL_NAME).execute(() -> {
                try {
//                    executeADBatchTask(adTask, task, listener);
                    executeADBatchTask(adTask, task);
                } catch (Exception e) {
                    listener.onFailure(e);
                    adTaskManager.handleADTaskException(adTask, e);
                }
            });
            listener.onResponse(new ADBatchAnomalyResultResponse("Task started on node " + clusterService.localNode().getId()));
        } catch (Exception e) {
            logger.error("Fail to start AD batch task " + adTask.getTaskId(), e);
            listener.onFailure(e);
        }
    }

    private void executeADBatchTask(ADTask adTask, Task task/*,ActionListener<ADBatchAnomalyResultResponse> actionListener*/) {
        String taskId = adTask.getTaskId();

        // wrap original listener to process before return response/failure: 1.clean cache 2.track task stats
//        ActionListener<ADBatchAnomalyResultResponse> listener = wrappedListener(adTask, actionListener);
        ActionListener<ADBatchAnomalyResultResponse> listener = wrappedListener(adTask);

        // every put action will check if exceeds task limitation, so no need to
        // call checkBatchTaskLimitation() here
        adStats.getStat(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()).increment();
        adStats.getStat(StatNames.AD_TOTAL_BATCH_TASK_EXECUTION_COUNT.getName()).increment();
        adBatchTaskCache.put(adTask);
//        adBatchTaskCache.putAdTransportTask(taskId, (ADTranspoertTask)task);

        // check if circuit breaker is open
        checkCircuitBreaker(adTask, listener);

        Instant executeStartTime = Instant.now();
        runFirstPiece(adTask, executeStartTime, listener);
    }

    private void checkCircuitBreaker(ADTask adTask, ActionListener<ADBatchAnomalyResultResponse> listener) {
        String taskId = adTask.getTaskId();
        if (adCircuitBreakerService.isOpen()) {
            String error = "Circuit breaker is open";
            logger.error("AD task: {}, {}", taskId, error);
            throw new LimitExceededException(adTask.getDetectorId(), error, true);
        }
    }

    private ActionListener<ADBatchAnomalyResultResponse> wrappedListener(ADTask adTask/*, ActionListener<ADBatchAnomalyResultResponse> actionListener*/) {
        // wrap original listener to process before return response/failure: 1.clean cache 2.track task stats
        String taskId = adTask.getTaskId();
        ActionListener<ADBatchAnomalyResultResponse> listener = ActionListener.wrap(response -> {
            adBatchTaskCache.remove(taskId);
            adStats.getStat(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()).decrement();
//            actionListener.onResponse(response);
        }, e -> {
            adBatchTaskCache.remove(taskId);
            adStats.getStat(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()).decrement();
            if (e instanceof TaskCancelledException) {
                adStats.getStat(StatNames.AD_CANCELED_BATCH_TASK_COUNT.getName()).increment();
            } else if (ExceptionUtil.isServerError(e)) {
                adStats.getStat(StatNames.AD_BATCH_TASK_FAILURE_COUNT.getName()).increment();
            }
//            actionListener.onFailure(e);
            adTaskManager.handleADTaskException(adTask, e);
        });
        return listener;
    }

    private void runFirstPiece(ADTask adTask, Instant executeStartTime,
                               ActionListener<ADBatchAnomalyResultResponse> listener) {
        try {
            adTaskManager
                .updateADTask(
                    adTask.getTaskId(),
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            ADTaskState.INIT.name(),
                            CURRENT_PIECE_FIELD,
                            adTask.getDetector().getDetectionDateRange().getStartTime().toEpochMilli(),
                                TASK_PROGRESS_FIELD,
                            0.0f,
                                INIT_PROGRESS_FIELD,
                                0.0f
                        ),
                    ActionListener.wrap(r -> {
                        try {
                            checkIfADTaskCancelled(adTask.getTaskId());
                            if (!shouldStart(listener, adTask)) {
                                return;
                            }
                            getDateRangeOfSourceData(adTask, (minDate, maxDate) -> {
                                long interval = ((IntervalTimeConfiguration) adTask.getDetector().getDetectionInterval())
                                        .toDuration()
                                        .toMillis();
                                if (maxDate - minDate < NUM_MIN_SAMPLES * interval) {
                                    listener.onFailure(new IllegalArgumentException(
                                            "There is no enough data to train model"));
                                    return;
                                }
                                DetectionDateRange detectionDateRange = adTask.getDetector().getDetectionDateRange();
                                long dataStartTime = detectionDateRange.getStartTime().toEpochMilli();
                                long dataEndTime = detectionDateRange.getEndTime().toEpochMilli();

                                if (minDate >= dataEndTime) {
                                    listener.onFailure(new ResourceNotFoundException(adTask.getDetectorId(),
                                            "There is no data in the detection date range"));
                                    return;
                                }
                                if (minDate > dataStartTime) {
                                    dataStartTime = minDate;
                                }
                                if (maxDate < dataEndTime) {
                                    dataEndTime = maxDate;
                                }

                                // normalize start/end time to make it consistent with feature data agg result
                                dataStartTime = dataStartTime - dataStartTime % interval;
                                dataEndTime = dataEndTime - dataEndTime % interval;
                                long expectedPieceEndTime = dataStartTime + pieceSize * interval;
                                long firstPieceEndTime = expectedPieceEndTime > dataEndTime ? dataEndTime : expectedPieceEndTime;
                                logger
                                        .info(
                                                "start first piece from {} to {}, interval {}, dataStartTime {}, dataEndTime {}, detectorId {}, taskId {}",
                                                dataStartTime,
                                                firstPieceEndTime,
                                                interval,
                                                dataStartTime,
                                                dataEndTime,
                                                adTask.getDetectorId(),
                                                adTask.getTaskId()
                                        );
                                getFeatureData(
                                        adTask,
                                        dataStartTime, // first piece start time
                                        firstPieceEndTime, // first piece end time
                                        dataStartTime,
                                        dataEndTime,
                                        interval,
                                        executeStartTime,
                                        listener
                                );
                            }, listener);
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }, e -> {
                        listener.onFailure(e);
                    })
                );
        } catch (Exception exception) {
            listener.onFailure(exception);
        }
    }

    private void getDateRangeOfSourceData(ADTask adTask, BiConsumer<Long, Long> consumer, ActionListener listener) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .aggregation(AggregationBuilders.min(MIN_DATE).field(adTask.getDetector().getTimeField()))
                .aggregation(AggregationBuilders.max(MAX_DATE).field(adTask.getDetector().getTimeField()))
                .size(0);
        SearchRequest request = new SearchRequest()
                .indices(adTask.getDetector().getIndices().toArray(new String[0]))
                .source(searchSourceBuilder);

        client.search(request, ActionListener.wrap( r-> {
            InternalMin minAgg = r.getAggregations().get(MIN_DATE);
            InternalMax maxAgg = r.getAggregations().get(MAX_DATE);
            double minValue = minAgg.getValue();
            double maxValue = maxAgg.getValue();
            // If time field not exist or there is no value, will return infinity value
            if (minValue == Double.POSITIVE_INFINITY) {
                listener.onFailure(new ResourceNotFoundException(adTask.getDetectorId(),
                        "There is no data in the time field"));
                return;
            }
            consumer.accept((long)minValue, (long)maxValue);
        }, e -> listener.onFailure(e)));
    }

    private void getFeatureData(
        ADTask adTask,
        long pieceStartTime,
        long pieceEndTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        Instant executeStartTime,
        ActionListener<ADBatchAnomalyResultResponse> listener
    ) {
        ActionListener<Map<Long, Optional<double[]>>> actionListener = ActionListener.wrap(dataPoints -> {
            try {
                if (dataPoints.size() == 0) {
                    logger.info("No data in current piece with end time: " + pieceEndTime);
                    runNextPiece(adTask, pieceEndTime, dataStartTime, dataEndTime, interval, listener);
                } else {
                    detectAnomaly(
                            adTask,
                            adTask.getDetector().getEnabledFeatureIds().size(),
                            dataPoints,
                            pieceStartTime,
                            pieceEndTime,
                            dataStartTime,
                            dataEndTime,
                            interval,
                            executeStartTime,
                            listener
                    );
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }, exception -> {
            logger.error("Fail to execute onFeatureResponseLocalRCF", exception);
            listener.onFailure(exception);
        });
        ThreadedActionListener threadedActionListener = new ThreadedActionListener<>(logger, threadPool, AD_BATCH_TASK_THREAD_POOL_NAME, actionListener, false);

        featureManager.getFeatureDataPoints(adTask.getDetector(), pieceStartTime,pieceEndTime, threadedActionListener);
    }

    private void detectAnomaly(
            ADTask adTask,
            int enabledFeatureSize,
            Map<Long, Optional<double[]>> dataPoints,
            long pieceStartTime,
            long pieceEndTime,
            long dataStartTime,
            long dataEndTime,
            long interval,
            Instant executeStartTime,
            ActionListener<ADBatchAnomalyResultResponse> listener
    ) {
        String taskId = adTask.getTaskId();
        int shingleSize = adTask.getDetector().getShingleSize();
        RandomCutForest rcf = adBatchTaskCache.getOrCreateRcfModel(taskId, shingleSize, enabledFeatureSize);
        ThresholdingModel threshold = adBatchTaskCache.getOrCreateThresholdModel(taskId);
        List<Double> thresholdTrainingScores = adBatchTaskCache.getThresholdTrainingData(taskId);

        List<AnomalyResult> anomalyResults = new ArrayList<>();

        Deque<Map.Entry<Long, Optional<double[]>>> shingle = adBatchTaskCache.getShingle(taskId);

        boolean thresholdTrained = adBatchTaskCache.isThresholdModelTrained(taskId);
        long intervalEndTime = pieceStartTime;
        for (int i = 0; i < pieceSize && intervalEndTime < dataEndTime; i++) {
            intervalEndTime = intervalEndTime + interval;
            SinglePointFeatures feature =
                    featureManager.getShingledFeature(adTask.getDetector(), shingle, dataPoints, intervalEndTime);
            List<FeatureData> featureData = null;
            if (feature.getUnprocessedFeatures().isPresent()) {
                featureData = ParseUtils.getFeatureData(feature.getUnprocessedFeatures().get(), adTask.getDetector());
            }
            if (!feature.getProcessedFeatures().isPresent()) {
                String error = feature.getUnprocessedFeatures().isPresent() ?
                        "No full shingle in current detection window" : "No data in current detection window";
                AnomalyResult anomalyResult = new AnomalyResult(
                        adTask.getDetectorId(),
                        taskId,
                        Double.NaN,
                        Double.NaN,
                        Double.NaN,
                        featureData,
                        Instant.ofEpochMilli(intervalEndTime - interval),
                        Instant.ofEpochMilli(intervalEndTime),
                        executeStartTime,
                        Instant.now(),
                        error,
                        null,
                        adTask.getDetector().getUser(),
                        anomalyDetectionIndices.getSchemaVersion(ADIndex.RESULT)
                );
                anomalyResults.add(anomalyResult);
            } else {
                double[] point = feature.getProcessedFeatures().get();
                double score = rcf.getAnomalyScore(point);
                rcf.update(point);
                double grade = 0d;
                double confidence = 0d;
                if (!thresholdTrained && thresholdTrainingScores.size() < THRESHOLD_MODEL_TRAINING_SIZE) {
                    if (score > 0) {
                        thresholdTrainingScores.add(score);
                    }
                } else {
                    if (!thresholdTrained && thresholdTrainingScores.size() >= THRESHOLD_MODEL_TRAINING_SIZE) {
                        double[] doubles = thresholdTrainingScores.stream().mapToDouble(d -> d).toArray();
                        logger.debug("training threshold model with {} data points", thresholdTrainingScores.size());
                        threshold.train(doubles);
                        thresholdTrained = true;
                        adBatchTaskCache.setThresholdModelTrained(taskId, thresholdTrained);
                    }
                    grade = threshold.grade(score);
                    confidence = threshold.confidence();
                    if (score > 0) {
                        threshold.update(score);
                    }
                }

                AnomalyResult anomalyResult = new AnomalyResult(
                        adTask.getDetectorId(),
                        taskId,
                        score,
                        grade,
                        confidence,
                        featureData,
                        Instant.ofEpochMilli(intervalEndTime - interval),
                        Instant.ofEpochMilli(intervalEndTime),
                        executeStartTime,
                        Instant.now(),
                        null,
                        null,
                        adTask.getDetector().getUser(),
                        anomalyDetectionIndices.getSchemaVersion(ADIndex.RESULT)
                );
                anomalyResults.add(anomalyResult);
            }
        }

        anomalyResultBulkIndexHandler
                .bulkIndexAnomalyResult(
                        anomalyResults,
                        new ThreadedActionListener<>(logger, threadPool, AD_BATCH_TASK_THREAD_POOL_NAME,
                                ActionListener
                                        .wrap(response -> {
                                            try {
                                                runNextPiece(adTask, pieceEndTime, dataStartTime, dataEndTime, interval, listener);
                                            } catch (Exception e) {
                                                listener.onFailure(e);
                                            }
                                        }, exception -> {
                                            // log error message and state
                                            logger.error("Fail to bulk index anomaly result", exception);
                                            throw new AnomalyDetectionException("Fail to bulk index anomaly result", exception);
                                        }), false)

                );
    }

    private void runNextPiece(
        ADTask adTask,
        long pieceStartTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        ActionListener<ADBatchAnomalyResultResponse> listener
    ) {
        String taskId = adTask.getTaskId();
        float initProgress = calculateInitProgress(taskId);
        String taskState = initProgress >= 1.0f ? ADTaskState.RUNNING.name() : ADTaskState.INIT.name();

        if (pieceStartTime < dataEndTime) {
            checkCircuitBreaker(adTask, listener);
            //check running task exceeds limitation or not for every piece,
            // so we can end extra task in case any race condition
            adBatchTaskCache.checkLimitation();
            long expectedPieceEndTime = pieceStartTime + pieceSize * interval;
            long pieceEndTime = expectedPieceEndTime > dataEndTime ? dataEndTime : expectedPieceEndTime;
            int i = 0;
            while (i < pieceIntervalSeconds) {
                checkIfADTaskCancelled(taskId);
                rateLimiter.acquire(1);
                i++;
            }
            logger.info("start next piece start from {} to {}, interval {}", pieceStartTime, pieceEndTime, interval);
            float taskProgress = (float) (pieceStartTime - dataStartTime) / (dataEndTime - dataStartTime);
            adTaskManager
                    .updateADTask(
                            taskId,
                            ImmutableMap
                                    .of(
                                            STATE_FIELD,
                                            taskState,
                                            CURRENT_PIECE_FIELD,
                                            pieceStartTime,
                                            TASK_PROGRESS_FIELD,
                                            taskProgress,
                                            INIT_PROGRESS_FIELD,
                                            initProgress
                                    ),
                            ActionListener
                                    .wrap(
                                            r -> getFeatureData(
                                                    adTask,
                                                    pieceStartTime,
                                                    pieceEndTime,
                                                    dataStartTime,
                                                    dataEndTime,
                                                    interval,
                                                    Instant.now(),
                                                    listener
                                            ),
                                            e -> listener.onFailure(e)
                                    )
                    );
        } else {
            logger.info("all pieces finished for task {}, detector {}", taskId, adTask.getDetectorId());
            adBatchTaskCache.remove(taskId);
            adTaskManager
                    .updateADTask(
                            taskId,
                            ImmutableMap.of(
                                    STATE_FIELD,
                                    ADTaskState.FINISHED.name(),
                                    CURRENT_PIECE_FIELD,
                                    dataEndTime,
                                    TASK_PROGRESS_FIELD,
                                    1.0f,
                                    EXECUTION_END_TIME_FIELD,
                                    Instant.now().toEpochMilli(),
                                    INIT_PROGRESS_FIELD,
                                    initProgress),
                            ActionListener.wrap(r -> {
                                ADBatchAnomalyResultResponse res = new ADBatchAnomalyResultResponse("task execution done");
                                listener.onResponse(res);
                            }, e -> listener.onFailure(e))
                    );
        }
    }

    private float calculateInitProgress(String taskId) {
        RandomCutForest rcf = adBatchTaskCache.getRcfModel(taskId);
        if (rcf == null) {
            return 0.0f;
        }
        float initProgress = (float)rcf.getTotalUpdates() / NUM_MIN_SAMPLES;
        logger.info("RCF total updates: {}, init progress: {}", rcf.getTotalUpdates(), initProgress);
        return initProgress > 1.0f ? 1.0f : initProgress;
    }

    /**
     * Since we need to read from customer index and write to anomaly result index,
     * we need to make sure we can read and write.
     *
     * @param state Cluster state
     * @return whether we have global block or not
     */
    private boolean checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ) != null
            || state.blocks().globalBlockedException(ClusterBlockLevel.WRITE) != null;
    }

    /**
     * Similar to checkGlobalBlock, we check block on the indices level.
     *
     * @param state   Cluster state
     * @param level   block level
     * @param indices the indices on which to check block
     * @return whether any of the index has block on the level.
     */
    private boolean checkIndicesBlocked(ClusterState state, ClusterBlockLevel level, String... indices) {
        // the original index might be an index expression with wildcards like "log*",
        // so we need to expand the expression to concrete index name
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), indices);

        return state.blocks().indicesBlockedException(level, concreteIndices) != null;
    }

    /**
     * Check if we should start anomaly prediction.
     *
     * @param listener listener to respond back to AnomalyResultRequest.
     * @param adTask anomaly detection task
     * @return if we can start anomaly prediction.
     */
    private boolean shouldStart(ActionListener<ADBatchAnomalyResultResponse> listener, ADTask adTask) {
        ClusterState state = clusterService.state();
        String detectorId = adTask.getDetectorId();
        if (checkGlobalBlock(state)) {
            listener.onFailure(new InternalFailure(detectorId, "Cannot read/write due to global block."));
            return false;
        }

        if (checkIndicesBlocked(state, ClusterBlockLevel.READ, adTask.getDetector().getIndices().toArray(new String[0]))) {
            listener.onFailure(new InternalFailure(detectorId, "Cannot read user index due to read block."));
            return false;
        }

        return true;
    }

//    private void checkIfADTaskCancelled(String taskId) {
//        ADTranspoertTask adTranspoertTask = adBatchTaskCache.getAdTransportTask(taskId);
//        if (adTranspoertTask != null && adTranspoertTask.isCancelled()) {
//            adBatchTaskCache.remove(taskId);
//            throw new TaskCancelledException("cancelled");
//        }
//    }

    private void checkIfADTaskCancelled(String taskId) {
        if (adBatchTaskCache.contains(taskId) && adBatchTaskCache.isCancelled(taskId)) {
            throw new ADTaskCancelledException(adBatchTaskCache.getCancelReason(taskId), adBatchTaskCache.getCancelledBy(taskId));
        }
    }

}
