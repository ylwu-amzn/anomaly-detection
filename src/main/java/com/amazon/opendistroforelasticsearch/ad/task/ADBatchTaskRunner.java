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
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskState;
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
import static com.amazon.opendistroforelasticsearch.ad.breaker.MemoryCircuitBreaker.DEFAULT_JVM_HEAP_USAGE_THRESHOLD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.CURRENT_PIECE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.EXECUTION_END_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.INIT_PROGRESS_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STATE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.TASK_PROGRESS_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_MIN_SAMPLES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.THRESHOLD_MODEL_TRAINING_SIZE;
import static com.amazon.opendistroforelasticsearch.ad.stats.InternalStatNames.JVM_HEAP_USAGE;
import static com.amazon.opendistroforelasticsearch.ad.stats.StatNames.AD_EXECUTING_BATCH_TASK_COUNT;

public class ADBatchTaskRunner {
    public static final String MIN_DATE = "min_date";
    public static final String MAX_DATE = "max_date";
    private final Logger logger = LogManager.getLogger(ADBatchTaskRunner.class);
    // TODO: tune threshold model training size
    // private final Integer THRESHOLD_MODEL_TRAINING_SIZE = 1000;

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

    private final ADTaskCacheManager adTaskCacheManager;
    private final TransportRequestOptions option; // TODO, test this config

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
        ADTaskCacheManager adTaskCacheManager
    ) {
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

        this.adTaskCacheManager = adTaskCacheManager;

        this.maxAdBatchTaskPerNode = MAX_BATCH_TASK_PER_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_BATCH_TASK_PER_NODE, it -> maxAdBatchTaskPerNode = it);

        this.pieceSize = BATCH_TASK_PIECE_SIZE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCH_TASK_PIECE_SIZE, it -> pieceSize = it);

        this.pieceIntervalSeconds = BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCH_TASK_PIECE_INTERVAL_SECONDS, it -> pieceIntervalSeconds = it);
    }

    public void run(ADTask adTask, Task task, TransportService transportService, ActionListener<ADBatchAnomalyResultResponse> listener) {
        Map<String, Object> updatedFields = new HashMap<>();
        updatedFields.put(STATE_FIELD, ADTaskState.INIT.name());
        updatedFields.put(INIT_PROGRESS_FIELD, 0.0f);
        adTaskManager.updateADTask(adTask.getTaskId(), updatedFields, ActionListener.wrap(r -> {
            getNodeStats(adTask, ActionListener.wrap(node -> {
                if (clusterService.localNode().getId().equals(node.getId())) {
                    // Execute batch task locally
                    logger
                        .info(
                            "execute task {} locally on node {} for detector {}",
                            adTask.getTaskId(),
                            node.getId(),
                            adTask.getDetectorId()
                        );
                    startADBatchTask(adTask, false, listener);
                } else {
                    // Execute batch task remotely
                    logger
                        .info(
                            "execute task {} remotely on node {} for detector {}",
                            adTask.getTaskId(),
                            node.getId(),
                            adTask.getDetectorId()
                        );
                    transportService.sendRequest(
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
        adStatsRequest.addAll(ImmutableSet.of(AD_EXECUTING_BATCH_TASK_COUNT.getName(), JVM_HEAP_USAGE.getName()));

        client.execute(ADStatsNodesAction.INSTANCE, adStatsRequest, ActionListener.wrap(adStatsResponse -> {
            List<ADStatsNodeResponse> candidateNodeResponse = adStatsResponse
                    .getNodes()
                    .stream()
                    .filter(stat -> (long) stat.getStatsMap().get(JVM_HEAP_USAGE.getName()) < DEFAULT_JVM_HEAP_USAGE_THRESHOLD)
                    .collect(Collectors.toList());

            if (candidateNodeResponse.size() == 0) {
                String errorMessage = "All nodes' memory usage exceeds limitation. No eligible node to run detector " + adTask.getDetectorId();
                logger.warn(errorMessage);
                listener.onFailure(new LimitExceededException(adTask.getDetectorId(), errorMessage));
                return;
            }
            candidateNodeResponse = candidateNodeResponse.stream()
                    .filter(stat -> (Long) stat.getStatsMap().get(AD_EXECUTING_BATCH_TASK_COUNT.getName()) < maxAdBatchTaskPerNode)
                    .collect(Collectors.toList());
            if (candidateNodeResponse.size() == 0) {
                String errorMessage = "All nodes' executing historical detector count exceeds limitation. No eligible node to run detector " + adTask.getDetectorId();
                logger.warn(errorMessage);
                listener.onFailure(new LimitExceededException(adTask.getDetectorId(), errorMessage));
                return;
            }
            candidateNodeResponse = candidateNodeResponse.stream()
                .sorted(
                    (ADStatsNodeResponse r1, ADStatsNodeResponse r2) -> ((Long) r1
                        .getStatsMap()
                        .get(AD_EXECUTING_BATCH_TASK_COUNT.getName()))
                            .compareTo((Long) r2.getStatsMap().get(AD_EXECUTING_BATCH_TASK_COUNT.getName()))
                )
                .collect(Collectors.toList());

            if (candidateNodeResponse.size() == 1) {
                listener.onResponse(candidateNodeResponse.get(0).getNode());
            } else {
                Long minTaskCount = (Long) candidateNodeResponse
                        .get(0)
                        .getStatsMap()
                        .get(AD_EXECUTING_BATCH_TASK_COUNT.getName());
                Optional<ADStatsNodeResponse> first = candidateNodeResponse
                        .stream()
                        .filter(c -> minTaskCount.equals(c.getStatsMap().get(AD_EXECUTING_BATCH_TASK_COUNT.getName())))
                        .sorted(
                                (ADStatsNodeResponse r1, ADStatsNodeResponse r2) -> ((Long) r1
                                        .getStatsMap()
                                        .get(JVM_HEAP_USAGE.getName()))
                                        .compareTo((Long) r2.getStatsMap().get(JVM_HEAP_USAGE.getName()))
                        )
                        .findFirst();
                listener.onResponse(first.get().getNode());
            }
        }, exception -> {
            logger.error("Failed to get node's task stats", exception);
            listener.onFailure(exception);
        }));
    }

    public void startADBatchTask(ADTask adTask, boolean runTaskRemotely, ActionListener<ADBatchAnomalyResultResponse> listener) {
        try {
            if (!EnabledSetting.isADPluginEnabled()) {
                throw new EndRunException(adTask.getDetectorId(), CommonErrorMessages.DISABLED_ERR_MSG, true);
            }
            threadPool.executor(AD_BATCH_TASK_THREAD_POOL_NAME).execute(() -> {
                try {
                    // executeADBatchTask(adTask, task, listener);
                    executeADBatchTask(adTask);
                } catch (Exception e) {
                    listener.onFailure(e);
                    adTaskManager.handleADTaskException(adTask, e);
                }
            });
            listener.onResponse(new ADBatchAnomalyResultResponse(clusterService.localNode().getId(), runTaskRemotely));
        } catch (Exception e) {
            logger.error("Fail to start AD batch task " + adTask.getTaskId(), e);
            listener.onFailure(e);
        }
    }

    private void executeADBatchTask(ADTask adTask) {
        ActionListener<String> listener = internalBatchTaskListener(adTask);

        // track AD executing batch task and total batch task execution count
        adStats.getStat(AD_EXECUTING_BATCH_TASK_COUNT.getName()).increment();
        adStats.getStat(StatNames.AD_TOTAL_BATCH_TASK_EXECUTION_COUNT.getName()).increment();

        // put AD task into cache
        adTaskCacheManager.put(adTask);

        // check if circuit breaker is open
        checkCircuitBreaker(adTask, listener);

        // start to run first piece
        Instant executeStartTime = Instant.now();
        runFirstPiece(adTask, executeStartTime, listener);
    }

    private ActionListener<String> internalBatchTaskListener(ADTask adTask) {
        String taskId = adTask.getTaskId();
        ActionListener<String> listener = ActionListener.wrap(response -> {
            // If batch task finished normally, remove task from cache and decrease executing task count by 1.
            adTaskCacheManager.remove(taskId);
            adStats.getStat(AD_EXECUTING_BATCH_TASK_COUNT.getName()).decrement();
        }, e -> {
            // If batch task failed, remove task from cache and decrease executing task count by 1.
            adTaskCacheManager.remove(taskId);
            adStats.getStat(AD_EXECUTING_BATCH_TASK_COUNT.getName()).decrement();

            // Check if batch task was cancelled or not by exception type.
            // If it's cancelled, then increase cancelled task count by 1, otherwise increase failure count by 1.
            if (e instanceof ADTaskCancelledException) {
                adStats.getStat(StatNames.AD_CANCELED_BATCH_TASK_COUNT.getName()).increment();
            } else if (ExceptionUtil.isServerError(e)) {
                adStats.getStat(StatNames.AD_BATCH_TASK_FAILURE_COUNT.getName()).increment();
            }
            // Handle AD task exception
            adTaskManager.handleADTaskException(adTask, e);
        });
        return listener;
    }


    private void checkCircuitBreaker(ADTask adTask, ActionListener<String> listener) {
        String taskId = adTask.getTaskId();
        if (adCircuitBreakerService.isOpen()) {
            String error = "Circuit breaker is open";
            logger.error("AD task: {}, {}", taskId, error);
            throw new LimitExceededException(adTask.getDetectorId(), error, true);
        }
    }

    private void runFirstPiece(ADTask adTask, Instant executeStartTime, ActionListener<String> listener) {
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
                                    listener.onFailure(new IllegalArgumentException("There is no enough data to train model"));
                                    return;
                                }
                                DetectionDateRange detectionDateRange = adTask.getDetector().getDetectionDateRange();
                                long dataStartTime = detectionDateRange.getStartTime().toEpochMilli();
                                long dataEndTime = detectionDateRange.getEndTime().toEpochMilli();

                                if (minDate >= dataEndTime) {
                                    listener
                                        .onFailure(
                                            new ResourceNotFoundException(
                                                adTask.getDetectorId(),
                                                "There is no data in the detection date range"
                                            )
                                        );
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
                                        "start first piece from {} to {}, interval {}, dataStartTime {}, dataEndTime {}," +
                                                " detectorId {}, taskId {}",
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
                    }, e -> { listener.onFailure(e); })
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

        client.search(request, ActionListener.wrap(r -> {
            InternalMin minAgg = r.getAggregations().get(MIN_DATE);
            InternalMax maxAgg = r.getAggregations().get(MAX_DATE);
            double minValue = minAgg.getValue();
            double maxValue = maxAgg.getValue();
            // If time field not exist or there is no value, will return infinity value
            if (minValue == Double.POSITIVE_INFINITY) {
                listener.onFailure(new ResourceNotFoundException(adTask.getDetectorId(), "There is no data in the time field"));
                return;
            }
            consumer.accept((long) minValue, (long) maxValue);
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
        ActionListener<String> listener
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
        ThreadedActionListener threadedActionListener = new ThreadedActionListener<>(
            logger,
            threadPool,
            AD_BATCH_TASK_THREAD_POOL_NAME,
            actionListener,
            false
        );

        featureManager.getFeatureDataPoints(adTask.getDetector(), pieceStartTime, pieceEndTime, threadedActionListener);
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
        ActionListener<String> listener
    ) {
        String taskId = adTask.getTaskId();
        int shingleSize = adTask.getDetector().getShingleSize();
        RandomCutForest rcf = adTaskCacheManager.getRcfModel(taskId);
        ThresholdingModel threshold = adTaskCacheManager.getThresholdModel(taskId);
        double[] thresholdTrainingScores = adTaskCacheManager.getThresholdModelTrainingData(taskId);

        List<AnomalyResult> anomalyResults = new ArrayList<>();

        Deque<Map.Entry<Long, Optional<double[]>>> shingle = adTaskCacheManager.getShingle(taskId);

        boolean thresholdTrained = adTaskCacheManager.isThresholdModelTrained(taskId);
        long intervalEndTime = pieceStartTime;
        for (int i = 0; i < pieceSize && intervalEndTime < dataEndTime; i++) {
            intervalEndTime = intervalEndTime + interval;
            SinglePointFeatures feature = featureManager.getShingledFeature(adTask.getDetector(), shingle, dataPoints, intervalEndTime);
            List<FeatureData> featureData = null;
            if (feature.getUnprocessedFeatures().isPresent()) {
                featureData = ParseUtils.getFeatureData(feature.getUnprocessedFeatures().get(), adTask.getDetector());
            }
            if (!feature.getProcessedFeatures().isPresent()) {
                String error = feature.getUnprocessedFeatures().isPresent()
                    ? "No full shingle in current detection window"
                    : "No data in current detection window";
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
                if (!thresholdTrained && thresholdTrainingScores.length < THRESHOLD_MODEL_TRAINING_SIZE) {
                    if (score > 0) {
                        thresholdTrainingScores[thresholdTrainingScores.length] = score;
                    }
                } else {
                    if (!thresholdTrained && thresholdTrainingScores.length >= THRESHOLD_MODEL_TRAINING_SIZE) {
//                        double[] doubles = thresholdTrainingScores.stream().mapToDouble(d -> d).toArray();
                        logger.debug("training threshold model with {} data points", thresholdTrainingScores.length);
                        threshold.train(thresholdTrainingScores);
                        thresholdTrained = true;
                        adTaskCacheManager.setThresholdModelTrained(taskId, thresholdTrained);
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
                new ThreadedActionListener<>(logger, threadPool, AD_BATCH_TASK_THREAD_POOL_NAME, ActionListener.wrap(response -> {
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
        ActionListener<String> listener
    ) {
        String taskId = adTask.getTaskId();
        float initProgress = calculateInitProgress(taskId);
        String taskState = initProgress >= 1.0f ? ADTaskState.RUNNING.name() : ADTaskState.INIT.name();

        if (pieceStartTime < dataEndTime) {
            checkCircuitBreaker(adTask, listener);
            // check running task exceeds limitation or not for every piece,
            // so we can end extra task in case any race condition
//            adTaskCacheManager.checkLimitation();
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
            adTaskCacheManager.remove(taskId);
            adTaskManager
                .updateADTask(
                    taskId,
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            ADTaskState.FINISHED.name(),
                            CURRENT_PIECE_FIELD,
                            dataEndTime,
                            TASK_PROGRESS_FIELD,
                            1.0f,
                            EXECUTION_END_TIME_FIELD,
                            Instant.now().toEpochMilli(),
                            INIT_PROGRESS_FIELD,
                            initProgress
                        ),
                    ActionListener.wrap(r -> {
                        listener.onResponse("task execution done");
                    }, e -> listener.onFailure(e))
                );
        }
    }

    private float calculateInitProgress(String taskId) {
        RandomCutForest rcf = adTaskCacheManager.getRcfModel(taskId);
        if (rcf == null) {
            return 0.0f;
        }
        float initProgress = (float) rcf.getTotalUpdates() / NUM_MIN_SAMPLES;
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
    private boolean shouldStart(ActionListener<String> listener, ADTask adTask) {
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

    private void checkIfADTaskCancelled(String taskId) {
        if (adTaskCacheManager.contains(taskId) && adTaskCacheManager.isCancelled(taskId)) {
            throw new ADTaskCancelledException(adTaskCacheManager.getCancelReason(taskId), adTaskCacheManager.getCancelledBy(taskId));
        }
    }

}