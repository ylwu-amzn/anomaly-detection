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

import static com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin.AD_BATCh_TASK_THREAD_POOL_NAME;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.CURRENT_PIECE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.ERROR_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.EXECUTION_END_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.PROGRESS_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STATE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_MIN_SAMPLES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_TREES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.TIME_DECAY;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ClientException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.feature.SinglePointFeatures;
import com.amazon.opendistroforelasticsearch.ad.ml.HybridThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
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
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyDetectionBatchTask;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.AnomalyResultBulkIndexHandler;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.randomcutforest.RandomCutForest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

public class ADBatchTaskRunner {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private static final String TASK_ID_HEADER = "anomaly_detection_task_id";
    private final Integer PIECE_SIZE = 1000;
    private final int PIECES_PER_MINUTE = 10;
    // TODO: tune threshold model training size
    private final Integer THRESHOLD_MODEL_TRAINING_SIZE = 1000;

    // TODO: test performance
    private final RateLimiter rateLimiter = RateLimiter.create(1);
    // TODO: limit how many running tasks

    // private final TransportService transportService;
    private final ThreadPool threadPool;
    private final Client client;
    private final ADStats adStats;
    private final Map<String, AnomalyDetectionBatchTask> adBatchTasks;
    private final DiscoveryNodeFilterer nodeFilter;
    // TODO: limit running tasks
    private final Integer MAX_RUNNING_TASK = 10;
    private final ClusterService clusterService;
    private final FeatureManager featureManager;
    private final ADCircuitBreakerService adCircuitBreakerService;
    private final ADTaskManager adTaskManager;
    private final AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final Map<String, RandomCutForest> taskRcfMap;
    private final Map<String, ThresholdingModel> taskThresholdModelMap;
    private final Map<String, Boolean> taskThresholdModelTrainedMap;
    private final Map<String, List<Double>> taskTrainingDataMap; // TODO, check if this class is singleton or not
    private final TransportRequestOptions option;

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
        ADStats adStats,
        AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler
    ) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
        this.anomalyResultBulkIndexHandler = anomalyResultBulkIndexHandler;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.adBatchTasks = new ConcurrentHashMap<>();
        this.nodeFilter = nodeFilter;
        this.adStats = adStats;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.adTaskManager = adTaskManager;
        this.featureManager = featureManager;

        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings))
            .build();

        this.taskRcfMap = new ConcurrentHashMap<>();
        this.taskThresholdModelMap = new ConcurrentHashMap<>();
        this.taskThresholdModelTrainedMap = new ConcurrentHashMap<>();
        this.taskTrainingDataMap = new ConcurrentHashMap<>();
    }

    public void run(ADTask adTask, Task task, TransportService transportService, ActionListener<ADBatchAnomalyResultResponse> listener) {
        Map<String, Object> updatedFields = new HashMap<>();
        updatedFields.put(STATE_FIELD, ADTaskState.INIT.name());
        adTaskManager.updateADTask(adTask.getTaskId(), updatedFields,

                ActionListener.wrap(r -> {
                    if (r.status() == RestStatus.OK) {
                        getNodeStats(adTask, ActionListener.wrap(node -> {
                            if (clusterService.localNode().getId().equals(node.getId())) {
                                // Execute batch task locally
                                startADBatchTask(adTask, task, listener);
                            } else {
                                // Execute batch task remotely
                                transportService
                                        .sendRequest(
                                                node,
                                                ADBatchTaskRemoteExecutionAction.NAME,
                                                new ADBatchAnomalyResultRequest(adTask),
                                                option,
                                                new ActionListenerResponseHandler<>(listener, ADBatchAnomalyResultResponse::new)
                                        );
                            }
                        }, exception -> handleExecuteException(adTask.getDetectorId(), exception, listener)));
                    } else {
                        logger.warn("Failed to move task {} to INIT state: {}", adTask.getTaskId(), r.status());
                    }
                }, e -> {
                    logger.error("Failed to move task {} to INIT state", adTask.getTaskId());
                    listener.onFailure(e);
                }));
    }

    public void startADBatchTask(ADTask adTask, Task task, ActionListener<ADBatchAnomalyResultResponse> listener) {
        try {
            threadPool.executor(AD_BATCh_TASK_THREAD_POOL_NAME).submit(() -> executeADBatchTask(adTask, task, listener));
        } catch (Exception e) {
            logger.error("Fail to start AD batch adTask " + adTask.getTaskId(), e);
            listener.onFailure(e);
            adTaskManager
                    .handleADTaskException(adTask, e);
//            adTaskManager
//                .updateADTask(
//                    adTask.getTaskId(),
//                    ImmutableMap
//                        .of(
//                            STATE_FIELD,
//                            ADTaskState.FAILED.name(),
//                            EXECUTION_END_TIME_FIELD,
//                            Instant.now(),
//                            ERROR_FIELD,
//                            ExceptionUtils.getFullStackTrace(e)
//                        )
//                );
        }
    }

    private void getNodeStats(ADTask task, ActionListener<DiscoveryNode> listener) {
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        ADStatsRequest adStatsRequest = new ADStatsRequest(dataNodes);
        adStatsRequest.addAll(ImmutableSet.of(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()));

        DiscoveryNode localNode = clusterService.localNode();
        String id = localNode.getId();
        client.execute(ADStatsNodesAction.INSTANCE, adStatsRequest, ActionListener.wrap(adStatsResponse -> {
            List<ADStatsNodeResponse> candidateNodeResponse = adStatsResponse
                .getNodes()
                .stream()
                // .filter(stat -> (short) stat.getStatsMap().get(StatNames.MEMORY_USAGE.name()) < 85 && (long)
                // stat.getStatsMap().get(StatNames.AD_EXECUTING_TASK_COUNT.name()) < 10)
                // TODO: ylwudebug make 3 as dynamic config
                .filter(stat -> (Long) stat.getStatsMap().get(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()) < 3)
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
            if (candidateNodeResponse.size() > 0) {
                listener.onResponse(candidateNodeResponse.get(0).getNode());
                // for (ADStatsNodeResponse r : candidateNodeResponse) {
                // if(!localNode.getId().equals(r.getNode().getId())) {
                // listener.onResponse(r.getNode());
                // break;
                // }
                // }

            } else {
                String errorMessage = "No eligible node to run detector " + task.getDetectorId();
                logger.warn(errorMessage);
                listener.onFailure(new LimitExceededException(task.getDetectorId(), errorMessage));
            }
        }, exception -> {
            logger.error("Failed to get node's task stats", exception);
            listener.onFailure(exception);
        }));
    }

    private void executeADBatchTask(ADTask adTask, Task task, ActionListener<ADBatchAnomalyResultResponse> actionListener) {
        String taskId = adTask.getTaskId();
        AnomalyDetector detector = adTask.getDetector();
        putADTaskInCache(adTask, (AnomalyDetectionBatchTask) task);

        ActionListener<ADBatchAnomalyResultResponse> listener = ActionListener.wrap(response -> {
            removeADTaskFromCache(taskId);
            adStats.getStat(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()).decrement();
            actionListener.onResponse(response);
        }, e -> {
            removeADTaskFromCache(taskId);
            adStats.getStat(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()).decrement();
            if (e instanceof TaskCancelledException) {
                adStats.getStat(StatNames.AD_CANCELED_BATCH_TASK_COUNT.getName()).increment();
            } else {
                adStats.getStat(StatNames.AD_BATCH_TASK_FAILURE_COUNT.getName()).increment();
            }
            actionListener.onFailure(e);
        });

        if (adCircuitBreakerService.isOpen()) {
            String errorMessage = "Circuit breaker is open. Can't run task.";
            logger.error(errorMessage + taskId);
            adTaskManager
                .updateADTask(
                    taskId,
                    ImmutableMap.of(STATE_FIELD, ADTaskState.FAILED.name(), ERROR_FIELD, errorMessage),
                    ActionListener.wrap(response -> {
                        ADBatchAnomalyResultResponse res = new ADBatchAnomalyResultResponse(errorMessage);
                        listener.onResponse(res);
                    }, exception -> { listener.onFailure(exception); })
                );
            return;
        }

        String error = checkLimitation();
        if (error != null) {
            listener.onFailure(new LimitExceededException(detector.getDetectorId(), error));
            return;
        }

        Instant executeStartTime = Instant.now();
        executeADTask(adTask, executeStartTime, listener);
    }

    private void executeADTask(ADTask adTask, Instant executeStartTime, ActionListener<ADBatchAnomalyResultResponse> listener) {
        try {
            if (!EnabledSetting.isADPluginEnabled()) {// TODO check AD plugin enable or not at other places
                throw new EndRunException(adTask.getDetectorId(), CommonErrorMessages.DISABLED_ERR_MSG, true);
            }
            adStats.getStat(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()).increment();
            adStats.getStat(StatNames.AD_TOTAL_BATCH_TASK_COUNT.getName()).increment();
            adTaskManager
                .updateADTask(
                    adTask.getTaskId(),
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            //TODO: move to RUNNING state only RCF model passed initilization.
                            ADTaskState.RUNNING.name(),
                            CURRENT_PIECE_FIELD,
                            adTask.getDetector().getDetectionDateRange().getStartTime(),
                            PROGRESS_FIELD,
                            0.0f
                        ),
                    ActionListener.wrap(r -> {
                        try {
                            checkIfADTaskCancelled(adTask.getTaskId());
                            if (!shouldStart(listener, adTask)) {
                                return;
                            }
                            DetectionDateRange detectionDateRange = adTask.getDetector().getDetectionDateRange();
                            long dataStartTime = detectionDateRange.getStartTime().toEpochMilli();
                            long dataEndTime = detectionDateRange.getEndTime().toEpochMilli();
                            long interval = ((IntervalTimeConfiguration) adTask.getDetector().getDetectionInterval())
                                .toDuration()
                                .toMillis();
                            // normalize start time to make it consistent with feature data agg result
                            dataStartTime = dataStartTime - dataStartTime % interval;
                            long expectedPieceEndTime = dataStartTime + PIECE_SIZE * interval;
                            long firstPieceEndTime = expectedPieceEndTime > dataEndTime ? dataEndTime : expectedPieceEndTime;
                            logger
                                .info(
                                    "start first piece from {} to {}, interval {}, dataStartTime {}, dataEndTime {}",
                                    dataStartTime,
                                    firstPieceEndTime,
                                    interval,
                                    dataStartTime,
                                    dataEndTime
                                );
                            getFeatureData(
                                listener,
                                adTask,
                                dataStartTime, // first piece start time
                                firstPieceEndTime, // first piece end time
                                dataStartTime,
                                dataEndTime,
                                interval,
                                executeStartTime
                            );
                        } catch (Exception ex) {
                            handleExecuteException(adTask.getDetectorId(), ex, listener);
                        }
                    }, e -> {
                        logger.error("Fail to update task " + adTask.getTaskId(), e);
                        listener.onFailure(e);
                    })
                );
        } catch (Exception exception) {
            // TODO: handle exception in task manager, persist error
            logger.error("Fail to run task " + adTask.getTaskId(), exception);
            listener.onFailure(exception);
        }
    }

    private void getFeatureData(
        ActionListener<ADBatchAnomalyResultResponse> listener,
        ADTask task,
        long pieceStartTime,
        long pieceEndTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        Instant executeStartTime
    ) {
        checkIfADTaskCancelled(task.getTaskId());
        featureManager
            .getFeatures(
                task.getDetector(),
                pieceStartTime,
                pieceEndTime,
                onGetFeatureDataResponse(task, listener, pieceEndTime, dataStartTime, dataEndTime, interval, executeStartTime)
            );
    }

    private ActionListener<List<SinglePointFeatures>> onGetFeatureDataResponse(
        ADTask task,
        ActionListener<ADBatchAnomalyResultResponse> listener,
        long pieceEndTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        Instant executeStartTime
    ) {
        ActionListener<List<SinglePointFeatures>> actionListener = ActionListener.wrap(featureList -> {
            if (featureList.size() == 0) {
                logger.error("No data in current window.");
                runNextPiece(task, pieceEndTime, dataStartTime, dataEndTime, interval, listener);
            } else if (featureList.size() <= task.getDetector().getShingleSize()) { // TODO: change to shingle_size * 85% , add
                                                                                    // interpolation
                logger.error("No full shingle in current detection window");
                runNextPiece(task, pieceEndTime, dataStartTime, dataEndTime, interval, listener);
            } else {
                getScoreFromRCF(
                    task,
                    task.getDetector().getEnabledFeatureIds().size(),
                    featureList,
                    pieceEndTime,
                    dataStartTime,
                    dataEndTime,
                    interval,
                    executeStartTime,
                    listener
                );
            }
        }, exception -> {
            // TODO: error handling
            logger.error("Fail to execute onFeatureResponseLocalRCF", exception);
            listener.onFailure(exception);
        });
        return new ThreadedActionListener<>(logger, threadPool, AD_BATCh_TASK_THREAD_POOL_NAME, actionListener, false);
    }

    private void getScoreFromRCF(
        ADTask task,
        int enabledFeatureSize,
        List<SinglePointFeatures> featureList,
        long pieceEndTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        Instant executeStartTime,
        ActionListener<ADBatchAnomalyResultResponse> listener
    ) {
        String taskId = task.getTaskId();
        if (!taskRcfMap.containsKey(taskId)) {
            logger.info("start to create new RCF model for task {}", taskId);
            RandomCutForest rcf = RandomCutForest
                .builder()
                .dimensions(task.getDetector().getShingleSize() * enabledFeatureSize)
                .numberOfTrees(NUM_TREES)
                .lambda(TIME_DECAY)
                .sampleSize(NUM_SAMPLES_PER_TREE)
                .outputAfter(NUM_MIN_SAMPLES)
                .parallelExecutionEnabled(false)
                .build();
            taskRcfMap.putIfAbsent(taskId, rcf);
        }
        if (!taskThresholdModelMap.containsKey(taskId)) {
            ThresholdingModel threshold = new HybridThresholdingModel(
                AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
                AnomalyDetectorSettings.THRESHOLD_MAX_RANK_ERROR,
                AnomalyDetectorSettings.THRESHOLD_MAX_SCORE,
                AnomalyDetectorSettings.THRESHOLD_NUM_LOGNORMAL_QUANTILES,
                AnomalyDetectorSettings.THRESHOLD_DOWNSAMPLES,
                AnomalyDetectorSettings.THRESHOLD_MAX_SAMPLES
            );
            taskThresholdModelMap.putIfAbsent(taskId, threshold);
        }
        RandomCutForest rcf = taskRcfMap.get(taskId);
        ThresholdingModel threshold = taskThresholdModelMap.get(taskId);
        if (!taskTrainingDataMap.containsKey(taskId)) {
            taskTrainingDataMap.put(taskId, new ArrayList<>());
        }
        List<Double> thresholdTrainingScores = taskTrainingDataMap.get(taskId);

        boolean thresholdTrained = false;
        if (!taskThresholdModelTrainedMap.containsKey(taskId)) {
            logger.info("threshold model not trained yet");
            taskThresholdModelTrainedMap.put(taskId, false);
        } else {
            logger.info("threshold model already trained");
            thresholdTrained = taskThresholdModelTrainedMap.get(taskId);
        }
        List<AnomalyResult> anomalyResults = new ArrayList<>();

        for (int i = 0; i < featureList.size(); i++) {
            double[] point = featureList.get(i).getProcessedFeatures().get();
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
                    logger.info("training threshold model with {} data points", thresholdTrainingScores.size());
                    threshold.train(doubles);
                    thresholdTrained = true;
                    taskThresholdModelTrainedMap.put(taskId, true);
                    taskTrainingDataMap.remove(taskId);
                }
                grade = threshold.grade(score);
                confidence = threshold.confidence();
                if (score > 0) {
                    threshold.update(score);
                }
            }

            List<FeatureData> featureData = new ArrayList<>();

            List<String> enabledFeatureIds = task.getDetector().getEnabledFeatureIds();
            List<String> enabledFeatureNames = task.getDetector().getEnabledFeatureNames();
            for (int j = 0; j < enabledFeatureIds.size(); j++) {
                featureData
                    .add(
                        new FeatureData(
                            enabledFeatureIds.get(j),
                            enabledFeatureNames.get(j),
                            featureList.get(i).getUnprocessedFeatures().get()[j]
                        )
                    );
            }

            AnomalyResult anomalyResult = new AnomalyResult(
                task.getDetectorId(),
                taskId, // TODO, add task id in AD result?
                score,
                grade,
                confidence,
                featureData,
                featureList.get(i).getDataStartTime(),
                featureList.get(i).getDataEndTime(),
                executeStartTime,
                Instant.now(),
                null,
                null,
                null,
                null
            );
            anomalyResults.add(anomalyResult);
        }

        anomalyResultBulkIndexHandler
            .bulkIndexAnomalyResult(
                anomalyResults,
                ActionListener
                    .wrap(response -> { runNextPiece(task, pieceEndTime, dataStartTime, dataEndTime, interval, listener); }, exception -> {
                        // log error message and state
                        logger.error("Fail to bulk index anomaly result", exception);
                        throw new AnomalyDetectionException("Fail to bulk index anomaly result", exception);
                    })
            );
    }

    private void runNextPiece(
        ADTask task,
        long previousPieceEndTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        ActionListener<ADBatchAnomalyResultResponse> listener
    ) {
        String taskId = task.getTaskId();
        Integer shingleSize = task.getDetector().getShingleSize();
        if (previousPieceEndTime < dataEndTime) {
            long pieceStartTime = previousPieceEndTime - (shingleSize - 1) * interval;
            long expectedPieceEndTime = previousPieceEndTime + (PIECE_SIZE - shingleSize + 1) * interval;
            long pieceEndTime = expectedPieceEndTime > dataEndTime ? dataEndTime : expectedPieceEndTime;
            // TODO: add limiter later
            rateLimiter.acquire(60 / PIECES_PER_MINUTE);
            logger.info("start next piece start from {} to {}, interval {}", previousPieceEndTime, pieceEndTime, interval);
            adTaskManager
                .updateADTask(
                    taskId,
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            ADTaskState.RUNNING.name(),
                            CURRENT_PIECE_FIELD,
                            previousPieceEndTime,
                            PROGRESS_FIELD,
                            (float) (previousPieceEndTime - dataStartTime) / (dataEndTime - dataStartTime)
                        ),
                    ActionListener
                        .wrap(
                            r -> getFeatureData(
                                listener,
                                task,
                                pieceStartTime,
                                pieceEndTime,
                                dataStartTime,
                                dataEndTime,
                                interval,
                                Instant.now()
                            ),
                            e -> listener.onFailure(e)
                        )
                );
        } else {
            adTaskManager
                .updateADTask(
                    taskId,
                    ImmutableMap.of(STATE_FIELD, ADTaskState.FINISHED.name(), CURRENT_PIECE_FIELD, dataEndTime, PROGRESS_FIELD, 1.0f),
                    ActionListener.wrap(r -> {
                        logger.info("all pieces finished for task {}, detector", taskId, task.getDetectorId());
                        taskThresholdModelMap.remove(taskId);
                        taskRcfMap.remove(taskId);
                        taskThresholdModelTrainedMap.remove(taskId);
                        ADBatchAnomalyResultResponse res = new ADBatchAnomalyResultResponse("task execution done");
                        listener.onResponse(res);
                    }, e -> { listener.onFailure(e); })
                );
        }
    }

    void handleExecuteException(String detectorId, Exception ex, ActionListener<ADBatchAnomalyResultResponse> listener) {
        if (ex instanceof ClientException || ex instanceof TaskCancelledException) {
            listener.onFailure(ex);
        } else if (ex instanceof AnomalyDetectionException) {
            listener.onFailure(new InternalFailure((AnomalyDetectionException) ex));
        } else {
            Throwable cause = ExceptionsHelper.unwrapCause(ex);
            listener.onFailure(new InternalFailure(detectorId, cause));
        }
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
     * @param task anomaly detection task
     * @return if we can start anomaly prediction.
     */
    private boolean shouldStart(ActionListener<ADBatchAnomalyResultResponse> listener, ADTask task) {
        ClusterState state = clusterService.state();
        String detectorId = task.getDetectorId();
        if (checkGlobalBlock(state)) {
            listener.onFailure(new InternalFailure(detectorId, "Cannot read/write due to global block."));
            return false;
        }

        if (checkIndicesBlocked(state, ClusterBlockLevel.READ, task.getDetector().getIndices().toArray(new String[0]))) {
            listener.onFailure(new InternalFailure(detectorId, "Cannot read user index due to read block."));
            return false;
        }

        return true;
    }

    // private ActionListener<ADBatchAnomalyResultResponse> taskExecutionListener(
    // ADTask task
    // ) {
    // return ActionListener.wrap(response -> {
    // logger.info("Task execution finished for {}, response: {}", task.getTaskId(), response.getMessage());
    // // remove task execution from map when finish
    // removeADTaskFromCache(task.getTaskId());
    // }, exception -> {
    // removeADTaskFromCache(task.getTaskId());
    // adTaskManager.handleADTaskException(task, exception); });
    // }

    // private void handleADTaskException(ADTask task, Exception exception) {
    // // remove task execution from map if execution fails
    // removeADTaskFromCache(task.getTaskId());
    // String state = ADTaskState.FAILED.name();
    // Map<String, Object> updatedFields = new HashMap<>();
    // if (exception instanceof TaskCancelledException) {
    // logger.error("AD task cancelled: " + task.getTaskId());
    // state = ADTaskState.STOPPED.name();
    // } else {
    // logger.error("Fail to execute batch task action " + task.getTaskId(), exception);
    // updatedFields.put(ERROR_FIELD, ExceptionUtils.getFullStackTrace(exception));
    // }
    // updatedFields.put(STATE_FIELD, state);
    // updatedFields.put(EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli());
    // adTaskManager.updateADTask(task.getTaskId(), updatedFields);
    // }

    private void putADTaskInCache(ADTask adTask, AnomalyDetectionBatchTask task) {
        adBatchTasks.put(adTask.getTaskId(), task);
    }

    private void removeADTaskFromCache(String taskId) {
        adBatchTasks.remove(taskId);
    }

    private String checkLimitation() {
        if (adBatchTasks.size() >= MAX_RUNNING_TASK) {
            return "Can't run more than " + MAX_RUNNING_TASK + " per node";
        }
        return null;
    }

    private void checkIfADTaskCancelled(String taskId) {
        if (adBatchTasks.containsKey(taskId) && adBatchTasks.get(taskId).isCancelled()) {
            removeADTaskFromCache(taskId);
            throw new TaskCancelledException("cancelled");
        }
    }

}
