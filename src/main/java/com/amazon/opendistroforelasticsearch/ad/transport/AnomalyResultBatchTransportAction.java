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

import static com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin.AD_BATCh_TASK_THREAD_POOL_NAME;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTaskExecution.CURRENT_DETECTION_INTERVAL_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTaskExecution.PROGRESS_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTaskExecution.STATE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_TREES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.TIME_DECAY;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ClientException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.feature.SinglePointFeatures;
import com.amazon.opendistroforelasticsearch.ad.ml.HybridThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
import com.amazon.opendistroforelasticsearch.ad.task.AnomalyDetectionTaskManager;
import com.amazon.opendistroforelasticsearch.ad.task.AnomalyDetectionTaskState;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.AnomalyResultBulkIndexHandler;
import com.amazon.randomcutforest.RandomCutForest;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;

public class AnomalyResultBatchTransportAction extends HandledTransportAction<ActionRequest, AnomalyResultBatchResponse> {

    // TODO: unify the logger name, we have LOG, log, logger currently
    private static final Logger LOG = LogManager.getLogger(AnomalyResultBatchTransportAction.class);
    static final String NODE_UNRESPONSIVE_ERR_MSG = "Model node is unresponsive.  Mute model";
    static final String READ_WRITE_BLOCKED = "Cannot read/write due to global block.";
    static final String INDEX_READ_BLOCKED = "Cannot read user index due to read block.";
    // TODO: test performance
    private final RateLimiter rateLimiter = RateLimiter.create(1);
    // TODO: limit how many running tasks

    private final Client client;
    // private final TransportService transportService;
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
    private final Map<String, AnomalyDetectionBatchTask> taskMap;
    private final AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler;
    // TODO: make the dynamic setting
    private final int PIECES_PER_MINUTE = 10;
    private final Integer PIECE_SIZE = 1000;
    private final Integer THRESHOLD_MODEL_TRAINING_SIZE = 1000;
    // private final Integer SHINGLE_SIZE = 8;
    private final TransportStateManager stateManager;
    private final ThreadPool threadPool;
    private final AnomalyDetectionTaskManager anomalyDetectionTaskManager;

    @Inject
    public AnomalyResultBatchTransportAction(
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
        TransportStateManager manager,
        ThreadPool threadPool,
        AnomalyDetectionTaskManager anomalyDetectionTaskManager
    ) {
        super(AnomalyResultBatchAction.NAME, transportService, actionFilters, AnomalyResultBatchRequest::new);
        this.client = client;
        this.stateManager = manager;
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
        this.anomalyDetectionTaskManager = anomalyDetectionTaskManager;
        taskRcfMap = new ConcurrentHashMap<>();
        taskThresholdModelMap = new ConcurrentHashMap<>();
        this.anomalyResultBulkIndexHandler = anomalyResultBulkIndexHandler;
        taskThresholdModelTrainedMap = new ConcurrentHashMap<>();
        taskTrainingDataMap = new ConcurrentHashMap<>();
        taskMap = new ConcurrentHashMap<>();
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, ActionRequest actionRequest, ActionListener<AnomalyResultBatchResponse> actionListener) {
        // TODO: threadId: 60, threadName: elasticsearch[integTest-0][ad-batch-task-threadpool][T#1]
        AnomalyDetectionBatchTask batchTask = (AnomalyDetectionBatchTask) task;
        LOG.info("Task cancellable: {}", batchTask.isCancelled());

        // TODO: add circuit breaker
        AnomalyResultBatchRequest request = AnomalyResultBatchRequest.fromActionRequest(actionRequest);
        ActionListener<AnomalyResultBatchResponse> listener = ActionListener.wrap(actionListener::onResponse, e -> {
            if (e instanceof TaskCancelledException) {
                adStats.getStat(StatNames.AD_CANCEL_TASK_COUNT.getName()).increment();
            } else {
                adStats.getStat(StatNames.AD_EXECUTE_TASK_FAIL_COUNT.getName()).increment();
            }
            actionListener.onFailure(e);
        });

        String taskId = request.getTaskId();
        String taskExecutionId = request.getTaskExecutionId();

        // TODO: only used in AD result.
        Instant executeStartTime = Instant.now();

        anomalyDetectionTaskManager.getTask(taskId, false, ActionListener.wrap(response -> {
            if (response.containsKey(AnomalyDetectionTaskManager.ANOMALY_DETECTION_TASK)) {
                AnomalyDetectionTask anomalyDetectionTask = (AnomalyDetectionTask) response
                    .get(AnomalyDetectionTaskManager.ANOMALY_DETECTION_TASK);
                executeTask(batchTask, request, taskId, taskExecutionId, executeStartTime, anomalyDetectionTask, listener);
            } else {
                listener.onFailure(new ResourceNotFoundException("Can't find task"));
            }
        }, exception -> listener.onFailure(exception)));
    }

    private void executeTask(
        AnomalyDetectionBatchTask batchTask,
        AnomalyResultBatchRequest request,
        String taskId,
        String taskExecutionId,
        Instant executeStartTime,
        AnomalyDetectionTask anomalyDetectionTask,
        ActionListener<AnomalyResultBatchResponse> listener
    ) {
        try {
            if (!EnabledSetting.isADPluginEnabled()) {// TODO check AD plugin enable or not at more places
                throw new EndRunException(CommonErrorMessages.DISABLED_ERR_MSG, true);
            }
            adStats.getStat(StatNames.AD_EXECUTE_TASK_COUNT.getName()).increment();
            anomalyDetectionTaskManager
                .updateTaskExecution(
                    taskExecutionId,
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            AnomalyDetectionTaskState.RUNNING.name(),
                            CURRENT_DETECTION_INTERVAL_FIELD,
                            request.getStart(),
                            PROGRESS_FIELD,
                            0.0f
                        ),
                    ActionListener.wrap(r -> {
                        // TODO: remove task execution from map if error happens
                        taskMap.put(taskExecutionId, batchTask);
                        try {
                            checkTaskCancelled(taskExecutionId);
                            if (!shouldStart(listener, anomalyDetectionTask)) {
                                return;
                            }
                            long dataStartTime = request.getStart();
                            long dataEndTime = request.getEnd();
                            // Step2. get feature data
                            long interval = ((IntervalTimeConfiguration) anomalyDetectionTask.getDetectionInterval())
                                .toDuration()
                                .toMillis();
                            // normalize start time to make it consistent with feature data agg result
                            dataStartTime = dataStartTime - dataStartTime % interval;
                            long timeStamp = dataStartTime + PIECE_SIZE * interval > dataEndTime
                                ? dataEndTime
                                : dataStartTime + PIECE_SIZE * interval;
                            LOG
                                .info(
                                    "start first piece from {} to {}, interval {}, dataStartTime {}, dataEndTime {}",
                                    dataStartTime,
                                    timeStamp,
                                    interval,
                                    dataStartTime,
                                    dataEndTime
                                );
                            getFeatureData(
                                listener,
                                taskId,
                                taskExecutionId,
                                anomalyDetectionTask,
                                request,
                                dataStartTime,
                                timeStamp,
                                dataStartTime,
                                dataEndTime,
                                interval,
                                executeStartTime
                            );
                        } catch (Exception ex) {
                            handleExecuteException(ex, listener);
                        }
                    }, e -> {
                        LOG.error("Fail to update task execution " + taskId, e);
                        listener.onFailure(e);
                    })
                );
        } catch (Exception exception) {
            // TODO: handle exception in task manager, persist error
            LOG.error("Fail to run task " + taskId, exception);
            listener.onFailure(exception);
        }
    }

    private void checkTaskCancelled(String taskExecutionId) {
        if (taskMap.containsKey(taskExecutionId) && taskMap.get(taskExecutionId).isCancelled()) {
            taskMap.remove(taskExecutionId);
            throw new TaskCancelledException("cancelled");
        }
    }

    private void getFeatureData(
        ActionListener<AnomalyResultBatchResponse> listener,
        String taskId,
        String taskExecutionId,
        AnomalyDetectionTask task,
        AnomalyResultBatchRequest request,
        long pieceStartTime,
        long pieceEndTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        Instant executeStartTime
    ) {
        try {
            checkTaskCancelled(taskExecutionId);
            featureManager
                .getFeatures(
                    task,
                    pieceStartTime,
                    pieceEndTime,
                    onFeatureResponseLocalRCF(
                        taskId,
                        taskExecutionId,
                        task,
                        listener,
                        request,
                        pieceStartTime,
                        pieceEndTime,
                        dataStartTime,
                        dataEndTime,
                        interval,
                        executeStartTime
                    )
                );
        } catch (Exception e) {
            handleExecuteException(e, listener);
        }

    }

    private ActionListener<List<SinglePointFeatures>> onFeatureResponseLocalRCF(
        String taskId,
        String taskExecutionId,
        AnomalyDetectionTask task,
        ActionListener<AnomalyResultBatchResponse> listener,
        AnomalyResultBatchRequest request,
        long pieceStartTime,
        long pieceEndTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        Instant executeStartTime
    ) {
        ActionListener<List<SinglePointFeatures>> actionListener = ActionListener.wrap(featureList -> {
            if (featureList.size() == 0) {
                LOG.error("No data in current window.");
                runNextPiece(
                    task,
                    taskId,
                    taskExecutionId,
                    request,
                    pieceStartTime,
                    pieceEndTime,
                    dataStartTime,
                    dataEndTime,
                    interval,
                    listener
                );
            } else if (featureList.size() <= task.getShingleSize()) { // TODO: change to shingle_size * 85% , add interpolation
                LOG.error("No full shingle in current detection window");
                runNextPiece(
                    task,
                    taskId,
                    taskExecutionId,
                    request,
                    pieceStartTime,
                    pieceEndTime,
                    dataStartTime,
                    dataEndTime,
                    interval,
                    listener
                );
            } else {
                getScoreFromRCF(
                    task,
                    taskId,
                    taskExecutionId,
                    task.getEnabledFeatureIds().size(),
                    featureList,
                    request,
                    pieceStartTime,
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
            LOG.error("Fail to execute onFeatureResponseLocalRCF", exception);
            listener.onFailure(exception);
        });
        return new ThreadedActionListener<>(LOG, threadPool, AD_BATCh_TASK_THREAD_POOL_NAME, actionListener, false);
    }

    private void getScoreFromRCF(
        AnomalyDetectionTask task,
        String taskId,
        String taskExecutionId,
        int enabledFeatureSize,
        List<SinglePointFeatures> featureList,
        AnomalyResultBatchRequest request,
        long pieceStartTime,
        long pieceEndTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        Instant executeStartTime,
        ActionListener<AnomalyResultBatchResponse> listener
    ) {
        if (!taskRcfMap.containsKey(taskExecutionId)) {
            LOG.info("Create new RCF model for task {}", taskExecutionId);
            RandomCutForest rcf = RandomCutForest
                .builder()
                .dimensions(task.getShingleSize() * enabledFeatureSize)
                .sampleSize(NUM_SAMPLES_PER_TREE)
                .numberOfTrees(NUM_TREES)
                .lambda(TIME_DECAY)
                .outputAfter(NUM_SAMPLES_PER_TREE)
                .parallelExecutionEnabled(false)
                .build();
            taskRcfMap.putIfAbsent(taskExecutionId, rcf);
        }
        if (!taskThresholdModelMap.containsKey(taskExecutionId)) {
            ThresholdingModel threshold = new HybridThresholdingModel(
                AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
                AnomalyDetectorSettings.THRESHOLD_MAX_RANK_ERROR,
                AnomalyDetectorSettings.THRESHOLD_MAX_SCORE,
                AnomalyDetectorSettings.THRESHOLD_NUM_LOGNORMAL_QUANTILES,
                AnomalyDetectorSettings.THRESHOLD_DOWNSAMPLES,
                AnomalyDetectorSettings.THRESHOLD_MAX_SAMPLES
            );
            taskThresholdModelMap.putIfAbsent(taskExecutionId, threshold);
        }
        RandomCutForest rcf = taskRcfMap.get(taskExecutionId);
        ThresholdingModel threshold = taskThresholdModelMap.get(taskExecutionId);
        if (!taskTrainingDataMap.containsKey(taskExecutionId)) {
            taskTrainingDataMap.put(taskExecutionId, new ArrayList<>());
        }
        List<Double> thresholdTrainingScores = taskTrainingDataMap.get(taskExecutionId);

        boolean thresholdTrained = false;
        if (!taskThresholdModelTrainedMap.containsKey(taskExecutionId)) {
            LOG.info("threshold model not trained yet");
            taskThresholdModelTrainedMap.put(taskExecutionId, false);
        } else {
            LOG.info("threshold model already trained");
            thresholdTrained = taskThresholdModelTrainedMap.get(taskExecutionId);
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
                    LOG.info("training threshold model with {} data points", thresholdTrainingScores.size());
                    threshold.train(doubles);
                    thresholdTrained = true;
                    taskThresholdModelTrainedMap.put(taskExecutionId, true);
                    taskTrainingDataMap.remove(taskExecutionId);
                }
                grade = threshold.grade(score);
                confidence = threshold.confidence();
                if (score > 0) {
                    threshold.update(score);
                }
            }

            List<FeatureData> featureData = new ArrayList<>();

            List<String> enabledFeatureIds = task.getEnabledFeatureIds();
            List<String> enabledFeatureNames = task.getEnabledFeatureNames();
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
                null, // TODO, add task id in AD result?
                taskExecutionId,
                score,
                grade,
                confidence,
                featureData,
                featureList.get(i).getDataStartTime(),
                featureList.get(i).getDataEndTime(),
                executeStartTime,
                Instant.now(),
                null
            );
            anomalyResults.add(anomalyResult);
        }

        anomalyResultBulkIndexHandler
            .bulkIndexAnomalyResult(
                anomalyResults,
                ActionListener
                    .wrap(
                        response -> {
                            runNextPiece(
                                task,
                                taskId,
                                taskExecutionId,
                                request,
                                pieceStartTime,
                                pieceEndTime,
                                dataStartTime,
                                dataEndTime,
                                interval,
                                listener
                            );
                        },
                        exception -> {
                            // log error message and state
                            LOG.error("Fail to bulk index anomaly result", exception);
                            throw new AnomalyDetectionException("Fail to bulk index anomaly result", exception);
                        }
                    )
            );
    }

    private void runNextPiece(
        AnomalyDetectionTask task,
        String taskId,
        String taskExecutionId,
        AnomalyResultBatchRequest request,
        long pieceStartTime,
        long pieceEndTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        ActionListener<AnomalyResultBatchResponse> listener
    ) {
        if (pieceEndTime < dataEndTime) {
            long endTimeStamp = pieceEndTime + (PIECE_SIZE - task.getShingleSize() + 1) * interval > dataEndTime
                ? dataEndTime
                : pieceEndTime + (PIECE_SIZE - task.getShingleSize() + 1) * interval;
            // TODO: add limiter later
            // rateLimiter.acquire(60 / PIECES_PER_MINUTE);
            LOG.info("start next piece start from {} to {}, interval {}", pieceEndTime, endTimeStamp, interval);
            anomalyDetectionTaskManager
                .updateTaskExecution(
                    taskExecutionId,
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            AnomalyDetectionTaskState.RUNNING.name(),
                            CURRENT_DETECTION_INTERVAL_FIELD,
                            pieceEndTime,
                            PROGRESS_FIELD,
                            (float) (pieceEndTime - dataStartTime) / (dataEndTime - dataStartTime)
                        ),
                    ActionListener.wrap(r -> {
                        getFeatureData(
                            listener,
                            taskId,
                            taskExecutionId,
                            task,
                            request,
                            pieceEndTime - (task.getShingleSize() - 1) * interval,
                            endTimeStamp,
                            dataStartTime,
                            dataEndTime,
                            interval,
                            Instant.now()
                        );
                    }, e -> { listener.onFailure(e); })
                );
        } else {
            anomalyDetectionTaskManager
                .updateTaskExecution(
                    taskExecutionId,
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            AnomalyDetectionTaskState.FINISHED.name(),
                            CURRENT_DETECTION_INTERVAL_FIELD,
                            dataEndTime,
                            PROGRESS_FIELD,
                            1.0f
                        ),
                    ActionListener.wrap(r -> {
                        LOG.info("all pieces finished for task {}, execution {}", task.getTaskId(), taskExecutionId);
                        AnomalyResultBatchResponse res = new AnomalyResultBatchResponse("task execution done");
                        taskThresholdModelMap.remove(taskExecutionId);
                        taskRcfMap.remove(taskExecutionId);
                        taskThresholdModelTrainedMap.remove(taskExecutionId);
                        listener.onResponse(res);
                    }, e -> { listener.onFailure(e); })
                );
        }
    }

    void handleExecuteException(Exception ex, ActionListener<AnomalyResultBatchResponse> listener) {
        if (ex instanceof ClientException || ex instanceof TaskCancelledException) {
            listener.onFailure(ex);
        } else if (ex instanceof AnomalyDetectionException) {
            listener.onFailure(new InternalFailure((AnomalyDetectionException) ex));
        } else {
            Throwable cause = ExceptionsHelper.unwrapCause(ex);
            listener.onFailure(new InternalFailure(cause));
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
    private boolean shouldStart(ActionListener<AnomalyResultBatchResponse> listener, AnomalyDetectionTask task) {
        ClusterState state = clusterService.state();
        if (checkGlobalBlock(state)) {
            listener.onFailure(new InternalFailure(READ_WRITE_BLOCKED));
            return false;
        }

        if (checkIndicesBlocked(state, ClusterBlockLevel.READ, task.getIndices().toArray(new String[0]))) {
            listener.onFailure(new InternalFailure(INDEX_READ_BLOCKED));
            return false;
        }

        return true;
    }

    /*private ActionListener<Map<String, Object>> onGetTask(
        String taskId,
        String taskExecutionId,
        AnomalyResultBatchRequest request,
        Instant executeStartTime,
        ActionListener<AnomalyResultBatchResponse> listener
    ) {
        return ActionListener.wrap(response -> {
            if (response.containsKey(ANOMALY_DETECTION_TASK)) {
                AnomalyDetectionTask task = (AnomalyDetectionTask) response.get(ANOMALY_DETECTION_TASK);
                if (!shouldStart(listener, task)) {
                    return;
                }
    
                // long delayMillis = Optional
                // .ofNullable((IntervalTimeConfiguration) task.getWindowDelay())
                // .map(t -> t.toDuration().toMillis())
                // .orElse(0L);
                long dataStartTime = request.getStart();
                long dataEndTime = request.getEnd();
                // Step2. get feature data
                long interval = ((IntervalTimeConfiguration) task.getDetectionInterval()).toDuration().toMillis();
                // normalize start time to make it consistent with feature data agg result
                dataStartTime = dataStartTime - dataStartTime % interval;
                long timeStamp = dataStartTime + 1000 * interval > dataEndTime ? dataEndTime : dataStartTime + 1000 * interval;
                LOG
                    .info(
                        "start first piece from {} to {}, interval {}, dataStartTime {}, dataEndTime {}",
                        dataStartTime,
                        timeStamp,
                        interval,
                        dataStartTime,
                        dataEndTime
                    );
                getFeatureData(
                    listener,
                    taskId,
                    taskExecutionId,
                    task,
                    request,
                    dataStartTime,
                    timeStamp,
                    dataEndTime,
                    interval,
                    executeStartTime
                );
            }
        }, exception -> listener.onFailure(new EndRunException("Task not found.", true)));
    }*/
}