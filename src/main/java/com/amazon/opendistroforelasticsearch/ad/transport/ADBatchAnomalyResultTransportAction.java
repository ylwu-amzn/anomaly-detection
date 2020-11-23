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
        // taskMap = new ConcurrentHashMap<>();
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, ADBatchAnomalyResultRequest request, ActionListener<ADBatchAnomalyResultResponse> actionListener) {
        adBatchTaskRunner.run(request.getTask(), task, transportService, actionListener);
    }
    // @Override
    // protected void doExecute(Task task, BatchAnomalyResultRequest request, ActionListener<BatchAnomalyResultResponse> actionListener) {
    //// AnomalyDetectionBatchTask batchTask = (AnomalyDetectionBatchTask) task;
    //// BatchAnomalyResultRequest request = BatchAnomalyResultRequest.fromActionRequest(actionRequest);
    // ADTask adTask = request.getTask();
    // String taskId = adTask.getTaskId();
    // AnomalyDetector detector = adTask.getDetector();
    //
    // ActionListener<BatchAnomalyResultResponse> listener = ActionListener.wrap(response -> {
    // adTaskManager.removeADTaskFromCache(taskId);
    // adStats.getStat(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()).decrement();
    // actionListener.onResponse(response);
    // }, e -> {
    // adTaskManager.removeADTaskFromCache(taskId);
    // adStats.getStat(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()).decrement();
    // if (e instanceof TaskCancelledException) {
    // adStats.getStat(StatNames.AD_CANCELED_BATCH_TASK_COUNT.getName()).increment();
    // } else {
    // adStats.getStat(StatNames.AD_BATCH_TASK_FAILURE_COUNT.getName()).increment();
    // }
    // actionListener.onFailure(e);
    // });
    //
    // if (adCircuitBreakerService.isOpen()) {
    // String errorMessage = "Circuit breaker is open. Can't run task.";
    // LOG.error(errorMessage + taskId);
    // adTaskManager
    // .updateADTask(
    // taskId,
    // ImmutableMap.of(STATE_FIELD, ADTaskState.FAILED.name(), ERROR_FIELD, errorMessage),
    // ActionListener.wrap(response -> {
    // BatchAnomalyResultResponse res = new BatchAnomalyResultResponse(errorMessage);
    // listener.onResponse(res);
    // }, exception -> { listener.onFailure(exception); })
    // );
    // return;
    // }
    //
    // String error = adTaskManager.checkLimitation();
    // if (error != null) {
    // listener.onFailure(new LimitExceededException(detector.getDetectorId(), error));
    // return;
    // }
    //
    // Instant executeStartTime = Instant.now();
    // executeADTask(adTask, request, executeStartTime, listener);
    //
    //// adTaskManager.getTask(taskId, false, ActionListener.wrap(response -> {
    //// if (response.containsKey(RestHandlerUtils.ANOMALY_DETECTION_TASK)) {
    //// ADTask anomalyDetectionTask = (ADTask) response.get(RestHandlerUtils.ANOMALY_DETECTION_TASK);
    //// executeTask(batchTask, request, taskId, taskExecutionId, executeStartTime, anomalyDetectionTask, listener);
    //// } else {
    //// listener.onFailure(new ResourceNotFoundException("Can't find task"));
    //// }
    //// }, exception -> listener.onFailure(exception)));
    // }
    //
    // private void executeADTask(
    // ADTask adTask,
    // BatchAnomalyResultRequest request,
    // Instant executeStartTime,
    // ActionListener<BatchAnomalyResultResponse> listener
    // ) {
    // try {
    // if (!EnabledSetting.isADPluginEnabled()) {// TODO check AD plugin enable or not at more places
    // throw new EndRunException(adTask.getDetectorId(), CommonErrorMessages.DISABLED_ERR_MSG, true);
    // }
    // adStats.getStat(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()).increment();
    // adStats.getStat(StatNames.AD_TOTAL_BATCH_TASK_COUNT.getName()).increment();
    // adTaskManager
    // .updateADTask(
    // adTask.getTaskId(),
    // ImmutableMap
    // .of(
    // STATE_FIELD,
    // ADTaskState.RUNNING.name(),
    // CURRENT_PIECE_FIELD,
    // adTask.getDetector().getDetectionDateRange().getStartTime(),
    // PROGRESS_FIELD,
    // 0.0f
    // ),
    // ActionListener.wrap(r -> {
    //// adTaskManager.putTaskExecution(taskExecutionId, batchTask);
    // try {
    // adTaskManager.isADTaskCancelled(adTask.getTaskId());
    // if (!shouldStart(listener, adTask)) {
    // return;
    // }
    // DetectionDateRange detectionDateRange = adTask.getDetector().getDetectionDateRange();
    // long dataStartTime = detectionDateRange.getStartTime().toEpochMilli();
    // long dataEndTime = detectionDateRange.getEndTime().toEpochMilli();
    // // Step2. get feature data
    // long interval = ((IntervalTimeConfiguration) adTask.getDetector().getDetectionInterval())
    // .toDuration()
    // .toMillis();
    // // normalize start time to make it consistent with feature data agg result
    // dataStartTime = dataStartTime - dataStartTime % interval;
    // long timeStamp = dataStartTime + PIECE_SIZE * interval > dataEndTime
    // ? dataEndTime
    // : dataStartTime + PIECE_SIZE * interval;
    // LOG
    // .info(
    // "start first piece from {} to {}, interval {}, dataStartTime {}, dataEndTime {}",
    // dataStartTime,
    // timeStamp,
    // interval,
    // dataStartTime,
    // dataEndTime
    // );
    // getFeatureData(
    // listener,
    // adTask,
    // request,
    // dataStartTime,
    // timeStamp,
    // dataStartTime,
    // dataEndTime,
    // interval,
    // executeStartTime
    // );
    // } catch (Exception ex) {
    // handleExecuteException(adTask.getDetectorId(), ex, listener);
    // }
    // }, e -> {
    // LOG.error("Fail to update task " + adTask.getTaskId(), e);
    // listener.onFailure(e);
    // })
    // );
    // } catch (Exception exception) {
    // // TODO: handle exception in task manager, persist error
    // LOG.error("Fail to run task " + adTask.getTaskId(), exception);
    // listener.onFailure(exception);
    // }
    // }
    //
    // private void getFeatureData(
    // ActionListener<BatchAnomalyResultResponse> listener,
    // ADTask task,
    // BatchAnomalyResultRequest request,
    // long pieceStartTime,
    // long pieceEndTime,
    // long dataStartTime,
    // long dataEndTime,
    // long interval,
    // Instant executeStartTime
    // ) {
    // try {
    // adTaskManager.isADTaskCancelled(task.getTaskId());
    // featureManager
    // .getFeatures(
    // task.getDetector(),
    // pieceStartTime,
    // pieceEndTime,
    // onFeatureResponseLocalRCF(
    // task,
    // listener,
    // request,
    // pieceStartTime,
    // pieceEndTime,
    // dataStartTime,
    // dataEndTime,
    // interval,
    // executeStartTime
    // )
    // );
    // } catch (Exception e) {
    // handleExecuteException(task.getDetectorId(), e, listener);
    // }
    //
    // }
    //
    // private ActionListener<List<SinglePointFeatures>> onFeatureResponseLocalRCF(
    // ADTask task,
    // ActionListener<BatchAnomalyResultResponse> listener,
    // BatchAnomalyResultRequest request,
    // long pieceStartTime,
    // long pieceEndTime,
    // long dataStartTime,
    // long dataEndTime,
    // long interval,
    // Instant executeStartTime
    // ) {
    // ActionListener<List<SinglePointFeatures>> actionListener = ActionListener.wrap(featureList -> {
    // if (featureList.size() == 0) {
    // LOG.error("No data in current window.");
    // runNextPiece(
    // task,
    // request,
    // pieceStartTime,
    // pieceEndTime,
    // dataStartTime,
    // dataEndTime,
    // interval,
    // listener
    // );
    // } else if (featureList.size() <= task.getDetector().getShingleSize()) { // TODO: change to shingle_size * 85% , add interpolation
    // LOG.error("No full shingle in current detection window");
    // runNextPiece(
    // task,
    // request,
    // pieceStartTime,
    // pieceEndTime,
    // dataStartTime,
    // dataEndTime,
    // interval,
    // listener
    // );
    // } else {
    // getScoreFromRCF(
    // task,
    // task.getDetector().getEnabledFeatureIds().size(),
    // featureList,
    // request,
    // pieceStartTime,
    // pieceEndTime,
    // dataStartTime,
    // dataEndTime,
    // interval,
    // executeStartTime,
    // listener
    // );
    // }
    // }, exception -> {
    // // TODO: error handling
    // LOG.error("Fail to execute onFeatureResponseLocalRCF", exception);
    // listener.onFailure(exception);
    // });
    // return new ThreadedActionListener<>(LOG, threadPool, AD_BATCh_TASK_THREAD_POOL_NAME, actionListener, false);
    // }
    //
    // private void getScoreFromRCF(
    // ADTask task,
    // int enabledFeatureSize,
    // List<SinglePointFeatures> featureList,
    // BatchAnomalyResultRequest request,
    // long pieceStartTime,
    // long pieceEndTime,
    // long dataStartTime,
    // long dataEndTime,
    // long interval,
    // Instant executeStartTime,
    // ActionListener<BatchAnomalyResultResponse> listener
    // ) {
    // String taskId = task.getTaskId();
    // if (!taskRcfMap.containsKey(taskId)) {
    // LOG.info("start to create new RCF model for task {}", taskId);
    // RandomCutForest rcf = RandomCutForest
    // .builder()
    // .dimensions(task.getDetector().getShingleSize() * enabledFeatureSize)
    // .sampleSize(NUM_SAMPLES_PER_TREE)
    // .numberOfTrees(NUM_TREES)
    // .lambda(TIME_DECAY)
    // .outputAfter(NUM_SAMPLES_PER_TREE)
    // .parallelExecutionEnabled(false)
    // .build();
    // taskRcfMap.putIfAbsent(taskId, rcf);
    // }
    // if (!taskThresholdModelMap.containsKey(taskId)) {
    // ThresholdingModel threshold = new HybridThresholdingModel(
    // AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
    // AnomalyDetectorSettings.THRESHOLD_MAX_RANK_ERROR,
    // AnomalyDetectorSettings.THRESHOLD_MAX_SCORE,
    // AnomalyDetectorSettings.THRESHOLD_NUM_LOGNORMAL_QUANTILES,
    // AnomalyDetectorSettings.THRESHOLD_DOWNSAMPLES,
    // AnomalyDetectorSettings.THRESHOLD_MAX_SAMPLES
    // );
    // taskThresholdModelMap.putIfAbsent(taskId, threshold);
    // }
    // RandomCutForest rcf = taskRcfMap.get(taskId);
    // ThresholdingModel threshold = taskThresholdModelMap.get(taskId);
    // if (!taskTrainingDataMap.containsKey(taskId)) {
    // taskTrainingDataMap.put(taskId, new ArrayList<>());
    // }
    // List<Double> thresholdTrainingScores = taskTrainingDataMap.get(taskId);
    //
    // boolean thresholdTrained = false;
    // if (!taskThresholdModelTrainedMap.containsKey(taskId)) {
    // LOG.info("threshold model not trained yet");
    // taskThresholdModelTrainedMap.put(taskId, false);
    // } else {
    // LOG.info("threshold model already trained");
    // thresholdTrained = taskThresholdModelTrainedMap.get(taskId);
    // }
    // List<AnomalyResult> anomalyResults = new ArrayList<>();
    //
    // for (int i = 0; i < featureList.size(); i++) {
    // double[] point = featureList.get(i).getProcessedFeatures().get();
    // double score = rcf.getAnomalyScore(point);
    // rcf.update(point);
    // double grade = 0d;
    // double confidence = 0d;
    // if (!thresholdTrained && thresholdTrainingScores.size() < THRESHOLD_MODEL_TRAINING_SIZE) {
    // if (score > 0) {
    // thresholdTrainingScores.add(score);
    // }
    // } else {
    // if (!thresholdTrained && thresholdTrainingScores.size() >= THRESHOLD_MODEL_TRAINING_SIZE) {
    // double[] doubles = thresholdTrainingScores.stream().mapToDouble(d -> d).toArray();
    // LOG.info("training threshold model with {} data points", thresholdTrainingScores.size());
    // threshold.train(doubles);
    // thresholdTrained = true;
    // taskThresholdModelTrainedMap.put(taskId, true);
    // taskTrainingDataMap.remove(taskId);
    // }
    // grade = threshold.grade(score);
    // confidence = threshold.confidence();
    // if (score > 0) {
    // threshold.update(score);
    // }
    // }
    //
    // List<FeatureData> featureData = new ArrayList<>();
    //
    // List<String> enabledFeatureIds = task.getDetector().getEnabledFeatureIds();
    // List<String> enabledFeatureNames = task.getDetector().getEnabledFeatureNames();
    // for (int j = 0; j < enabledFeatureIds.size(); j++) {
    // featureData
    // .add(
    // new FeatureData(
    // enabledFeatureIds.get(j),
    // enabledFeatureNames.get(j),
    // featureList.get(i).getUnprocessedFeatures().get()[j]
    // )
    // );
    // }
    //
    // AnomalyResult anomalyResult = new AnomalyResult(
    // task.getDetectorId(),
    // taskId, // TODO, add task id in AD result?
    // score,
    // grade,
    // confidence,
    // featureData,
    // featureList.get(i).getDataStartTime(),
    // featureList.get(i).getDataEndTime(),
    // executeStartTime,
    // Instant.now(),
    // null,
    // null,
    // null,
    // null
    // );
    // anomalyResults.add(anomalyResult);
    // }
    //
    // anomalyResultBulkIndexHandler
    // .bulkIndexAnomalyResult(
    // anomalyResults,
    // ActionListener
    // .wrap(
    // response -> {
    // runNextPiece(
    // task,
    // request,
    // pieceStartTime,
    // pieceEndTime,
    // dataStartTime,
    // dataEndTime,
    // interval,
    // listener
    // );
    // },
    // exception -> {
    // // log error message and state
    // LOG.error("Fail to bulk index anomaly result", exception);
    // throw new AnomalyDetectionException("Fail to bulk index anomaly result", exception);
    // }
    // )
    // );
    // }
    //
    // private void runNextPiece(
    // ADTask task,
    // BatchAnomalyResultRequest request,
    // long pieceStartTime,
    // long pieceEndTime,
    // long dataStartTime,
    // long dataEndTime,
    // long interval,
    // ActionListener<BatchAnomalyResultResponse> listener
    // ) {
    // String taskId = task.getTaskId();
    // if (pieceEndTime < dataEndTime) {
    // long endTimeStamp = pieceEndTime + (PIECE_SIZE - task.getDetector().getShingleSize() + 1) * interval > dataEndTime
    // ? dataEndTime
    // : pieceEndTime + (PIECE_SIZE - task.getDetector().getShingleSize() + 1) * interval;
    // // TODO: add limiter later
    // rateLimiter.acquire(60 / PIECES_PER_MINUTE);
    // LOG.info("start next piece start from {} to {}, interval {}", pieceEndTime, endTimeStamp, interval);
    // adTaskManager
    // .updateADTask(
    // taskId,
    // ImmutableMap
    // .of(
    // STATE_FIELD,
    // ADTaskState.RUNNING.name(),
    // CURRENT_PIECE_FIELD,
    // pieceEndTime,
    // PROGRESS_FIELD,
    // (float) (pieceEndTime - dataStartTime) / (dataEndTime - dataStartTime)
    // ),
    // ActionListener.wrap(r -> {
    // getFeatureData(
    // listener,
    // task,
    // request,
    // pieceEndTime - (task.getDetector().getShingleSize() - 1) * interval,
    // endTimeStamp,
    // dataStartTime,
    // dataEndTime,
    // interval,
    // Instant.now()
    // );
    // }, e -> { listener.onFailure(e); })
    // );
    // } else {
    // adTaskManager
    // .updateADTask(
    // taskId,
    // ImmutableMap
    // .of(
    // STATE_FIELD,
    // ADTaskState.FINISHED.name(),
    // CURRENT_PIECE_FIELD,
    // dataEndTime,
    // PROGRESS_FIELD,
    // 1.0f
    // ),
    // ActionListener.wrap(r -> {
    // LOG.info("all pieces finished for task {}, detector", taskId, task.getDetectorId());
    // taskThresholdModelMap.remove(taskId);
    // taskRcfMap.remove(taskId);
    // taskThresholdModelTrainedMap.remove(taskId);
    // BatchAnomalyResultResponse res = new BatchAnomalyResultResponse("task execution done");
    // listener.onResponse(res);
    // }, e -> { listener.onFailure(e); })
    // );
    // }
    // }
    //
    // void handleExecuteException(String detectorId, Exception ex, ActionListener<BatchAnomalyResultResponse> listener) {
    // if (ex instanceof ClientException || ex instanceof TaskCancelledException) {
    // listener.onFailure(ex);
    // } else if (ex instanceof AnomalyDetectionException) {
    // listener.onFailure(new InternalFailure((AnomalyDetectionException) ex));
    // } else {
    // Throwable cause = ExceptionsHelper.unwrapCause(ex);
    // listener.onFailure(new InternalFailure(detectorId, cause));
    // }
    // }
    //
    // /**
    // * Since we need to read from customer index and write to anomaly result index,
    // * we need to make sure we can read and write.
    // *
    // * @param state Cluster state
    // * @return whether we have global block or not
    // */
    // private boolean checkGlobalBlock(ClusterState state) {
    // return state.blocks().globalBlockedException(ClusterBlockLevel.READ) != null
    // || state.blocks().globalBlockedException(ClusterBlockLevel.WRITE) != null;
    // }
    //
    // /**
    // * Similar to checkGlobalBlock, we check block on the indices level.
    // *
    // * @param state Cluster state
    // * @param level block level
    // * @param indices the indices on which to check block
    // * @return whether any of the index has block on the level.
    // */
    // private boolean checkIndicesBlocked(ClusterState state, ClusterBlockLevel level, String... indices) {
    // // the original index might be an index expression with wildcards like "log*",
    // // so we need to expand the expression to concrete index name
    // String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), indices);
    //
    // return state.blocks().indicesBlockedException(level, concreteIndices) != null;
    // }
    //
    // /**
    // * Check if we should start anomaly prediction.
    // *
    // * @param listener listener to respond back to AnomalyResultRequest.
    // * @param task anomaly detection task
    // * @return if we can start anomaly prediction.
    // */
    // private boolean shouldStart(ActionListener<BatchAnomalyResultResponse> listener, ADTask task) {
    // ClusterState state = clusterService.state();
    // String detectorId = task.getDetectorId();
    // if (checkGlobalBlock(state)) {
    // listener.onFailure(new InternalFailure(detectorId, READ_WRITE_BLOCKED));
    // return false;
    // }
    //
    // if (checkIndicesBlocked(state, ClusterBlockLevel.READ, task.getDetector().getIndices().toArray(new String[0]))) {
    // listener.onFailure(new InternalFailure(detectorId, INDEX_READ_BLOCKED));
    // return false;
    // }
    //
    // return true;
    // }

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
