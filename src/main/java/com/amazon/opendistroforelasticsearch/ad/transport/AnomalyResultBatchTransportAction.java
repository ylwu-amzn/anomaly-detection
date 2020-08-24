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

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_TREES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.TIME_DECAY;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.amazon.opendistroforelasticsearch.ad.transport.handler.AnomalyResultBulkIndexHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ClientException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.feature.SinglePointFeatures;
import com.amazon.opendistroforelasticsearch.ad.ml.HybridThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.AnomalyIndexHandler;
import com.amazon.randomcutforest.RandomCutForest;
import com.google.common.util.concurrent.RateLimiter;

public class AnomalyResultBatchTransportAction extends HandledTransportAction<ActionRequest, AnomalyResultBatchResponse> {

    // TODO: unify the logger name, we have LOG, log, logger currently
    private static final Logger LOG = LogManager.getLogger(AnomalyResultBatchTransportAction.class);
    static final String NODE_UNRESPONSIVE_ERR_MSG = "Model node is unresponsive.  Mute model";
    static final String READ_WRITE_BLOCKED = "Cannot read/write due to global block.";
    static final String INDEX_READ_BLOCKED = "Cannot read user index due to read block.";
    private final RateLimiter rateLimiter = RateLimiter.create(1);

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
    private final Map<String, List<Double>> taskTrainingDataMap;
    private final Map<String, AnomalyDetectionBatchTask> taskMap;
    private final AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler;
    // TODO: make the dynamic setting
    private final int PIECES_PER_MINUTE = 10;
    private final Integer PIECE_SIZE = 1000;
    private final Integer THRESHOLD_MODEL_TRAINING_SIZE = 1000;
    private final Integer SHINGLE_SIZE = 8;
    private final TransportStateManager stateManager;
    // private final ThreadPool threadPool;

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
        TransportStateManager manager
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
        taskRcfMap = new ConcurrentHashMap<>();
        taskThresholdModelMap = new ConcurrentHashMap<>();
        this.anomalyResultBulkIndexHandler = anomalyResultBulkIndexHandler;
        taskThresholdModelTrainedMap = new ConcurrentHashMap<>();
        taskTrainingDataMap = new ConcurrentHashMap<>();
        taskMap = new ConcurrentHashMap<>();
        // this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, ActionRequest actionRequest, final ActionListener<AnomalyResultBatchResponse> listener) {

        AnomalyDetectionBatchTask batchTask = (AnomalyDetectionBatchTask) task;
        LOG.info("Task cancellable: {}", batchTask.isCancelled());

        // TODO: add circuit breaker
        AnomalyResultBatchRequest request = AnomalyResultBatchRequest.fromActionRequest(actionRequest);
        ActionListener<AnomalyResultBatchResponse> original = listener;
        // listener = ActionListener.wrap(original::onResponse, e -> {
        // //TODO: add task failure metrics
        //// adStats.getStat(StatNames.AD_EXECUTE_BATCH_FAIL_COUNT.getName()).increment();
        // original.onFailure(e);
        // });

        String detectorId = request.getDetectorId();
        String taskId = request.getTaskExecutionId();

        taskMap.put(taskId, batchTask);
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new EndRunException(detectorId, CommonErrorMessages.DISABLED_ERR_MSG, true);
        }
        try {
            checkTaskCancelled(taskId);
            stateManager.getAnomalyDetector(detectorId, onGetDetector(listener, detectorId, taskId, request));
            // threadPool.executor(AD_BATCh_TASK_THREAD_POOL_NAME).execute(
            // () -> stateManager.getAnomalyDetector(detectorId, onGetDetector(listener, detectorId, taskId, request)));

        } catch (Exception ex) {
            handleExecuteException(ex, listener, detectorId);
        }
    }

    private void checkTaskCancelled(String taskExecutionId) {
        if (taskMap.containsKey(taskExecutionId) && taskMap.get(taskExecutionId).isCancelled()) {
            taskMap.remove(taskExecutionId);
            throw new TaskCancelledException("cancelled");
        }
    }

    private ActionListener<Optional<AnomalyDetector>> onGetDetector(
        ActionListener<AnomalyResultBatchResponse> listener,
        String adID,
        String taskId,
        AnomalyResultBatchRequest request
    ) {
        return ActionListener.wrap(detector -> {
            if (!detector.isPresent()) {
                listener.onFailure(new EndRunException(adID, "AnomalyDetector is not available.", true));
                return;
            }
            AnomalyDetector anomalyDetector = detector.get();

            String thresholdModelID = modelManager.getThresholdModelId(adID);
            Optional<DiscoveryNode> asThresholdNode = hashRing.getOwningNode(thresholdModelID);
            if (!asThresholdNode.isPresent()) {
                listener.onFailure(new InternalFailure(adID, "Threshold model node is not available."));
                return;
            }

            DiscoveryNode thresholdNode = asThresholdNode.get();

            if (!shouldStart(listener, adID, anomalyDetector, thresholdNode.getId(), thresholdModelID)) {
                return;
            }

            // long delayMillis = Optional
            // .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
            // .map(t -> t.toDuration().toMillis())
            // .orElse(0L);
            long dataStartTime = request.getStart();
            long dataEndTime = request.getEnd();

            // Step2. get feature data
            long interval = ((IntervalTimeConfiguration) anomalyDetector.getDetectionInterval()).toDuration().toMillis();
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
            getFeatureData(listener, taskId, anomalyDetector, dataStartTime, timeStamp, dataEndTime, interval, Instant.now());

        }, exception -> handleExecuteException(exception, listener, adID));
    }

    private void getFeatureData(
        ActionListener<AnomalyResultBatchResponse> listener,
        String taskId,
        AnomalyDetector detector,
        long dataStartTime,
        long timeStamp,
        long dataEndTime,
        long interval,
        Instant executeStartTime
    ) {
        checkTaskCancelled(taskId);
        featureManager
            .getFeatures(
                detector,
                dataStartTime,
                timeStamp,
                onFeatureResponseLocalRCF(taskId, detector, listener, timeStamp, dataEndTime, interval, executeStartTime)
            );
    }

    private ActionListener<List<SinglePointFeatures>> onFeatureResponseLocalRCF(
        String taskId,
        AnomalyDetector detector,
        ActionListener<AnomalyResultBatchResponse> listener,
        long timeStamp,
        long dataEndTime,
        long interval,
        Instant executeStartTime
    ) {
        return ActionListener.wrap(featureList -> {
            if (featureList.size() == 0) {
                LOG.error("No data in current window.");
                runNextPiece(detector, taskId, timeStamp, dataEndTime, interval, listener);
            } else if (featureList.size() <= SHINGLE_SIZE) { // TODO: change to shingle_size * 85% , add interpolation
                LOG.error("No full shingle in current detection window");
                runNextPiece(detector, taskId, timeStamp, dataEndTime, interval, listener);
            } else {
                getScoreFromRCF(
                    detector,
                    taskId,
                    detector.getEnabledFeatureIds().size(),
                    featureList,
                    timeStamp,
                    dataEndTime,
                    interval,
                    executeStartTime,
                    listener
                );
            }
        },
            exception -> {
                // TODO: error handling
                LOG.error("Fail to execute onFeatureResponseLocalRCF", exception);
            }
        );
    }

    private void getScoreFromRCF(
        AnomalyDetector detector,
        String taskId,
        int enabledFeatureSize,
        List<SinglePointFeatures> featureList,
        long timeStamp,
        long dataEndTime,
        long interval,
        Instant executeStartTime,
        ActionListener<AnomalyResultBatchResponse> listener
    ) {
        if (!taskRcfMap.containsKey(taskId)) {
            LOG.info("Create new RCF model for task {}", taskId);
            RandomCutForest rcf = RandomCutForest
                .builder()
                .dimensions(SHINGLE_SIZE * enabledFeatureSize)
                .sampleSize(NUM_SAMPLES_PER_TREE)
                .numberOfTrees(NUM_TREES)
                .lambda(TIME_DECAY)
                .outputAfter(NUM_SAMPLES_PER_TREE)
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
            LOG.info("threshold model not trained yet");
            taskThresholdModelTrainedMap.put(taskId, false);
        } else {
            LOG.info("threshold model already trained");
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
                    LOG.info("training threshold model with {} data points", thresholdTrainingScores.size());
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

            List<String> enabledFeatureIds = detector.getEnabledFeatureIds();
            List<String> enabledFeatureNames = detector.getEnabledFeatureNames();
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
                detector.getDetectorId(),
                taskId,
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
                            // log current executing interval
                            // LOG.info("Bulk index took: {}", response.getIngestTookInMillis());
                            //
                            runNextPiece(detector, taskId, timeStamp, dataEndTime, interval, listener);
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
        AnomalyDetector detector,
        String taskId,
        long timeStamp,
        long dataEndTime,
        long interval,
        ActionListener<AnomalyResultBatchResponse> listener
    ) {
        if (timeStamp < dataEndTime) {
            long endTimeStamp = timeStamp + (PIECE_SIZE - SHINGLE_SIZE + 1) * interval > dataEndTime
                ? dataEndTime
                : timeStamp + (PIECE_SIZE - SHINGLE_SIZE + 1) * interval;
            rateLimiter.acquire(60 / PIECES_PER_MINUTE);
            LOG.info("start next piece start from {} to {}, interval {}", timeStamp, endTimeStamp, interval);
            getFeatureData(
                listener,
                taskId,
                detector,
                timeStamp - (SHINGLE_SIZE - 1) * interval,
                endTimeStamp,
                dataEndTime,
                interval,
                Instant.now()
            );
        } else {
            LOG.info("all pieces finished for task {}, detector {}", taskId, detector.getDetectorId());
            AnomalyResultBatchResponse res = new AnomalyResultBatchResponse("task execution done");
            taskThresholdModelMap.remove(taskId);
            taskRcfMap.remove(taskId);
            taskThresholdModelTrainedMap.remove(taskId);
            listener.onResponse(res);
        }
    }

    void handleExecuteException(Exception ex, ActionListener<AnomalyResultBatchResponse> listener, String adID) {
        if (ex instanceof ClientException) {
            listener.onFailure(ex);
        } else if (ex instanceof AnomalyDetectionException) {
            listener.onFailure(new InternalFailure((AnomalyDetectionException) ex));
        } else {
            Throwable cause = ExceptionsHelper.unwrapCause(ex);
            listener.onFailure(new InternalFailure(adID, cause));
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
     * @param adID     detector ID
     * @param detector detector instance corresponds to adID
     * @param thresholdNodeId the threshold model hosting node ID for adID
     * @param thresholdModelID the threshold model ID for adID
     * @return if we can start anomaly prediction.
     */
    private boolean shouldStart(
        ActionListener<AnomalyResultBatchResponse> listener,
        String adID,
        AnomalyDetector detector,
        String thresholdNodeId,
        String thresholdModelID
    ) {
        ClusterState state = clusterService.state();
        if (checkGlobalBlock(state)) {
            listener.onFailure(new InternalFailure(adID, READ_WRITE_BLOCKED));
            return false;
        }

        if (stateManager.isMuted(thresholdNodeId)) {
            listener.onFailure(new InternalFailure(adID, String.format(NODE_UNRESPONSIVE_ERR_MSG + " %s", thresholdModelID)));
            return false;
        }

        if (checkIndicesBlocked(state, ClusterBlockLevel.READ, detector.getIndices().toArray(new String[0]))) {
            listener.onFailure(new InternalFailure(adID, INDEX_READ_BLOCKED));
            return false;
        }

        return true;
    }

}
