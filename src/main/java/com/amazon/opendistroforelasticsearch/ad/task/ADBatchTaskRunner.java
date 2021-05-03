/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin.AD_BATCH_TASK_THREAD_POOL_NAME;
import static com.amazon.opendistroforelasticsearch.ad.breaker.MemoryCircuitBreaker.DEFAULT_JVM_HEAP_USAGE_THRESHOLD;
import static com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages.NO_ELIGIBLE_NODE_TO_RUN_DETECTOR;
import static com.amazon.opendistroforelasticsearch.ad.constant.CommonName.AGG_NAME_MAX_TIME;
import static com.amazon.opendistroforelasticsearch.ad.constant.CommonName.AGG_NAME_MIN_TIME;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.CURRENT_PIECE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.EXECUTION_END_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.INIT_PROGRESS_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STATE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.TASK_PROGRESS_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.WORKER_NODE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_TOP_ENTITIES_PER_HC_DETECTOR;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_MIN_SAMPLES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.THRESHOLD_MODEL_TRAINING_SIZE;
import static com.amazon.opendistroforelasticsearch.ad.stats.InternalStatNames.JVM_HEAP_USAGE;
import static com.amazon.opendistroforelasticsearch.ad.stats.StatNames.AD_EXECUTING_BATCH_TASK_COUNT;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.InternalMin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.caching.PriorityTracker;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ADTaskCancelledException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
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
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskType;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.DetectionDateRange;
import com.amazon.opendistroforelasticsearch.ad.model.Entity;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class ADBatchTaskRunner {
    private final Logger logger = LogManager.getLogger(ADBatchTaskRunner.class);

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
    private final TransportRequestOptions option;

    private volatile Integer maxAdBatchTaskPerNode;
    private volatile Integer pieceSize;
    private volatile Integer pieceIntervalSeconds;
    private volatile Integer maxTopEntitiesPerHcDetector;
    private volatile Integer maxRunningEntitiesPerDetector;

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

        this.maxTopEntitiesPerHcDetector = MAX_TOP_ENTITIES_PER_HC_DETECTOR.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_TOP_ENTITIES_PER_HC_DETECTOR, it -> maxTopEntitiesPerHcDetector = it);

        this.maxRunningEntitiesPerDetector = MAX_RUNNING_ENTITIES_PER_DETECTOR.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_RUNNING_ENTITIES_PER_DETECTOR, it -> maxRunningEntitiesPerDetector = it);

    }

    /**
     * For HC detector, init top entities if not initialized yet.
     * For single entity detector or top entities initialized, just run AD task directly.
     *
     * @param adTask single entity or HC detector task
     * @param transportService transport service
     * @param listener action listener
     */
    public void run(ADTask adTask, TransportService transportService, ActionListener<ADBatchAnomalyResultResponse> listener) {
        boolean isHCDetector = adTask.getDetector().isMultientityDetector();
        if (isHCDetector && !adTaskCacheManager.topEntityInited(adTask.getDetectorId())) {
            // HC detector top entities no initialized
            adTaskCacheManager.add(adTask);

            threadPool.executor(AD_BATCH_TASK_THREAD_POOL_NAME).execute(() -> {
                ActionListener<ADBatchAnomalyResultResponse> hcDelegatedListener = getInternalHCDelegatedListener();
                ActionListener<String> internalHCListener = internalHCListener(adTask, transportService, hcDelegatedListener);
                try {
                    getTopEntities(adTask, internalHCListener);
                } catch (Exception e) {
                    internalHCListener.onFailure(e);
                }
            });
            listener.onResponse(new ADBatchAnomalyResultResponse(clusterService.localNode().getId(), false));
        } else {
            // single entity detector or HC detector which top entities initialized
            runADTask(adTask, transportService, listener);
        }
    }

    private ActionListener<ADBatchAnomalyResultResponse> getInternalHCDelegatedListener() {
        return ActionListener
            .wrap(
                r -> { logger.debug("[InternalHCDelegatedListener]: running task on nodeId {}", r.getNodeId()); },
                e -> { logger.error("[InternalHCDelegatedListener]: failed to run task", e); }
            );
    }

    private ActionListener<String> internalHCListener(
        ADTask adTask,
        TransportService transportService,
        ActionListener<ADBatchAnomalyResultResponse> listener
    ) {
        ActionListener<String> actionListener = ActionListener.wrap(response -> {
            adTaskCacheManager.setTopEntityInited(adTask.getDetectorId());
            int totalEntities = adTaskCacheManager.getPendingEntityCount(adTask.getDetectorId());
            logger.debug("Initialized top entity for detector: {}, total top entities: {}", adTask.getDetectorId(), totalEntities);
            int numberOfEligibleDataNodes = nodeFilter.getNumberOfEligibleDataNodes();
            // TODO: use 1/10 of total maxAdBatchTaskPerNode per HC detector to make sure user can run multiple HC detectors in parallel.
            int maxRunningEntities = Math
                .min(totalEntities, Math.min(numberOfEligibleDataNodes * maxAdBatchTaskPerNode, maxRunningEntitiesPerDetector));
            adTaskCacheManager.setAllowedRunningEntities(adTask.getDetectorId(), maxRunningEntities);
            runADTask(adTask, transportService, listener);
        }, e -> {
            logger.error("Failed to run HC detector " + adTask.getDetectorId(), e);
            if (adTask.getTaskType().equals(ADTaskType.HISTORICAL_HC_DETECTOR.name())) {
                adTaskCacheManager.remove(adTask.getTaskId());
                adTaskManager.entityTaskDone(adTask, e, transportService);
            }
        });
        ThreadedActionListener<String> threadedActionListener = new ThreadedActionListener<>(
            logger,
            threadPool,
            AD_BATCH_TASK_THREAD_POOL_NAME,
            actionListener,
            false
        );
        return threadedActionListener;
    }

    public void getTopEntities(ADTask adTask, ActionListener<String> internalHCListener) {
        getDateRangeOfSourceData(adTask, (dataStartTime, dataEndTime) -> {
            PriorityTracker priorityTracker = new PriorityTracker(
                Clock.systemUTC(),
                adTask.getDetector().getDetectorIntervalInSeconds(),
                adTask.getDetectionDateRange().getStartTime().toEpochMilli(),
                10_000
            );
            long interval = adTask.getDetector().getDetectorIntervalInMilliseconds();
            logger
                .debug(
                    "start to search top entities, data start time: {}, data end time: {}, interval: {}",
                    dataStartTime,
                    dataEndTime,
                    interval
                );
            searchTopEntities(
                adTask,
                priorityTracker,
                dataEndTime,
                Math.max((dataEndTime - dataStartTime) / 1000, interval),
                dataStartTime,
                dataStartTime + interval,
                internalHCListener
            );
        }, internalHCListener);
    }

    private void searchTopEntities(
        ADTask adTask,
        PriorityTracker priorityTracker,
        long detectionEndTime,
        long interval,
        long dataStartTime,
        long dataEndTime,
        ActionListener<String> internalHCListener
    ) {
        checkIfADTaskCancelled(adTask.getTaskId());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(adTask.getDetector().getTimeField())
            .gte(dataStartTime)
            .lte(dataEndTime)
            .format("epoch_millis");
        ;
        boolQueryBuilder.filter(rangeQueryBuilder);
        boolQueryBuilder.filter(adTask.getDetector().getFilterQuery());
        sourceBuilder.query(boolQueryBuilder);

        String topEntitiesAgg = "topEntities";
        AggregationBuilder aggregation = new TermsAggregationBuilder(topEntitiesAgg)
            .field(adTask.getDetector().getCategoryField().get(0))
            .size(1000); // TODO: remove this hard code
        sourceBuilder.aggregation(aggregation).size(0);
        // TODO: add historical date range
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        searchRequest.indices(adTask.getDetector().getIndices().toArray(new String[0]));
        client.search(searchRequest, ActionListener.wrap(r -> {
            StringTerms a = r.getAggregations().get(topEntitiesAgg);
            List<StringTerms.Bucket> buckets = a.getBuckets();
            List<String> topEntities = new ArrayList<>();
            for (StringTerms.Bucket b : buckets) {
                // TODO: fix stats
                String key = b.getKeyAsString();
                topEntities.add(key);
            }

            topEntities.forEach(e -> priorityTracker.updatePriority(e));
            if (dataEndTime < detectionEndTime) {
                searchTopEntities(
                    adTask,
                    priorityTracker,
                    detectionEndTime,
                    interval,
                    dataEndTime,
                    dataEndTime + interval,
                    internalHCListener
                );
            } else {
                adTaskCacheManager.remove(adTask.getTaskId());
                List<String> topNEntities = priorityTracker.getTopNEntities(maxTopEntitiesPerHcDetector);
                adTaskCacheManager.addPendingEntities(adTask.getDetectorId(), topNEntities);
                adTaskCacheManager.setTopEntityCount(adTask.getDetectorId(), topNEntities.size());
                if (adTaskCacheManager.getPendingEntityCount(adTask.getDetectorId()) == 0) {
                    logger.error("There is no entity found for detector " + adTask.getDetectorId());
                    internalHCListener.onFailure(new ResourceNotFoundException(adTask.getDetectorId(), "No entity found"));
                } else {
                    internalHCListener.onResponse("Get top entities done");
                }
            }
        }, e -> {
            logger.error("Failed to get top entities for detector " + adTask.getDetectorId(), e);
            internalHCListener.onFailure(e);
        }));
    }

    /**
     * Run AD task.
     * 1. Set AD task state as {@link ADTaskState#INIT}
     * 2. Gather node stats and find node with least load to run AD task.
     *
     * @param adTask AD task
     * @param transportService transport service
     * @param listener action listener
     */
    public void runADTask(ADTask adTask, TransportService transportService, ActionListener<ADBatchAnomalyResultResponse> listener) {
        Map<String, Object> updatedFields = new HashMap<>();
        updatedFields.put(STATE_FIELD, ADTaskState.INIT.name());
        updatedFields.put(INIT_PROGRESS_FIELD, 0.0f);

        String detectorId = adTask.getDetectorId();
        boolean isHCDetector = adTask.getDetector().isMultientityDetector();
        String entity = isHCDetector ? adTaskCacheManager.pollEntity(detectorId) : null;
        if (isHCDetector) {
            logger.debug("Start to run entity: {}", entity);
            if (entity == null) {
                listener.onResponse(new ADBatchAnomalyResultResponse(clusterService.localNode().getId(), false));
                return;
            }
            ActionListener<Object> wrappedListener = ActionListener.wrap(r -> {}, e -> {
                logger.error("Failed to start entity task for detector: {}, entity: {}", detectorId, entity);
                // If fail, move the entity into pending task cache
                adTaskCacheManager.addPendingEntity(detectorId, entity);
            });
            adTaskManager.getLatestADTask(detectorId, entity, ImmutableList.of(ADTaskType.HISTORICAL_HC_ENTITY), existingEntityTask -> {
                if (existingEntityTask.isPresent()) { // retry failed entity caused by limit exceed exception
                    // TODO: if task failed due to limit exceed exception in half way, resume from the break
                    // point or just clear the old AD tasks and rerun it? Currently we just support rerunning
                    // task failed due to limit exceed exception before starting.
                    ADTask adEntityTask = existingEntityTask.get();
                    logger.info("Rerun entity task: {}", adEntityTask.getTaskId());
                    ActionListener<ADBatchAnomalyResultResponse> delegatedListener = getDelegatedListener(
                        adEntityTask,
                        transportService,
                        listener
                    );
                    executeSingleEntityTask(adEntityTask, transportService, delegatedListener);
                } else {
                    logger.info("Create entity task for entity:{}", entity);
                    Instant now = Instant.now();
                    String parentTaskId = adTask.getTaskType().equals(ADTaskType.HISTORICAL_HC_ENTITY.name())
                        ? adTask.getParentTaskId()
                        : adTask.getTaskId();
                    ADTask adEntityTask = new ADTask.Builder()
                        .detectorId(adTask.getDetectorId())
                        .detector(adTask.getDetector())
                        .isLatest(true)
                        .taskType(ADTaskType.HISTORICAL_HC_ENTITY.name())
                        .executionStartTime(now)
                        .taskProgress(0.0f)
                        .initProgress(0.0f)
                        .state(ADTaskState.INIT.name()) // TODO where to set INIT state
                        .initProgress(0.0f) // TODO where to set INIT state
                        .lastUpdateTime(now)
                        .startedBy(adTask.getStartedBy())
                        .coordinatingNode(clusterService.localNode().getId())
                        .detectionDateRange(adTask.getDetectionDateRange())
                        .user(adTask.getUser())
                        .entity(ImmutableList.of(new Entity(adTask.getDetector().getCategoryField().get(0), entity)))
                        .parentTaskId(parentTaskId)
                        .build();
                    adTaskManager.createADTaskDirectly(adEntityTask, r -> {
                        adEntityTask.setTaskId(r.getId());
                        ActionListener<ADBatchAnomalyResultResponse> delegatedListener = getDelegatedListener(
                            adEntityTask,
                            transportService,
                            listener
                        );
                        executeSingleEntityTask(adEntityTask, transportService, delegatedListener);
                    }, wrappedListener);
                }
            }, transportService, false, wrappedListener);

        } else {
            ActionListener<ADBatchAnomalyResultResponse> delegatedListener = getDelegatedListener(adTask, transportService, listener);
            logger.info("ylwudebug1: run single entity task  {}", adTask.getTaskId());
            // executeSingleEntityTask(adTask, transportService, updatedFields, delegatedListener);
            adTaskManager
                .updateADTask(
                    adTask.getTaskId(),
                    updatedFields,
                    ActionListener.wrap(r -> executeSingleEntityTask(adTask, transportService, delegatedListener), e -> {
                        logger.error("333333333 ", e);
                        delegatedListener.onFailure(e);
                    })
                );
        }
    }

    private String getParentTaskId(ADTask adTask) {
        return adTask.getTaskType().equals(ADTaskType.HISTORICAL_HC_ENTITY.name()) ? adTask.getParentTaskId() : adTask.getTaskId();
    }

    private ActionListener<ADBatchAnomalyResultResponse> getDelegatedListener(
        ADTask adTask,
        TransportService transportService,
        ActionListener<ADBatchAnomalyResultResponse> listener
    ) {
        ActionListener<ADBatchAnomalyResultResponse> actionListener = ActionListener.wrap(r -> {
            listener.onResponse(r);
            if (adTask.isEntityTask()) {
                // When reach this line, the entity task already been put into worker node's cache
                adTaskCacheManager.moveToRunningEntity(adTask.getDetectorId(), adTask.getEntity().get(0).getValue());
            }
            // start new entity task lane
            startNewEntityTaskLane(adTask, transportService);
        }, e -> {
            logger.error("Failed to run task " + adTask.getTaskId(), e);
            if (adTask.isEntityTask()) {
                // When reach this line, failed to start this entity task on worker node
                // adTaskCacheManager.addPendingEntity(adTask.getDetectorId(), adTask.getEntity().get(0).getValue());
                // logger.warn("Push back entity to cache : " + adTask.getEntity().get(0).getValue());
                if (adTaskManager.retryableError(adTask.getError())
                    && !adTaskManager.taskRetryExceedLimits(adTask.getDetectorId(), adTask.getTaskId())) {
                    adTaskManager.pushBackEntityToCache(adTask.getTaskId(), adTask.getDetectorId(), adTask.getEntity().get(0).getValue());
                } else {
                    logger.debug("Remove entity as exception is not retryable");
                    adTaskCacheManager.removeEntity(adTask.getDetectorId(), adTask.getEntity().get(0).getValue());
                }
            }
            listener.onFailure(e);
            // handleException(adTask, e, adEntityTaskExceptionConsumer(adTask));
            logger.debug("ylwudebug8: error happends for task " + adTask.getTaskId() + ", " + adTask.getTaskType());
            handleException(adTask, e);
            if (adTask.isEntityTask()) {
                // logger.info("ylwudebug0319-2: thread: {}, {}",
                // Thread.currentThread().getName(), Thread.currentThread().getId());
                waitBeforeNextEntity(5000);
                adTaskManager.entityTaskDone(adTask, e, transportService);
            }
            startNewEntityTaskLane(adTask, transportService);
        });

        ThreadedActionListener threadedActionListener = new ThreadedActionListener<>(
            logger,
            threadPool,
            AD_BATCH_TASK_THREAD_POOL_NAME,
            actionListener,
            false
        );
        return threadedActionListener;
    }

    private void executeSingleEntityTask(
        ADTask adTask,
        TransportService transportService,
        ActionListener<ADBatchAnomalyResultResponse> delegatedListener
    ) {
        dispatchTask(adTask, ActionListener.wrap(node -> {
            if (clusterService.localNode().getId().equals(node.getId())) {
                // Execute batch task locally
                logger
                    .debug(
                        "execute AD task {} locally on node {} for detector {}",
                        adTask.getTaskId(),
                        node.getId(),
                        adTask.getDetectorId()
                    );
                startADBatchTask(adTask, false, transportService, delegatedListener);
            } else {
                // Execute batch task remotely
                logger
                    .info(
                        "execute AD task {} remotely on node {} for detector {}",
                        adTask.getTaskId(),
                        node.getId(),
                        adTask.getDetectorId()
                    );
                transportService
                    .sendRequest(
                        node,
                        ADBatchTaskRemoteExecutionAction.NAME,
                        new ADBatchAnomalyResultRequest(adTask),
                        option,
                        // TODO: check if still need to add true/false in startADBatchTask
                        new ActionListenerResponseHandler<>(delegatedListener, ADBatchAnomalyResultResponse::new)
                    );
            }
            // runNextEntity(adTask, transportService);
        },
            e -> {
                // runNextEntity(adTask, transportService);
                delegatedListener.onFailure(e);
            }
        ));
    }

    // start new entity task lane
    private void startNewEntityTaskLane(ADTask adTask, TransportService transportService) {
        if (ADTaskType.HISTORICAL_HC_ENTITY.name().equals(adTask.getTaskType())
            && adTaskCacheManager.getAndDecreaseEntityTaskLanes(adTask.getDetectorId()) > 0) {
            logger
                .info(
                    "Start new task lane for detector {}, detector task {}, after task {}, {}",
                    adTask.getDetectorId(),
                    getParentTaskId(adTask),
                    adTask.getTaskId(),
                    adTask.getTaskType()
                );
            runADTask(adTask, transportService, getInternalHCDelegatedListener());
        }
    }

    private void dispatchTask(ADTask adTask, ActionListener<DiscoveryNode> listener) {
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
                StringBuilder errorMessageBuilder = new StringBuilder("All nodes' memory usage exceeds limitation ")
                    .append(DEFAULT_JVM_HEAP_USAGE_THRESHOLD)
                    .append("%. ")
                    .append(NO_ELIGIBLE_NODE_TO_RUN_DETECTOR)
                    .append(adTask.getDetectorId());
                String errorMessage = errorMessageBuilder.toString();
                logger.warn(errorMessage + ", task id " + adTask.getTaskId() + ", " + adTask.getTaskType());
                listener.onFailure(new LimitExceededException(adTask.getDetectorId(), errorMessage));
                // TODO: If no eligible nodes, put the entity back to top entity cache
                return;
            }
            candidateNodeResponse = candidateNodeResponse
                .stream()
                .filter(stat -> (Long) stat.getStatsMap().get(AD_EXECUTING_BATCH_TASK_COUNT.getName()) < maxAdBatchTaskPerNode)
                .collect(Collectors.toList());
            if (candidateNodeResponse.size() == 0) {
                StringBuilder errorMessageBuilder = new StringBuilder("All nodes' memory usage exceeds limitation ")
                    .append(NO_ELIGIBLE_NODE_TO_RUN_DETECTOR)
                    .append(adTask.getDetectorId());
                String errorMessage = errorMessageBuilder.toString();
                logger.warn(errorMessage);
                listener.onFailure(new LimitExceededException(adTask.getDetectorId(), errorMessage));
                return;
            }
            Optional<ADStatsNodeResponse> targetNode = candidateNodeResponse
                .stream()
                .sorted((ADStatsNodeResponse r1, ADStatsNodeResponse r2) -> {
                    int result = ((Long) r1.getStatsMap().get(AD_EXECUTING_BATCH_TASK_COUNT.getName()))
                        .compareTo((Long) r2.getStatsMap().get(AD_EXECUTING_BATCH_TASK_COUNT.getName()));
                    if (result == 0) {
                        // if multiple nodes have same running task count, choose the one with least
                        // JVM heap usage.
                        return ((Long) r1.getStatsMap().get(JVM_HEAP_USAGE.getName()))
                            .compareTo((Long) r2.getStatsMap().get(JVM_HEAP_USAGE.getName()));
                    }
                    return result;
                })
                .findFirst();
            listener.onResponse(targetNode.get().getNode());
        }, exception -> {
            logger.error("Failed to get node's task stats", exception);
            listener.onFailure(exception);
        }));
    }

    /**
     * Start AD task in dedicated batch task thread pool on worker node.
     *
     * @param adTask ad task
     * @param runTaskRemotely run task remotely or not
     * @param transportService transport service
     * @param delegatedListener action listener
     */
    public void startADBatchTask(
        ADTask adTask,
        boolean runTaskRemotely,
        TransportService transportService,
        ActionListener<ADBatchAnomalyResultResponse> delegatedListener
    ) {
        try {
            // check if cluster is eligible to run AD currently, if not eligible like
            // circuit breaker open, will throw exception.
            checkClusterState(adTask);
            // track AD executing batch task and total batch task execution count
            adStats.getStat(AD_EXECUTING_BATCH_TASK_COUNT.getName()).increment();
            adStats.getStat(StatNames.AD_TOTAL_BATCH_TASK_EXECUTION_COUNT.getName()).increment();

            // put AD task into cache
            adTaskCacheManager.add(adTask);
            threadPool.executor(AD_BATCH_TASK_THREAD_POOL_NAME).execute(() -> {
                ActionListener<String> internalListener = internalBatchTaskListener(adTask, transportService);
                try {
                    executeADBatchTask(adTask, internalListener);
                } catch (Exception e) {
                    internalListener.onFailure(e);
                }
            });
            delegatedListener.onResponse(new ADBatchAnomalyResultResponse(clusterService.localNode().getId(), runTaskRemotely));
        } catch (Exception e) {
            logger.error("Fail to start AD batch task " + adTask.getTaskId(), e);
            delegatedListener.onFailure(e);
        }
    }

    private ActionListener<String> internalBatchTaskListener(ADTask adTask, TransportService transportService) {
        String taskId = adTask.getTaskId();
        ActionListener<String> listener = ActionListener.wrap(response -> {
            // If batch task finished normally, remove task from cache and decrease executing task count by 1.
            adTaskCacheManager.remove(taskId);
            adStats.getStat(AD_EXECUTING_BATCH_TASK_COUNT.getName()).decrement();

            if (!adTask.getDetector().isMultientityDetector()) {
                // TODO: check if it's necessary to set task as FINISHED here
                adTaskManager
                    .cleanDetectorCache(
                        adTask,
                        transportService,
                        () -> adTaskManager.updateADTask(taskId, ImmutableMap.of(STATE_FIELD, ADTaskState.FINISHED.name()))
                    );
            } else {
                // TODO: check if it's necessary to set task as FINISHED here
                adTaskManager.updateADTask(adTask.getTaskId(), ImmutableMap.of(STATE_FIELD, ADTaskState.FINISHED.name()));
                adTaskManager.entityTaskDone(adTask, transportService);
            }
        }, e -> {
            // If batch task failed, remove task from cache and decrease executing task count by 1.
            logger.error("ylwudebug7: taskId " + taskId, e);
            adTaskCacheManager.remove(taskId);
            adStats.getStat(AD_EXECUTING_BATCH_TASK_COUNT.getName()).decrement();
            if (!adTask.getDetector().isMultientityDetector()) {
                adTaskManager.cleanDetectorCache(adTask, transportService, () -> handleException(adTask, e));
            } else {
                logger.info("ylwudebug0319-3: thread: {}, {}", Thread.currentThread().getName(), Thread.currentThread().getId());
                waitBeforeNextEntity(5000);
                adTaskManager.entityTaskDone(adTask, e, transportService);
                handleException(adTask, e);
                // });
            }

        });
        ThreadedActionListener<String> threadedActionListener = new ThreadedActionListener<>(
            logger,
            threadPool,
            AD_BATCH_TASK_THREAD_POOL_NAME,
            listener,
            false
        );
        return threadedActionListener;
    }

    private void waitBeforeNextEntity(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException interruptedException) {
            logger.warn("Exception while waiting", interruptedException);
        }
    }

    // TODO: entity task state not change from INIT to RUNNING
    // private void handleException(ADTask adTask, Exception e, Consumer<Exception> consumer) {
    private void handleException(ADTask adTask, Exception e) {
        // Check if batch task was cancelled or not by exception type.
        // If it's cancelled, then increase cancelled task count by 1, otherwise increase failure count by 1.
        if (e instanceof ADTaskCancelledException) {
            adStats.getStat(StatNames.AD_CANCELED_BATCH_TASK_COUNT.getName()).increment();
        } else if (ExceptionUtil.countInStats(e)) {
            adStats.getStat(StatNames.AD_BATCH_TASK_FAILURE_COUNT.getName()).increment();
        }
        // logger.info("ylwudebugA: task id " + adTask.getTaskId());
        // TODO: handle limit exceed exception
        // Handle AD task exception
        adTaskManager.handleADTaskException(adTask, e);
        // if (consumer != null) {
        // consumer.accept(e);
        // }
    }

    private void executeADBatchTask(ADTask adTask, ActionListener<String> internalListener) {
        // // track AD executing batch task and total batch task execution count
        // adStats.getStat(AD_EXECUTING_BATCH_TASK_COUNT.getName()).increment();
        // adStats.getStat(StatNames.AD_TOTAL_BATCH_TASK_EXECUTION_COUNT.getName()).increment();
        //
        // // put AD task into cache
        // adTaskCacheManager.add(adTask);

        // start to run first piece
        Instant executeStartTime = Instant.now();
        // TODO: refactor to make the workflow more clear
        runFirstPiece(adTask, executeStartTime, internalListener);
    }

    private void checkClusterState(ADTask adTask) {
        // check if AD plugin is enabled
        checkADPluginEnabled(adTask.getDetectorId());

        // check if circuit breaker is open
        checkCircuitBreaker(adTask);
    }

    private void checkADPluginEnabled(String detectorId) {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new EndRunException(detectorId, CommonErrorMessages.DISABLED_ERR_MSG, true).countedInStats(false);
        }
    }

    private void checkCircuitBreaker(ADTask adTask) {
        String taskId = adTask.getTaskId();
        if (adCircuitBreakerService.isOpen()) {
            String error = "Circuit breaker is open";
            logger.error("AD task: {}, {}", taskId, error);
            throw new LimitExceededException(adTask.getDetectorId(), error, true);
        }
    }

    private void runFirstPiece(ADTask adTask, Instant executeStartTime, ActionListener<String> internalListener) {
        try {
            adTaskManager
                .updateADTask(
                    adTask.getTaskId(),
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            ADTaskState.INIT.name(),
                            CURRENT_PIECE_FIELD,
                            adTask.getDetectionDateRange().getStartTime().toEpochMilli(),
                            TASK_PROGRESS_FIELD,
                            0.0f,
                            INIT_PROGRESS_FIELD,
                            0.0f,
                            WORKER_NODE_FIELD,
                            clusterService.localNode().getId()
                        ),
                    ActionListener.wrap(r -> {
                        try {
                            checkIfADTaskCancelled(adTask.getTaskId());
                            getDateRangeOfSourceData(adTask, (dataStartTime, dataEndTime) -> {
                                long interval = ((IntervalTimeConfiguration) adTask.getDetector().getDetectionInterval())
                                    .toDuration()
                                    .toMillis();
                                long expectedPieceEndTime = dataStartTime + pieceSize * interval;
                                long firstPieceEndTime = Math.min(expectedPieceEndTime, dataEndTime);
                                logger
                                    .debug(
                                        "start first piece from {} to {}, interval {}, dataStartTime {}, dataEndTime {},"
                                            + " detectorId {}, taskId {}",
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
                                    internalListener
                                );
                            }, internalListener);
                        } catch (Exception e) {
                            internalListener.onFailure(e);
                        }
                    }, e -> {
                        logger.error("4444444444 ", e);
                        internalListener.onFailure(e);
                    })
                );
        } catch (Exception exception) {
            internalListener.onFailure(exception);
        }
    }

    private void getDateRangeOfSourceData(ADTask adTask, BiConsumer<Long, Long> consumer, ActionListener internalListener) {
        String taskId = adTask.getTaskId();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.min(AGG_NAME_MIN_TIME).field(adTask.getDetector().getTimeField()))
            .aggregation(AggregationBuilders.max(AGG_NAME_MAX_TIME).field(adTask.getDetector().getTimeField()))
            .size(0);
        if (adTask.getEntity() != null && adTask.getEntity().size() > 0) {
            BoolQueryBuilder query = new BoolQueryBuilder();
            adTask.getEntity().forEach(entity -> query.filter(new TermQueryBuilder(entity.getName(), entity.getValue())));
            searchSourceBuilder.query(query);
        }

        SearchRequest request = new SearchRequest()
            .indices(adTask.getDetector().getIndices().toArray(new String[0]))
            .source(searchSourceBuilder);

        client.search(request, ActionListener.wrap(r -> {
            InternalMin minAgg = r.getAggregations().get(AGG_NAME_MIN_TIME);
            InternalMax maxAgg = r.getAggregations().get(AGG_NAME_MAX_TIME);
            double minValue = minAgg.getValue();
            double maxValue = maxAgg.getValue();
            // If time field not exist or there is no value, will return infinity value
            if (minValue == Double.POSITIVE_INFINITY) {
                internalListener.onFailure(new ResourceNotFoundException(adTask.getDetectorId(), "There is no data in the time field"));
                return;
            }
            long interval = ((IntervalTimeConfiguration) adTask.getDetector().getDetectionInterval()).toDuration().toMillis();

            DetectionDateRange detectionDateRange = adTask.getDetectionDateRange();
            long dataStartTime = detectionDateRange.getStartTime().toEpochMilli();
            long dataEndTime = detectionDateRange.getEndTime().toEpochMilli();
            long minDate = (long) minValue;
            long maxDate = (long) maxValue;

            if (minDate >= dataEndTime || maxDate <= dataStartTime) {
                internalListener
                    .onFailure(new ResourceNotFoundException(adTask.getDetectorId(), "There is no data in the detection date range"));
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
            logger.debug("ylwudebug: adjusted range: start: {}, end: {}, taskId: {}", dataStartTime, dataEndTime, taskId);
            if ((dataEndTime - dataStartTime) < THRESHOLD_MODEL_TRAINING_SIZE * interval) {
                internalListener.onFailure(new AnomalyDetectionException("There is no enough data to train model").countedInStats(false));
                return;
            }
            consumer.accept(dataStartTime, dataEndTime);
        }, e -> { internalListener.onFailure(e); }));
    }

    private void getFeatureData(
        ADTask adTask,
        long pieceStartTime,
        long pieceEndTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        Instant executeStartTime,
        ActionListener<String> internalListener
    ) {
        ActionListener<Map<Long, Optional<double[]>>> actionListener = ActionListener.wrap(dataPoints -> {
            try {
                if (dataPoints.size() == 0) {
                    logger.debug("No data in current piece with end time: " + pieceEndTime);
                    runNextPiece(adTask, pieceEndTime, dataStartTime, dataEndTime, interval, internalListener);
                } else {
                    detectAnomaly(
                        adTask,
                        dataPoints,
                        pieceStartTime,
                        pieceEndTime,
                        dataStartTime,
                        dataEndTime,
                        interval,
                        executeStartTime,
                        internalListener
                    );
                }
            } catch (Exception e) {
                internalListener.onFailure(e);
            }
        }, exception -> {
            logger.debug("Fail to get feature data by batch for this piece with end time: " + pieceEndTime);
            // TODO: Exception may be caused by wrong feature query or some bad data. Differentiate these
            // and skip current piece if error caused by bad data.
            internalListener.onFailure(exception);
        });
        ThreadedActionListener<Map<Long, Optional<double[]>>> threadedActionListener = new ThreadedActionListener<>(
            logger,
            threadPool,
            AD_BATCH_TASK_THREAD_POOL_NAME,
            actionListener,
            false
        );

        featureManager
            .getFeatureDataPointsByBatch(adTask.getDetector(), adTask.getEntity(), pieceStartTime, pieceEndTime, threadedActionListener);
    }

    private void detectAnomaly(
        ADTask adTask,
        Map<Long, Optional<double[]>> dataPoints,
        long pieceStartTime,
        long pieceEndTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        Instant executeStartTime,
        ActionListener<String> internalListener
    ) {
        String taskId = adTask.getTaskId();
        RandomCutForest rcf = adTaskCacheManager.getRcfModel(taskId);
        ThresholdingModel threshold = adTaskCacheManager.getThresholdModel(taskId);
        Deque<Map.Entry<Long, Optional<double[]>>> shingle = adTaskCacheManager.getShingle(taskId);

        List<AnomalyResult> anomalyResults = new ArrayList<>();

        long intervalEndTime = pieceStartTime;
        for (int i = 0; i < pieceSize && intervalEndTime < dataEndTime; i++) {
            Optional<double[]> dataPoint = dataPoints.containsKey(intervalEndTime) ? dataPoints.get(intervalEndTime) : Optional.empty();
            intervalEndTime = intervalEndTime + interval;
            SinglePointFeatures feature = featureManager
                .getShingledFeatureForHistoricalAnalysis(adTask.getDetector(), shingle, dataPoint, intervalEndTime);
            List<FeatureData> featureData = null;
            if (feature.getUnprocessedFeatures().isPresent()) {
                featureData = ParseUtils.getFeatureData(feature.getUnprocessedFeatures().get(), adTask.getDetector());
            }
            if (!feature.getProcessedFeatures().isPresent()) {
                String error = feature.getUnprocessedFeatures().isPresent()
                    ? "No full shingle in current detection window"
                    : "No data in current detection window";
                String resultTaskId = adTask.getParentTaskId() != null ? adTask.getParentTaskId() : adTask.getTaskId();
                AnomalyResult anomalyResult = new AnomalyResult(
                    adTask.getDetectorId(),
                    resultTaskId,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    featureData,
                    Instant.ofEpochMilli(intervalEndTime - interval),
                    Instant.ofEpochMilli(intervalEndTime),
                    executeStartTime,
                    Instant.now(),
                    error,
                    adTask.getEntity(),
                    adTask.getDetector().getUser(),
                    anomalyDetectionIndices.getSchemaVersion(ADIndex.RESULT)
                );
                anomalyResults.add(anomalyResult);
            } else {
                double[] point = feature.getProcessedFeatures().get();
                double score = rcf.getAnomalyScore(point);
                // TODO:don't put back the anomalous data points?
                rcf.update(point);
                double grade = 0d;
                double confidence = 0d;
                if (!adTaskCacheManager.isThresholdModelTrained(taskId)) {
                    if (adTaskCacheManager.getThresholdModelTrainingDataSize(taskId) < THRESHOLD_MODEL_TRAINING_SIZE) {
                        if (score > 0) {
                            adTaskCacheManager.addThresholdModelTrainingData(taskId, score);
                        }
                    } else {
                        logger.debug("training threshold model");
                        threshold.train(adTaskCacheManager.getThresholdModelTrainingData(taskId));
                        adTaskCacheManager.setThresholdModelTrained(taskId, true);
                    }
                } else {
                    grade = threshold.grade(score);
                    confidence = threshold.confidence();
                    if (score > 0) {
                        threshold.update(score);
                    }
                }
                String resultTaskId = adTask.getParentTaskId() != null ? adTask.getParentTaskId() : adTask.getTaskId();
                AnomalyResult anomalyResult = new AnomalyResult(
                    adTask.getDetectorId(),
                    resultTaskId,
                    score,
                    grade,
                    confidence,
                    featureData,
                    Instant.ofEpochMilli(intervalEndTime - interval),
                    Instant.ofEpochMilli(intervalEndTime),
                    executeStartTime,
                    Instant.now(),
                    null,
                    adTask.getEntity(),
                    adTask.getDetector().getUser(),
                    anomalyDetectionIndices.getSchemaVersion(ADIndex.RESULT)
                );
                anomalyResults.add(anomalyResult);
            }
        }

        anomalyResultBulkIndexHandler
            .bulkIndexAnomalyResult(
                anomalyResults,
                new ThreadedActionListener<>(logger, threadPool, AD_BATCH_TASK_THREAD_POOL_NAME, ActionListener.wrap(r -> {
                    try {
                        runNextPiece(adTask, pieceEndTime, dataStartTime, dataEndTime, interval, internalListener);
                    } catch (Exception e) {
                        internalListener.onFailure(e);
                    }
                }, e -> {
                    logger.error("Fail to bulk index anomaly result", e);
                    internalListener.onFailure(e);
                }), false)
            );
    }

    private void runNextPiece(
        ADTask adTask,
        long pieceStartTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        ActionListener<String> internalListener
    ) {
        String taskId = adTask.getTaskId();
        float initProgress = calculateInitProgress(taskId);
        String taskState = initProgress >= 1.0f ? ADTaskState.RUNNING.name() : ADTaskState.INIT.name();
        logger.debug("Init progress: {}, taskState:{}, task id: {}", initProgress, taskState, taskId);

        if (pieceStartTime < dataEndTime) {
            checkClusterState(adTask);
            long expectedPieceEndTime = pieceStartTime + pieceSize * interval;
            long pieceEndTime = expectedPieceEndTime > dataEndTime ? dataEndTime : expectedPieceEndTime;
            int i = 0;
            while (i < pieceIntervalSeconds) {
                // check if task cancelled every second, so frontend can get STOPPED state
                // in 1 second once task cancelled.
                checkIfADTaskCancelled(taskId);
                adTaskCacheManager.getRateLimiter(adTask.getDetectorId(), adTask.getTaskId()).acquire(1);
                i++;
            }
            logger
                .debug(
                    "task id: {}, start next piece start from {} to {}, interval {}",
                    adTask.getTaskId(),
                    pieceStartTime,
                    pieceEndTime,
                    interval
                );
            float taskProgress = (float) (pieceStartTime - dataStartTime) / (dataEndTime - dataStartTime);
            logger.debug("Task progress: {}, task id:{}, detector id:{}", taskProgress, taskId, adTask.getDetectorId());
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
                            r -> {
                                getFeatureData(
                                    adTask,
                                    pieceStartTime,
                                    pieceEndTime,
                                    dataStartTime,
                                    dataEndTime,
                                    interval,
                                    Instant.now(),
                                    internalListener
                                );
                            },
                            e -> {
                                logger.error("Failed to update task " + adTask.getTaskId(), e);
                                internalListener.onFailure(e);
                            }
                        )
                );
        } else {
            logger.info("AD task finished for detector {}, task id: {}", adTask.getDetectorId(), taskId);
            // adTaskCacheManager.remove(taskId, adTask.getEntity());
            adTaskCacheManager.remove(taskId);
            adTaskManager
                .updateADTask(
                    taskId,
                    ImmutableMap
                        .of(
                            CURRENT_PIECE_FIELD,
                            dataEndTime,
                            TASK_PROGRESS_FIELD,
                            1.0f,
                            EXECUTION_END_TIME_FIELD,
                            Instant.now().toEpochMilli(),
                            INIT_PROGRESS_FIELD,
                            initProgress,
                            STATE_FIELD,
                            ADTaskState.FINISHED
                        ),
                    ActionListener.wrap(r -> internalListener.onResponse("task execution done"), e -> { internalListener.onFailure(e); })
                );
        }
    }

    private float calculateInitProgress(String taskId) {
        RandomCutForest rcf = adTaskCacheManager.getRcfModel(taskId);
        if (rcf == null) {
            return 0.0f;
        }
        float initProgress = rcf.getTotalUpdates() >= NUM_MIN_SAMPLES ? 1.0f : (float) rcf.getTotalUpdates() / NUM_MIN_SAMPLES;
        logger.debug("RCF total updates {} for task {}", rcf.getTotalUpdates(), taskId);
        return initProgress;
    }

    private void checkIfADTaskCancelled(String taskId) {
        if (adTaskCacheManager.contains(taskId) && adTaskCacheManager.isCancelled(taskId)) {
            throw new ADTaskCancelledException(adTaskCacheManager.getCancelReason(taskId), adTaskCacheManager.getCancelledBy(taskId));
        }
    }

}
