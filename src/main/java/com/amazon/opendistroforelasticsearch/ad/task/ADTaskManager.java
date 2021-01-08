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

import static com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages.DETECTOR_ALREADY_RUNNING;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.DETECTOR_ID_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.ERROR_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.EXECUTION_END_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.EXECUTION_START_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.IS_LATEST_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.LAST_UPDATE_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STATE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STOPPED_BY_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil.getErrorMessage;
import static com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil.getShardsFailure;
import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ADTaskCancelledException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.DuplicateTaskException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskProfile;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskState;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskType;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ADCancelTaskAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADCancelTaskRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileNodeResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyDetectorJobResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ForwardADTaskAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ForwardADTaskRequest;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;

/**
 * Manage AD task.
 */
public class ADTaskManager {
    private final Logger logger = LogManager.getLogger(this.getClass());

    private final Client client;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final AnomalyDetectionIndices detectionIndices;
    private final DiscoveryNodeFilterer nodeFilter;
    private final ADTaskCacheManager adTaskCacheManager;

    private final HashRing hashRing;
    private final TransportRequestOptions option;
    private volatile Integer maxAdTaskDocsPerDetector;
    private volatile Integer pieceIntervalSeconds;

    public ADTaskManager(
        Settings settings,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry,
        AnomalyDetectionIndices detectionIndices,
        DiscoveryNodeFilterer nodeFilter,
        HashRing hashRing,
        ADTaskCacheManager adTaskCacheManager
    ) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.detectionIndices = detectionIndices;
        this.nodeFilter = nodeFilter;
        this.clusterService = clusterService;
        this.adTaskCacheManager = adTaskCacheManager;
        this.hashRing = hashRing;

        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings))
            .build();

        this.maxAdTaskDocsPerDetector = MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR, it -> maxAdTaskDocsPerDetector = it);

        this.pieceIntervalSeconds = BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCH_TASK_PIECE_INTERVAL_SECONDS, it -> pieceIntervalSeconds = it);
    }

    /**
     * Start detector. Will create schedule job for realtime detector,
     * and start AD task for historical detector.
     *
     * @param detectorId detector id
     * @param handler anomaly detector job action handler
     * @param user user
     * @param listener action listener
     */
    public void startDetector1(
        String detectorId,
        IndexAnomalyDetectorJobActionHandler handler,
        User user,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        getDetector(
            detectorId,
            (detector) -> handler.startAnomalyDetectorJob(detector), // run realtime detector
            (detector) -> startHistoricalDetector(detector, user, listener), // run historical detector
            listener
        );
    }

    public void startDetector(
        String detectorId,
        IndexAnomalyDetectorJobActionHandler handler,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        getDetector(
            detectorId,
            (detector) -> handler.startAnomalyDetectorJob(detector), // run realtime detector
            (detector) -> forwardToOwningNode(detector, user, transportService, listener), // run historical detector
            listener
        );
    }

    private void forwardToOwningNode(
        AnomalyDetector detector,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        Optional<DiscoveryNode> owningNode = hashRing.getOwningNode(detector.getDetectorId());
        if (!owningNode.isPresent()) {
            listener.onFailure(new ElasticsearchStatusException("No eligible node to run detector", RestStatus.INTERNAL_SERVER_ERROR));
            return;
        }
        transportService
            .sendRequest(
                owningNode.get(),
                ForwardADTaskAction.NAME,
                new ForwardADTaskRequest(detector, user),
                this.option,
                new ActionListenerResponseHandler<>(listener, AnomalyDetectorJobResponse::new)
            );
    }

    /**
     * Stop detector.
     * For realtime detector, will set detector job as disabled.
     * For historical detector, will set its AD task as cancelled.
     *
     * @param detectorId detector id
     * @param handler AD job action handler
     * @param user user
     * @param listener action listener
     */
    public void stopDetector(
        String detectorId,
        IndexAnomalyDetectorJobActionHandler handler,
        User user,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        getDetector(
            detectorId,
            // stop realtime detector job
            (detector) -> handler.stopAnomalyDetectorJob(detectorId),
            // stop historical detector AD task
            (detector) -> getLatestADTask(detectorId, (task) -> stopHistoricalDetector(detectorId, task, user, listener), listener),
            listener
        );
    }

    private void getDetector(
        String detectorId,
        Consumer<AnomalyDetector> realTimeDetectorConsumer,
        Consumer<AnomalyDetector> historicalDetectorConsumer,
        ActionListener listener
    ) {
        GetRequest getRequest = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).id(detectorId);
        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                listener
                    .onFailure(
                        new ElasticsearchStatusException("AnomalyDetector is not found with id: " + detectorId, RestStatus.NOT_FOUND)
                    );
                return;
            }
            try (
                XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());

                String error = validateDetector(detector);
                if (error != null) {
                    listener.onFailure(new ElasticsearchStatusException(error, RestStatus.BAD_REQUEST));
                    return;
                }

                if (detector.isRealTimeDetector()) {
                    // run realtime detector
                    realTimeDetectorConsumer.accept(detector);
                } else {
                    // run historical detector
                    historicalDetectorConsumer.accept(detector);
                }
            } catch (Exception e) {
                String message = "Failed to start anomaly detector " + detectorId;
                logger.error(message, e);
                listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> listener.onFailure(exception)));
    }

    /**
     * Get latest AD task and execute consumer function.
     *
     * @param detectorId detector id
     * @param function consumer function
     * @param listener action listener
     */
    public void getLatestADTask(String detectorId, Consumer<Optional<ADTask>> function, ActionListener listener) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        searchRequest.indices(CommonName.DETECTION_STATE_INDEX);

        client.search(searchRequest, ActionListener.wrap(r -> {
            long totalTasks = r.getHits().getTotalHits().value;
            if (totalTasks < 1) {
                // don't throw exception here as consumer functions need to handle this case
                // in different way.
                function.accept(Optional.empty());
                return;
            }
            SearchHit searchHit = r.getHits().getAt(0);
            try (XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                ADTask adTask = ADTask.parse(parser, searchHit.getId());

                if (!isADTaskEnded(adTask) && lastUpdateTimeExpired(adTask)) {
                    // If AD task is still running, but its last updated time not refreshed
                    // for 2 pieces intervals, we will get task profile to check if it's
                    // really running.
                    // For example, ES process crashes, then all tasks running on it will stay
                    // as running. We can reset the task state when next read happen.
                    getADTaskProfile(adTask, ActionListener.wrap(taskProfile -> {
                        if (taskProfile.getNodeId() == null) {
                            resetTaskStateAsStopped(adTask);
                            adTask.setState(ADTaskState.STOPPED.name());
                        }
                        function.accept(Optional.of(adTask));
                    }, e -> listener.onFailure(e)));
                } else {
                    function.accept(Optional.of(adTask));
                }
            } catch (Exception e) {
                String message = "Failed to parse AD task for detector " + detectorId;
                logger.error(message, e);
                listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.accept(Optional.empty());
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private void stopHistoricalDetector(
        String detectorId,
        Optional<ADTask> adTask,
        User user,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        if (!adTask.isPresent()) {
            listener.onFailure(new ResourceNotFoundException(detectorId, "Detector not started"));
            return;
        }

        if (isADTaskEnded(adTask.get())) {
            listener.onFailure(new ResourceNotFoundException(detectorId, "No running task found"));
            return;
        }

        String taskId = adTask.get().getTaskId();
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        String userName = user == null ? null : user.getName();

        ADCancelTaskRequest cancelTaskRequest = new ADCancelTaskRequest(detectorId, userName, dataNodes);
        client
            .execute(
                ADCancelTaskAction.INSTANCE,
                cancelTaskRequest,
                ActionListener
                    .wrap(response -> { listener.onResponse(new AnomalyDetectorJobResponse(taskId, 0, 0, 0, RestStatus.OK)); }, e -> {
                        logger.error("Failed to cancel AD task " + taskId, e);
                        listener.onFailure(e);
                    })
            );
    }

    private boolean lastUpdateTimeExpired(ADTask adTask) {
        return adTask.getLastUpdateTime().plus(2 * pieceIntervalSeconds, ChronoUnit.SECONDS).isBefore(Instant.now());
    }

    private boolean isADTaskEnded(ADTask adTask) {
        return ADTaskState.STOPPED.name().equals(adTask.getState())
            || ADTaskState.FINISHED.name().equals(adTask.getState())
            || ADTaskState.FAILED.name().equals(adTask.getState());
    }

    private void resetTaskStateAsStopped(ADTask adTask) {
        if (!isADTaskEnded(adTask)) {
            Map<String, Object> updatedFields = new HashMap<>();
            updatedFields.put(STATE_FIELD, ADTaskState.STOPPED.name());
            updateADTask(adTask.getTaskId(), updatedFields);
        }
    }

    /**
     * Get AD task profile data.
     *
     * @param detectorId detector id
     * @param listener action listener
     */
    public void getLatestADTaskProfile(String detectorId, ActionListener<DetectorProfile> listener) {
        getLatestADTask(detectorId, adTask -> {
            if (adTask.isPresent()) {
                getADTaskProfile(adTask.get(), ActionListener.wrap(adTaskProfile -> {
                    DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
                    profileBuilder.adTaskProfile(adTaskProfile);
                    listener.onResponse(profileBuilder.build());
                }, e -> {
                    logger.error("Failed to get AD task profile for task " + adTask.get().getTaskId(), e);
                    listener.onFailure(e);
                }));
            } else {
                listener.onFailure(new ResourceNotFoundException(detectorId, "Can't find task for detector"));
            }
        }, listener);
    }

    private void getADTaskProfile(ADTask adTask, ActionListener<ADTaskProfile> listener) {
        String taskId = adTask.getTaskId();

        if (adTaskCacheManager.contains(taskId)) {
            ADTaskProfile adTaskProfile = getLocalADTaskProfile(taskId);
            adTaskProfile.setAdTask(adTask);
            listener.onResponse(adTaskProfile);
        } else {
            DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
            ADTaskProfileRequest adTaskProfileRequest = new ADTaskProfileRequest(taskId, dataNodes);
            client.execute(ADTaskProfileAction.INSTANCE, adTaskProfileRequest, ActionListener.wrap(response -> {
                List<ADTaskProfile> nodeResponses = response
                    .getNodes()
                    .stream()
                    .filter(r -> r.getAdTaskProfile() != null)
                    .map(ADTaskProfileNodeResponse::getAdTaskProfile)
                    .collect(Collectors.toList());

                if (nodeResponses.size() > 1) {
                    String error = nodeResponses.size() + " tasks running for detector " + adTask.getDetectorId();
                    logger.error(error);
                    listener.onFailure(new InternalFailure(adTask.getDetectorId(), error));
                    return;
                }
                if (nodeResponses.size() == 0) {
                    ADTaskProfile adTaskProfile = new ADTaskProfile(adTask, null, null, null, null, null);
                    listener.onResponse(adTaskProfile);
                } else {
                    ADTaskProfile nodeResponse = nodeResponses.get(0);
                    ADTaskProfile adTaskProfile = new ADTaskProfile(
                        adTask,
                        nodeResponse.getShingleSize(),
                        nodeResponse.getRcfTotalUpdates(),
                        nodeResponse.getThresholdModelTrained(),
                        nodeResponse.getThresholdNodelTrainingDataSize(),
                        nodeResponse.getNodeId()
                    );
                    listener.onResponse(adTaskProfile);
                }
            }, e -> {
                logger.error("Failed to get task profile for task " + adTask.getTaskId(), e);
                listener.onFailure(e);
            }));
        }
    }

    public ADTaskProfile getLocalADTaskProfile(String taskId) {
        ADTaskProfile adTaskProfile = null;
        if (adTaskCacheManager.contains(taskId)) {
            adTaskProfile = new ADTaskProfile(
                adTaskCacheManager.getShingle(taskId).size(),
                adTaskCacheManager.getRcfModel(taskId).getTotalUpdates(),
                adTaskCacheManager.isThresholdModelTrained(taskId),
                adTaskCacheManager.getThresholdModelTrainingDataSize(taskId),
                clusterService.localNode().getId()
            );
        }
        return adTaskProfile;
    }

    private String validateDetector(AnomalyDetector detector) {
        if (detector.getFeatureAttributes().size() == 0) {
            return "Can't start detector job as no features configured";
        }
        if (detector.getEnabledFeatureIds().size() == 0) {
            return "Can't start detector job as no enabled features configured";
        }
        return null;
    }

    public void startHistoricalDetector(AnomalyDetector detector, User user, ActionListener<AnomalyDetectorJobResponse> listener) {
        try {
            adTaskCacheManager.put(detector.getDetectorId());
            if (detectionIndices.doesDetectorStateIndexExist()) {
                checkCurrentTaskState(detector, user, listener);
            } else {
                detectionIndices.initDetectionStateIndex(ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        logger.info("Created {} with mappings.", CommonName.DETECTION_STATE_INDEX);
                        executeHistoricalDetector(detector, user, listener);
                    } else {
                        String error = "Create index " + CommonName.DETECTION_STATE_INDEX + " with mappings not acknowledged";
                        logger.warn(error);
                        listener.onFailure(new ElasticsearchStatusException(error, RestStatus.INTERNAL_SERVER_ERROR));
                    }
                }, e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        executeHistoricalDetector(detector, user, listener);
                    } else {
                        logger.error("Failed to init anomaly detection state index", e);
                        listener.onFailure(e);
                    }
                }));
            }
        } catch (Exception e) {
            if (e instanceof LimitExceededException && DETECTOR_ALREADY_RUNNING.equals(e.getMessage())) {
                logger.debug(DETECTOR_ALREADY_RUNNING + ", detector id: " + detector.getDetectorId());
                listener.onFailure(new ElasticsearchStatusException(DETECTOR_ALREADY_RUNNING, RestStatus.BAD_REQUEST));
            } else {
                logger.error("Failed to start historical detector " + detector.getDetectorId(), e);
                listener.onFailure(e);
            }
        }
    }

    private void checkCurrentTaskState(AnomalyDetector detector, User user, ActionListener<AnomalyDetectorJobResponse> listener) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detector.getDetectorId()));
        query.filter(new TermsQueryBuilder(STATE_FIELD, ADTaskState.CREATED.name(), ADTaskState.INIT.name(), ADTaskState.RUNNING.name()));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);
        searchRequest.indices(CommonName.DETECTION_STATE_INDEX);

        client.search(searchRequest, ActionListener.wrap(r -> {
            if (r.getHits().getTotalHits().value > 0) {
                listener.onFailure(new ElasticsearchStatusException(DETECTOR_ALREADY_RUNNING, RestStatus.BAD_REQUEST));
            } else {
                executeHistoricalDetector(detector, user, listener);
            }
        }, e -> {
            logger.error("Failed to search current running task for detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }));
    }

    private void executeHistoricalDetector(AnomalyDetector detector, User user, ActionListener<AnomalyDetectorJobResponse> listener) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(CommonName.DETECTION_STATE_INDEX);
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detector.getDetectorId()));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        updateByQueryRequest.setQuery(query);
        updateByQueryRequest.setRefresh(true);
        updateByQueryRequest.setScript(new Script("ctx._source.is_latest = false;"));

        client.execute(UpdateByQueryAction.INSTANCE, updateByQueryRequest, ActionListener.wrap(r -> {
            List<BulkItemResponse.Failure> bulkFailures = r.getBulkFailures();
            if (bulkFailures.isEmpty()) {
                createNewADTask(detector, user, listener);
            } else {
                logger.error("Failed to update old task's state for detector: {}, response: {} ", detector.getDetectorId(), r.toString());
                listener.onFailure(bulkFailures.get(0).getCause());
            }
        }, e -> {
            logger.error("Failed to reset old tasks as not latest for detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }));
    }

    private void createNewADTask(AnomalyDetector detector, User user, ActionListener<AnomalyDetectorJobResponse> listener) {
        String userName = user == null ? null : user.getName();
        Instant now = Instant.now();
        ADTask adTask = new ADTask.Builder()
            .detectorId(detector.getDetectorId())
            .detector(detector)
            .isLatest(true)
            .taskType(ADTaskType.HISTORICAL.name())
            .executionStartTime(now)
            .taskProgress(0.0f)
            .initProgress(0.0f)
            .state(ADTaskState.CREATED.name())
            .lastUpdateTime(now)
            .startedBy(userName)
            .build();

        IndexRequest request = new IndexRequest(CommonName.DETECTION_STATE_INDEX);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            request
                .source(adTask.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client
                .index(
                    request,
                    ActionListener
                        .wrap(
                            r -> onIndexADTaskResponse(
                                r,
                                adTask,
                                (response, delegatedListener) -> cleanOldAdTaskDocs(response, adTask, delegatedListener),
                                listener
                            ),
                            e -> {
                                logger.error("Failed to create AD task for detector " + detector.getDetectorId(), e);
                                listener.onFailure(e);
                            }
                        )
                );
        } catch (Exception e) {
            logger.error("Failed to create AD task for detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }
    }

    private void onIndexADTaskResponse(
        IndexResponse response,
        ADTask adTask,
        BiConsumer<IndexResponse, ActionListener<AnomalyDetectorJobResponse>> function,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        if (response == null || response.getResult() != CREATED) {
            String errorMsg = getShardsFailure(response);
            listener.onFailure(new ElasticsearchStatusException(errorMsg, response.status()));
            return;
        }
        adTask.setTaskId(response.getId());
        ActionListener<AnomalyDetectorJobResponse> delegatedListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> {
            listener.onFailure(e);
            handleADTaskException(adTask, e);
        });
        if (function != null) {
            function.accept(response, delegatedListener);
        }
    }

    private void cleanOldAdTaskDocs(IndexResponse response, ADTask adTask, ActionListener<AnomalyDetectorJobResponse> listener) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, adTask.getDetectorId()));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, false));
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder
            .query(query)
            .sort(EXECUTION_START_TIME_FIELD, SortOrder.DESC)
            // Search query "from" starts from 0.
            .from(maxAdTaskDocsPerDetector - 1)
            .trackTotalHits(true)
            .size(1);
        searchRequest.source(sourceBuilder).indices(CommonName.DETECTION_STATE_INDEX);
        String detectorId = adTask.getDetectorId();

        client.search(searchRequest, ActionListener.wrap(r -> {
            Iterator<SearchHit> iterator = r.getHits().iterator();
            if (iterator.hasNext()) {
                logger
                    .debug(
                        "AD tasks count for detector {} is {}, exceeds limit of {}",
                        detectorId,
                        r.getHits().getTotalHits().value,
                        maxAdTaskDocsPerDetector
                    );
                SearchHit searchHit = r.getHits().getAt(0);
                try (
                    XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    ADTask task = ADTask.parse(parser, searchHit.getId());

                    DeleteByQueryRequest request = new DeleteByQueryRequest(CommonName.DETECTION_STATE_INDEX);
                    RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(EXECUTION_START_TIME_FIELD);
                    rangeQueryBuilder.lt(task.getExecutionStartTime().toEpochMilli()).format("epoch_millis");
                    request.setQuery(rangeQueryBuilder);
                    client.execute(DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(res -> {
                        logger
                            .debug(
                                "Deleted {} old AD tasks started equals or before {} for detector {}",
                                res.getDeleted(),
                                adTask.getExecutionStartTime().toEpochMilli(),
                                detectorId
                            );
                        runBatchResultAction(response, adTask, listener);
                    }, e -> {
                        logger.warn("Failed to clean AD tasks for detector " + detectorId, e);
                        listener.onFailure(e);
                    }));
                } catch (Exception e) {
                    logger.warn("Failed to parse AD tasks for detector " + detectorId, e);
                    listener.onFailure(e);
                }
            } else {
                runBatchResultAction(response, adTask, listener);
            }
        }, e -> {
            logger.warn("Failed to search AD tasks for detector " + detectorId, e);
            listener.onFailure(e);
        }));
    }

    private void runBatchResultAction(IndexResponse response, ADTask adTask, ActionListener<AnomalyDetectorJobResponse> listener) {
        client.execute(ADBatchAnomalyResultAction.INSTANCE, new ADBatchAnomalyResultRequest(adTask), ActionListener.wrap(r -> {
            String remoteOrLocal = r.isRunTaskRemotely() ? "remote" : "local";
            logger
                .info(
                    "AD task {} of detector {} dispatched to {} node {}",
                    adTask.getTaskId(),
                    adTask.getDetectorId(),
                    remoteOrLocal,
                    r.getNodeId()
                );
            AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                response.getId(),
                response.getVersion(),
                response.getSeqNo(),
                response.getPrimaryTerm(),
                RestStatus.OK
            );
            listener.onResponse(anomalyDetectorJobResponse);
        }, e -> listener.onFailure(e)));
    }

    /**
     * Handle exceptions for AD task. Update task state and record error message.
     *
     * @param adTask AD task
     * @param e exception
     */
    public void handleADTaskException(ADTask adTask, Exception e) {
        // TODO: remove task execution from map if execution fails
        // TODO: handle timeout exception
        String state = ADTaskState.FAILED.name();
        Map<String, Object> updatedFields = new HashMap<>();
        if (e instanceof ADTaskCancelledException) {
            logger.warn("AD task cancelled: " + adTask.getTaskId());
            state = ADTaskState.STOPPED.name();
            String stoppedBy = ((ADTaskCancelledException) e).getCancelledBy();
            if (stoppedBy != null) {
                updatedFields.put(STOPPED_BY_FIELD, stoppedBy);
            }
        } else if (e instanceof DuplicateTaskException) {
            updatedFields.put(IS_LATEST_FIELD, false);
        } else {
            logger.error("Failed to execute AD batch task, task id: " + adTask.getTaskId() + ", detector id: " + adTask.getDetectorId(), e);
        }
        updatedFields.put(ERROR_FIELD, getErrorMessage(e));
        updatedFields.put(STATE_FIELD, state);
        updatedFields.put(EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli());
        updateADTask(adTask.getTaskId(), updatedFields);
    }

    private void updateADTask(String taskId, Map<String, Object> updatedFields) {
        updateADTask(taskId, updatedFields, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                logger.debug("Updated AD task successfully: {}", response.status());
            } else {
                logger.error("Failed to update AD task {}, status: {}", taskId, response.status());
            }
        }, e -> { logger.error("Failed to update task: " + taskId, e); }));
    }

    /**
     * Update AD task for specific fields.
     *
     * @param taskId task id
     * @param updatedFields updated fields, key: filed name, value: new value
     * @param listener action listener
     */
    public void updateADTask(String taskId, Map<String, Object> updatedFields, ActionListener<UpdateResponse> listener) {
        UpdateRequest updateRequest = new UpdateRequest(CommonName.DETECTION_STATE_INDEX, taskId);
        Map<String, Object> updatedContent = new HashMap<>();
        updatedContent.putAll(updatedFields);
        updatedContent.put(LAST_UPDATE_TIME_FIELD, Instant.now().toEpochMilli());
        updateRequest.doc(updatedContent);
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client
            .update(
                updateRequest,
                ActionListener.wrap(response -> listener.onResponse(response), exception -> listener.onFailure(exception))
            );
    }

    /**
     * Cancel running task by detector id.
     *
     * @param detectorId detector id
     * @param reason reason to cancel AD task
     * @param userName which user cancel the AD task
     * @return AD task cancellation state
     */
    public ADTaskCancellationState cancelLocalTaskByDetectorId(String detectorId, String reason, String userName) {
        return adTaskCacheManager.cancelByDetectorId(detectorId, reason, userName);
    }

}
