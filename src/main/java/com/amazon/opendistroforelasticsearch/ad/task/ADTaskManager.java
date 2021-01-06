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
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
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

import com.amazon.opendistroforelasticsearch.ad.common.exception.ADTaskCancelledException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
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
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ADCancelTaskAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADCancelTaskNodeResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ADCancelTaskRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileNodeResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyDetectorJobResponse;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.google.common.collect.ImmutableMap;

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

    private volatile Integer maxAdTaskDocsPerDetector;
    private volatile Integer pieceIntervalSeconds;

    public ADTaskManager(
        Settings settings,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry,
        AnomalyDetectionIndices detectionIndices,
        DiscoveryNodeFilterer nodeFilter,
        ADTaskCacheManager adTaskCacheManager
    ) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.detectionIndices = detectionIndices;
        this.nodeFilter = nodeFilter;
        this.clusterService = clusterService;
        this.adTaskCacheManager = adTaskCacheManager;

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
    public void startDetector(
        String detectorId,
        IndexAnomalyDetectorJobActionHandler handler,
        User user,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        getDetector(
            detectorId,
            (detector) -> handler.startAnomalyDetectorJob(detector), // run realtime detector
            (detector) -> createADTaskIndex(detector, user, listener), // run historical detector
            listener
        );
    }

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
            (detector) -> getLatestADTask(detectorId, (task) -> stopTask(detectorId, task, user, listener), listener),
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
                String message = "Failed to start anomaly detector";
                logger.error(message, e);
                listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> listener.onFailure(exception)));
    }

    private void stopTask(String detectorId, Optional<ADTask> adTask, User user, ActionListener<AnomalyDetectorJobResponse> listener) {
        if (!adTask.isPresent()) {
            listener.onFailure(new ResourceNotFoundException(detectorId, "Detector not started"));
            return;
        }

        // no need to stop task if its state is one of end states
        if (isADTaskEnded(adTask.get())) {
            // TODO: tune error messages when intgrate with frontend
            listener.onFailure(new ResourceNotFoundException(detectorId, "No running task found"));
            return;
        }
        String taskId = adTask.get().getTaskId();

        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        String userName = user == null ? null : user.getName();
        ADCancelTaskRequest cancelTaskRequest = new ADCancelTaskRequest(taskId, userName, dataNodes);
        client.execute(ADCancelTaskAction.INSTANCE, cancelTaskRequest, ActionListener.wrap(response -> {
            Set<ADTaskCancellationState> states = response
                .getNodes()
                .stream()
                .filter(r -> r.getState() != null)
                .map(ADCancelTaskNodeResponse::getState)
                .collect(Collectors.toSet());
            if (states.contains(ADTaskCancellationState.CANCELLED) || states.contains(ADTaskCancellationState.CANCELLED)) {
                listener.onResponse(new AnomalyDetectorJobResponse(taskId, 0, 0, 0, RestStatus.OK));
            } else if (states.contains(ADTaskCancellationState.NOT_FOUND)) {
                // If ES process stopped, the running task may still stay on RUNNING state after restart, for this case
                // the cancel result will return not found, and we should reset task state as STOPPED.
                updateADTask(
                    taskId,
                    ImmutableMap.of(STATE_FIELD, ADTaskState.STOPPED),
                    ActionListener
                        .wrap(
                            r -> { listener.onResponse(new AnomalyDetectorJobResponse(taskId, 0, 0, 0, RestStatus.NOT_FOUND)); },
                            e -> listener.onFailure(e)
                        )
                );
            }
        }, e -> { listener.onFailure(e); }));
    }

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
            if (totalTasks == 1) {
                SearchHit searchHit = r.getHits().getAt(0);
                try (
                    XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    ADTask adTask = ADTask.parse(parser, searchHit.getId());

                    if (!isADTaskEnded(adTask) && lastUpdateTimeExpired(adTask)) {
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
                    String message = "Failed to parse AD task " + detectorId;
                    logger.error(message, e);
                    listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
                }
            } else if (totalTasks < 1) {
                function.accept(Optional.empty());
                // listener.onFailure(new ResourceNotFoundException(detectorId, "No latest AD task found"));
            } else {
                // TODO: handle multiple running lastest task. Iterate and cancel all of them
                listener.onFailure(new ElasticsearchStatusException("Multiple", RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.accept(Optional.empty());
            } else {
                listener.onFailure(e);
            }
        }));
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

    public void getADTaskProfile(String detectorId, ActionListener<DetectorProfile> listener) {
        getLatestADTask(detectorId, adTask -> {
            if (adTask.isPresent()) {
                getADTaskProfile(adTask.get(), ActionListener.wrap(adTaskProfile -> {
                    DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
                    profileBuilder.adTaskProfile(adTaskProfile);
                    listener.onResponse(profileBuilder.build());
                }, e -> listener.onFailure(e)));
            } else {
                listener.onFailure(new ResourceNotFoundException(detectorId, "Can't find task for detector"));
            }
        }, listener);
    }

    private void getADTaskProfile(ADTask adTask, ActionListener<ADTaskProfile> listener) {
        String taskId = adTask.getTaskId();

        if (adTaskCacheManager.contains(taskId)) {
            ADTaskProfile adTaskProfile = getLocalADTaskProfile(taskId, adTask);
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
                    listener.onFailure(new InternalFailure(adTask.getDetectorId(), "Multiple tasks running"));
                } else if (nodeResponses.size() > 0) {
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
                } else {
                    ADTaskProfile adTaskProfile = new ADTaskProfile(adTask, null, null, null, null, null);
                    listener.onResponse(adTaskProfile);
                }
            }, e -> { listener.onFailure(e); }));
        }
    }

    public ADTaskProfile getLocalADTaskProfile(String taskId) {
        return getLocalADTaskProfile(taskId, (ADTask) null);
    }

    private ADTaskProfile getLocalADTaskProfile(String taskId, ADTask adTask) {
        ADTaskProfile adTaskProfile = null;
        if (adTaskCacheManager.contains(taskId)) {
            adTaskProfile = new ADTaskProfile(
                adTask,
                adTaskCacheManager.getShingle(taskId) == null ? 0 : adTaskCacheManager.getShingle(taskId).size(),
                adTaskCacheManager.getRcfModel(taskId) == null ? 0 : adTaskCacheManager.getRcfModel(taskId).getTotalUpdates(),
                adTaskCacheManager.isThresholdModelTrained(taskId),
                adTaskCacheManager.getThresholdModelTrainingData(taskId) == null
                    ? 0
                    : adTaskCacheManager.getThresholdModelTrainingData(taskId).length,
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

    protected void createADTaskIndex(AnomalyDetector detector, User user, ActionListener<AnomalyDetectorJobResponse> listener) {
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
                listener.onFailure(new ElasticsearchStatusException("Detector is already running", RestStatus.BAD_REQUEST));
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
        String s = sourceBuilder.toString();
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
                logger.info("Updated AD task successfully: {}", response.status());
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

    public ADTaskCancellationState cancelTask(String taskId, String reason, String userName) {
        if (!adTaskCacheManager.contains(taskId)) {
            return ADTaskCancellationState.NOT_FOUND;
        }
        if (adTaskCacheManager.isCancelled(taskId)) {
            return ADTaskCancellationState.ALREADY_CANCELLED;
        }
        adTaskCacheManager.cancel(taskId, reason, userName);
        return ADTaskCancellationState.CANCELLED;
    }

}
