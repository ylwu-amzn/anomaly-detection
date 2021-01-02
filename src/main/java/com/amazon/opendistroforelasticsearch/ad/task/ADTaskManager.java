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
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.IS_LATEST_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.LAST_UPDATE_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STATE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskProfile;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskState;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileNodeResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileRequest;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

public class ADTaskManager {
    private final Logger logger = LogManager.getLogger(this.getClass());

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final DiscoveryNodeFilterer nodeFilter;
    private final ClusterService clusterService;
    private final ADTaskCacheManager adTaskCacheManager;
    private volatile Integer pieceIntervalSeconds;

    public ADTaskManager(
        Settings settings,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeFilterer nodeFilter,
        ADTaskCacheManager adTaskCacheManager
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.nodeFilter = nodeFilter;
        this.adTaskCacheManager = adTaskCacheManager;

        this.pieceIntervalSeconds = BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCH_TASK_PIECE_INTERVAL_SECONDS, it -> pieceIntervalSeconds = it);
        this.pieceIntervalSeconds = BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings);
    }

    public void getLatestADTask(String detectorId, Consumer<Optional<ADTask>> function, ActionListener listener) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        searchRequest.indices(ADTask.DETECTOR_STATE_INDEX);

        client.search(searchRequest, ActionListener.wrap(r -> {
            long totalTasks = r.getHits().getTotalHits().value;
            if (totalTasks == 1) {
                SearchHit searchHit = r.getHits().getAt(0);
                try (
                    XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    ADTask adTask = ADTask.parse(parser, searchHit.getId());

                    if (isADTaskRunning(adTask) && lastUpdateTimeExpired(adTask)) {
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

    private void getADTaskProfile(ADTask adTask, ActionListener<ADTaskProfile> listener) {
        String taskId = adTask.getTaskId();

        if (adTaskCacheManager.contains(taskId)) {
            ADTaskProfile adTaskProfile = getTaskProfile(taskId, adTask);
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

    public ADTaskProfile getTaskProfile(String taskId) {
        return getTaskProfile(taskId, (ADTask) null);
    }

    private ADTaskProfile getTaskProfile(String taskId, ADTask adTask) {
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

    private void resetTaskStateAsStopped(ADTask adTask) {
        Map<String, Object> updatedFields = new HashMap<>();
        updatedFields.put(STATE_FIELD, ADTaskState.STOPPED.name());
        updateADTask(adTask.getTaskId(), updatedFields);
    }

    private void updateADTask(String taskId, Map<String, Object> updatedFields) {
        updateADTask(taskId, updatedFields, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                logger.info("Updated task execution result {}", response.status());
            } else {
                logger.error("Failed to update task execution {}, status: {}", taskId, response.status());
            }
        }, exception -> { logger.error("Failed to update task execution" + taskId, exception); }));
    }

    public void updateADTask(String taskId, Map<String, Object> updatedFields, ActionListener<UpdateResponse> listener) {
        UpdateRequest updateRequest = new UpdateRequest(ADTask.DETECTOR_STATE_INDEX, taskId);
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

    public boolean isADTaskRunning(ADTask adTask) {
        return ADTaskState.INIT.name().equals(adTask.getState())
            || ADTaskState.RUNNING.name().equals(adTask.getState())
            || ADTaskState.CREATED.name().equals(adTask.getState());
    }

    private boolean lastUpdateTimeExpired(ADTask adTask) {
        return adTask.getLastUpdateTime().plus(2 * pieceIntervalSeconds, ChronoUnit.SECONDS).isBefore(Instant.now());
    }

}
