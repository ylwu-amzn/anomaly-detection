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
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.IS_LATEST_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.LAST_UPDATE_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STATE_FIELD;
import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.action.DocWriteResponse.Result.UPDATED;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.function.ADTaskFunction;
import com.amazon.opendistroforelasticsearch.ad.function.AnomalyDetectorFunction;
import com.amazon.opendistroforelasticsearch.ad.function.DetectorFunction;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyDetectorJobResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.DetectionStateHandler;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

public class ADTaskManager {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private static final String TASK_ID_HEADER = "anomaly_detection_task_id";

    private final ThreadPool threadPool;
    private final Client client;
    private final ADStats adStats;
    private final NamedXContentRegistry xContentRegistry;
    private final DiscoveryNodeFilterer nodeFilter;
    // TODO: limit running tasks
    private final ClusterService clusterService;
    private final DetectionStateHandler detectorStateHandler;
    private final AnomalyDetectionIndices detectionIndices;

    public ADTaskManager(
        ThreadPool threadPool,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeFilterer nodeFilter,
        AnomalyDetectionIndices detectionIndices,
        DetectionStateHandler detectorStateHandler,
        ADStats adStats
    ) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.nodeFilter = nodeFilter;
        this.detectionIndices = detectionIndices;
        this.detectorStateHandler = detectorStateHandler;
        this.adStats = adStats;
    }

    public void startDetector(
        String detectorId,
        IndexAnomalyDetectorJobActionHandler handler,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        getDetector(
            detectorId,
            (detector) -> handler.startAnomalyDetectorJob(detector),
            (detector) -> createADTaskIndex(detector, listener),
            listener
        );
    }

    public void stopDetector(
        String detectorId,
        IndexAnomalyDetectorJobActionHandler handler,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        getDetector(
            detectorId,
            (detector) -> handler.stopAnomalyDetectorJob(detectorId),
            (detector) -> getLatestADTask(detectorId, (task) -> stopTask(task, listener), listener),
            listener
        );
    }

    public void getDetector(
        String detectorId,
        DetectorFunction realTimeDetectorFunction,
        DetectorFunction historicalDetectorFunction,
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

                if (detector.isRealTimeDetector()) {
                    // create schedule job for realtime detector
                    realTimeDetectorFunction.execute(detector);
                } else {
                    // execute historical detector
                    historicalDetectorFunction.execute(detector);
                }
            } catch (Exception e) {
                String message = "Failed to parse anomaly detector job " + detectorId;
                logger.error(message, e);
                listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> listener.onFailure(exception)));
    }

    public void getLatestADTask(String detectorId, ADTaskFunction function, ActionListener listener) {
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
                    function.execute(adTask);
                } catch (Exception e) {
                    String message = "Failed to parse AD task " + detectorId;
                    logger.error(message, e);
                    listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
                }
            } else if (totalTasks < 1) {
                listener.onFailure(new ResourceNotFoundException(detectorId, "No latest AD task found"));
            } else {
                // TODO: handle multiple running lastest task. Iterate and cancel all of them
                listener.onFailure(new ElasticsearchStatusException("Multiple", RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.execute(null);
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private void stopTask(ADTask task, ActionListener<AnomalyDetectorJobResponse> listener) {
        assert task != null;
        String taskId = task.getTaskId();
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions("*detector/batch_run*");
        client.execute(ListTasksAction.INSTANCE, listTasksRequest, ActionListener.wrap(res -> {
            logger.info("AD batch tasks: {}", res.getPerNodeTasks());
            List<TaskInfo> tasks = res.getTasks();
            List<TaskInfo> infos = tasks
                .stream()
                .filter(
                    taskInfo -> StringUtils.equals(taskInfo.getHeaders().get(Task.X_OPAQUE_ID), getADTaskopaqueId(taskId))
                        && taskInfo.isCancellable()
                )
                .collect(Collectors.toList());
            if (infos.size() > 0) {
                logger.info("Found {} tasks for taskId {}", infos.size(), taskId);
                infos.forEach(info -> {
                    CancelTasksRequest cancelTaskRequest = new CancelTasksRequest();
                    cancelTaskRequest.setTaskId(infos.get(0).getTaskId());
                    client.execute(CancelTasksAction.INSTANCE, cancelTaskRequest, ActionListener.wrap(r -> {
                        logger.info("Finished to cancel task {}", infos.get(0));
                        listener.onResponse(new AnomalyDetectorJobResponse(taskId, 0, 0, 0, RestStatus.OK));
                        // channel.sendResponse(new BytesRestResponse(RestStatus.OK, "Task cancelled successfully"));
                    }, exception -> {
                        logger.error("Failed to cancel task " + taskId, exception);
                        listener.onFailure(exception);
                    }));
                });
            } else {
                // listener.onResponse("Task is not running");
                Map<String, Object> updatedFields = new HashMap<>();
                updatedFields.put(STATE_FIELD, ADTaskState.STOPPED.name());
                updateADTask(task.getTaskId(), updatedFields);
                listener.onFailure(new ResourceNotFoundException(task.getDetectorId(), "Task is not running"));
            }
        }, exception -> {
            logger.error("Fail to stop task " + taskId, exception);
            listener.onFailure(exception);
        }));
    }

    private void createADTaskIndex(AnomalyDetector detector, ActionListener<AnomalyDetectorJobResponse> listener) {
        if (detectionIndices.doesDetectorStateIndexExist()) {
            checkCurrentTaskState(detector, listener);
        } else {
            detectionIndices.initDetectionStateIndex(ActionListener.wrap(r -> {
                if (r.isAcknowledged()) {
                    logger.info("Created {} with mappings.", ADTask.DETECTOR_STATE_INDEX);
                    executeHistoricalDetector(detector, listener);
                } else {
                    logger.warn("Created {} with mappings call not acknowledged.", ADTask.DETECTOR_STATE_INDEX);
                    listener
                        .onFailure(
                            new ElasticsearchStatusException(
                                "Created " + ADTask.DETECTOR_STATE_INDEX + "with mappings call not acknowledged.",
                                RestStatus.INTERNAL_SERVER_ERROR
                            )
                        );
                }
            }, e -> { listener.onFailure(e); }));
        }
    }

    private void checkCurrentTaskState(AnomalyDetector detector, ActionListener<AnomalyDetectorJobResponse> listener) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detector.getDetectorId()));
        query.filter(new TermQueryBuilder(STATE_FIELD, ADTaskState.RUNNING.name()));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);
        searchRequest.indices(ADTask.DETECTOR_STATE_INDEX);
        client.search(searchRequest, ActionListener.wrap(r -> {
            if (r.getHits().getTotalHits().value > 0) {
                listener.onFailure(new ElasticsearchStatusException("Detector is already running", RestStatus.BAD_REQUEST));
            } else {
                executeHistoricalDetector(detector, listener);
            }
        }, e -> listener.onFailure(e)));

    }

    public void executeHistoricalDetector(AnomalyDetector detector, ActionListener<AnomalyDetectorJobResponse> listener) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(ADTask.DETECTOR_STATE_INDEX);
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detector.getDetectorId()));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        updateByQueryRequest.setQuery(query);
        updateByQueryRequest.setRefresh(true);
        updateByQueryRequest.setScript(new Script("ctx._source.is_latest = false;"));

        client
            .execute(
                UpdateByQueryAction.INSTANCE,
                updateByQueryRequest,
                ActionListener
                    .wrap(
                        r -> { createNewADTask(detector, listener); },
                        e -> {
                            // Not check IndexNotFoundException here as we have created index before this line
                            listener.onFailure(e);
                        }
                    )
            );
    }

    private void createNewADTask(AnomalyDetector detector, ActionListener<AnomalyDetectorJobResponse> listener) {
        ADTask task = new ADTask.Builder()
            .detectorId(detector.getDetectorId())
            .detector(detector)
            .isLatest(true)
            .taskType(ADTaskType.HISTORICAL.name())
            .executionStartTime(Instant.now())
            .progress(0.0f)
            .state(ADTaskState.INIT.name())
            .lastUpdateTime(Instant.now())
            .build();

        IndexRequest request = new IndexRequest(ADTask.DETECTOR_STATE_INDEX);
        try (final XContentBuilder builder = XContentFactory.jsonBuilder()) {
            request.source(task.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            client.index(request, ActionListener.wrap(r -> onIndexADTaskResponse(r, task, null, listener), e -> listener.onFailure(e)));
        } catch (Exception e) {
            logger.error("fail to start a new task for " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }
    }

    private void onIndexADTaskResponse(
        IndexResponse response,
        ADTask task,
        AnomalyDetectorFunction function,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        if (response == null || (response.getResult() != CREATED && response.getResult() != UPDATED)) {
            String errorMsg = checkShardsFailure(response);
            listener.onFailure(new ElasticsearchStatusException(errorMsg, response.status()));
            return;
        }
        if (function != null) {
            function.execute();
        } else {
            AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                response.getId(),
                response.getVersion(),
                response.getSeqNo(),
                response.getPrimaryTerm(),
                RestStatus.OK
            );
            task.setTaskId(response.getId());
            listener.onResponse(anomalyDetectorJobResponse);
            try (ThreadContext.StoredContext context = threadPool.getThreadContext().stashContext()) {
                assert context != null;
                threadPool.getThreadContext().putHeader(Task.X_OPAQUE_ID, getADTaskopaqueId(task.getTaskId()));

                client
                    .execute(
                        ADBatchAnomalyResultAction.INSTANCE,
                        new ADBatchAnomalyResultRequest(task),
                        ActionListener
                            .wrap(
                                r -> logger.info("Task execution finished for {}, response: {}", task.getTaskId(), r.getMessage()),
                                exception -> handleADTaskException(task, exception)
                            )
                    );
            } catch (Exception e) {
                handleADTaskException(task, e);
                listener.onFailure(e);
            }
        }
    }

    private String getADTaskopaqueId(String taskId) {
        return TASK_ID_HEADER + ":" + taskId;
    }

    // private void handleADTaskException(ADTask task, Exception exception) {
    // logger.error("Fail to execute batch task action " + task.getTaskId(), exception);
    // String state = ADTaskState.FAILED.name();
    // Map<String, Object> updatedFields = new HashMap<>();
    // if (exception instanceof TaskCancelledException) {
    // state = ADTaskState.STOPPED.name();
    // } else {
    // updatedFields.put(ERROR_FIELD, ExceptionUtils.getFullStackTrace(exception));
    // }
    // updatedFields.put(STATE_FIELD, state);
    // updatedFields.put(EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli());
    // updateADTask(task.getTaskId(), updatedFields);
    // }
    public void handleADTaskException(ADTask task, Exception exception) {
        // remove task execution from map if execution fails
        String state = ADTaskState.FAILED.name();
        Map<String, Object> updatedFields = new HashMap<>();
        if (exception instanceof TaskCancelledException) {
            logger.error("AD task cancelled: " + task.getTaskId());
            state = ADTaskState.STOPPED.name();
        } else {
            logger.error("Fail to execute batch task action " + task.getTaskId(), exception);
            updatedFields.put(ERROR_FIELD, ExceptionUtils.getFullStackTrace(exception));
        }
        updatedFields.put(STATE_FIELD, state);
        updatedFields.put(EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli());
        updateADTask(task.getTaskId(), updatedFields);
    }

    public void updateADTask(String taskId, Map<String, Object> updatedFields) {
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
        client
            .update(
                updateRequest,
                ActionListener.wrap(response -> listener.onResponse(response), exception -> listener.onFailure(exception))
            );
    }

    private String checkShardsFailure(IndexResponse response) {
        StringBuilder failureReasons = new StringBuilder();
        if (response.getShardInfo().getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : response.getShardInfo().getFailures()) {
                failureReasons.append(failure);
            }
            return failureReasons.toString();
        }
        return null;
    }

}
