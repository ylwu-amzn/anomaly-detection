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

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.function.AnomalyDetectorFunction;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskProfile;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileNodeResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyDetectorJobResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.DetectionStateHandler;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
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
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
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
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.DETECTOR_ID_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.ERROR_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.EXECUTION_END_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.IS_LATEST_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.LAST_UPDATE_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STATE_FIELD;
import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.action.DocWriteResponse.Result.UPDATED;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class ADTaskManager {
    private final Logger logger = LogManager.getLogger(this.getClass());
    public static final String AD_TASK_ID_HEADER = "odfe_anomaly_detection_task_id";

    private final ThreadPool threadPool;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final DiscoveryNodeFilterer nodeFilter;
    // TODO: limit running tasks
    private final ClusterService clusterService;
    private final DetectionStateHandler detectorStateHandler;
    private final AnomalyDetectionIndices detectionIndices;
    private final ADBatchTaskCache adBatchTaskCache;

    public ADTaskManager(
        ThreadPool threadPool,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeFilterer nodeFilter,
        AnomalyDetectionIndices detectionIndices,
        DetectionStateHandler detectorStateHandler,
        ADBatchTaskCache adBatchTaskCache
    ) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.nodeFilter = nodeFilter;
        this.detectionIndices = detectionIndices;
        this.detectorStateHandler = detectorStateHandler;
        this.adBatchTaskCache = adBatchTaskCache;
    }

    public void startDetector(
        String detectorId,
        IndexAnomalyDetectorJobActionHandler handler,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        getDetector(
            detectorId,
            (detector) -> handler.startAnomalyDetectorJob(detector), // realtime detector
            (detector) -> createADTaskIndex(detector, listener), // historical detector
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
            (detector) -> getLatestADTask(detectorId, (task) -> stopTask(detectorId, task, listener), listener),
            listener
        );
    }

    public void getDetector(
        String detectorId,
        Consumer<AnomalyDetector> realTimeDetectorFunction,
        Consumer<AnomalyDetector> historicalDetectorFunction,
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
                    realTimeDetectorFunction.accept(detector);
                } else {
                    // execute historical detector
                    historicalDetectorFunction.accept(detector);
                }
            } catch (IOException e) {
                String message = "Failed to parse anomaly detector job";
                logger.error(message, e);
                listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            } catch (LimitExceededException e) {
                logger.error(e.getMessage(), e);
                listener.onFailure(new ElasticsearchStatusException(e.getMessage(), RestStatus.TOO_MANY_REQUESTS));
            } catch (Exception e) {
                String message = "Failed to start anomaly detector job";
                logger.error(message, e);
                listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> listener.onFailure(exception)));
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
                    getRunningEsTask(adTask, taskInfos -> {
                        if (taskInfos.size() > 0) {
                            adTask.setState(ADTaskState.RUNNING.name());
                            function.accept(Optional.of(adTask));
                        } else {
                            if (isADTaskRunning(adTask)) {
                                resetTaskStateAsStopped(adTask);
                                adTask.setState(ADTaskState.STOPPED.name());
                            }
                            function.accept(Optional.of(adTask));
                        }
                    }, listener);
                } catch (Exception e) {
                    String message = "Failed to parse AD task " + detectorId;
                    logger.error(message, e);
                    listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
                }
            } else if (totalTasks < 1) {
                function.accept(Optional.empty());
//                listener.onFailure(new ResourceNotFoundException(detectorId, "No latest AD task found"));
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

    private void getRunningEsTask(ADTask adTask, Consumer<List<TaskInfo>> consumer, ActionListener listener) {
        String taskId = adTask.getTaskId();
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions("*detector/batch_run*");
        client.execute(ListTasksAction.INSTANCE, listTasksRequest, ActionListener.wrap(res -> {
            logger.info("AD batch tasks: {}", res.getPerNodeTasks());
            List<TaskInfo> tasks = res.getTasks();
            List<TaskInfo> taskInfos = tasks
                    .stream()
                    .filter(
                            taskInfo -> StringUtils.equals(taskInfo.getHeaders().get(AD_TASK_ID_HEADER), taskId)
                                    && taskInfo.isCancellable()
                    )
                    .collect(Collectors.toList());
            logger.info("Found {} running ES tasks for AD task {}", taskInfos.size(), taskId);
            consumer.accept(taskInfos);
//            if (infos.size() == 0 && !isADTaskEnded(adTask)) {
//                Map<String, Object> updatedFields = new HashMap<>();
//                updatedFields.put(STATE_FIELD, ADTaskState.STOPPED.name());
//                updateADTask(taskId, updatedFields);
//                listener.onFailure(new ResourceNotFoundException(adTask.getDetectorId(), "Task is not running"));
//            }
//            if (infos.size() > 0) {
//                consumer.accept(infos);
//            } else {
//                Map<String, Object> updatedFields = new HashMap<>();
//                updatedFields.put(STATE_FIELD, ADTaskState.STOPPED.name());
//                updateADTask(taskId, updatedFields);
//                listener.onFailure(new ResourceNotFoundException(adTask.getDetectorId(), "Task is not running"));
//            }
        }, exception -> {
            logger.error("Fail to stop AD task " + taskId, exception);
            listener.onFailure(exception);
        }));
    }

    // TODO: configure the max waiting time based on Ratelimtier maximum setting.
    public boolean isADTaskRunning(ADTask adTask) {
        return ADTaskState.INIT.name().equals(adTask.getState()) || ADTaskState.RUNNING.name().equals(adTask.getState())
                || (ADTaskState.CREATED.name().equals(adTask.getState())
                && adTask.getLastUpdateTime().plus(60, ChronoUnit.SECONDS).isBefore(Instant.now()));
    }

    private boolean isADTaskEnded(ADTask adTask) {
        return ADTaskState.STOPPED.name().equals(adTask.getState()) || ADTaskState.FINISHED.name().equals(adTask.getState())
                || ADTaskState.FAILED.name().equals(adTask.getState());
    }

    private void resetTaskStateAsStopped(ADTask adTask) {
        if (!isADTaskEnded(adTask)) {
            Map<String, Object> updatedFields = new HashMap<>();
            updatedFields.put(STATE_FIELD, ADTaskState.STOPPED.name());
            updateADTask(adTask.getTaskId(), updatedFields);
        }
    }

    private void stopTask(String detectorId, Optional<ADTask> adTask, ActionListener<AnomalyDetectorJobResponse> listener) {
        if(!adTask.isPresent()) {
            listener.onFailure(new ResourceNotFoundException(detectorId, "Detector has never been started"));
        }
        String taskId = adTask.get().getTaskId();
        getRunningEsTask(adTask.get(), (taskInfos) -> {
            if (taskInfos.size() > 0) {
                AtomicInteger count = new AtomicInteger(0);
                taskInfos.forEach(taskInfo -> {
                    CancelTasksRequest cancelTaskRequest = new CancelTasksRequest();
                    cancelTaskRequest.setTaskId(taskInfo.getTaskId());
                    client.execute(CancelTasksAction.INSTANCE, cancelTaskRequest, ActionListener.wrap(r -> {
                        logger.info("Finished to cancel task {}", taskInfo);
                        int cancelledTasks = count.incrementAndGet();
                        if (cancelledTasks == taskInfos.size()) {
                            listener.onResponse(new AnomalyDetectorJobResponse(taskId, 0, 0, 0, RestStatus.OK));
                        }
                    }, exception -> {
                        logger.error("Failed to cancel task " + taskId, exception);
                        listener.onFailure(exception);
                    }));
                });
            } else {
                listener.onFailure(new ResourceNotFoundException(adTask.get().getDetectorId(), "Task is not running"));
                resetTaskStateAsStopped(adTask.get());
            }

        }, listener);
//        String taskId = task.get().getTaskId();
//        ListTasksRequest listTasksRequest = new ListTasksRequest();
//        listTasksRequest.setActions("*detector/batch_run*");
//        client.execute(ListTasksAction.INSTANCE, listTasksRequest, ActionListener.wrap(res -> {
//            logger.info("AD batch tasks: {}", res.getPerNodeTasks());
//            List<TaskInfo> tasks = res.getTasks();
//            List<TaskInfo> infos = tasks
//                .stream()
//                .filter(
//                    taskInfo -> StringUtils.equals(taskInfo.getHeaders().get(Task.X_OPAQUE_ID), getADTaskOpaqueId(taskId))
//                        && taskInfo.isCancellable()
//                )
//                .collect(Collectors.toList());
//            if (infos.size() > 0) {
//                logger.info("Found {} tasks for taskId {}", infos.size(), taskId);
//                infos.forEach(info -> {
//                    CancelTasksRequest cancelTaskRequest = new CancelTasksRequest();
//                    cancelTaskRequest.setTaskId(infos.get(0).getTaskId());
//                    client.execute(CancelTasksAction.INSTANCE, cancelTaskRequest, ActionListener.wrap(r -> {
//                        logger.info("Finished to cancel task {}", infos.get(0));
//                        listener.onResponse(new AnomalyDetectorJobResponse(taskId, 0, 0, 0, RestStatus.OK));
//                        // channel.sendResponse(new BytesRestResponse(RestStatus.OK, "Task cancelled successfully"));
//                    }, exception -> {
//                        logger.error("Failed to cancel task " + taskId, exception);
//                        listener.onFailure(exception);
//                    }));
//                });
//            } else {
//                // listener.onResponse("Task is not running");
//                Map<String, Object> updatedFields = new HashMap<>();
//                updatedFields.put(STATE_FIELD, ADTaskState.STOPPED.name());
//                updateADTask(taskId, updatedFields);
//                listener.onFailure(new ResourceNotFoundException(task.get().getDetectorId(), "Task is not running"));
//            }
//        }, exception -> {
//            logger.error("Fail to stop task " + taskId, exception);
//            listener.onFailure(exception);
//        }));
    }

    private void createADTaskIndex(AnomalyDetector detector, ActionListener<AnomalyDetectorJobResponse> listener) {
        if (adBatchTaskCache.containsTaskOfDetector(detector.getDetectorId())){
            listener.onFailure(new ElasticsearchStatusException("Detector is already running", RestStatus.BAD_REQUEST));
            return;
        }
        adBatchTaskCache.allowToPutNewTask();
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
        query.filter(new TermsQueryBuilder(STATE_FIELD, ADTaskState.CREATED.name(), ADTaskState.INIT.name(),ADTaskState.RUNNING.name()));
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
                        r -> createNewADTask(detector, listener),
                        e -> {
                            // Not check IndexNotFoundException here as we have created index before this line
                            listener.onFailure(e);
                        }
                    )
            );
    }

    private void createNewADTask(AnomalyDetector detector, ActionListener<AnomalyDetectorJobResponse> listener) {
        ADTask adTask = new ADTask.Builder()
            .detectorId(detector.getDetectorId())
            .detector(detector)
            .isLatest(true)
            .taskType(ADTaskType.HISTORICAL.name())
            .executionStartTime(Instant.now())
            .taskProgress(0.0f)
            .state(ADTaskState.CREATED.name())
            .lastUpdateTime(Instant.now())
            .build();

        IndexRequest request = new IndexRequest(ADTask.DETECTOR_STATE_INDEX);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            request.source(adTask.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            client.index(request, ActionListener.wrap(r -> onIndexADTaskResponse(r, adTask, null, listener), e -> listener.onFailure(e)));
        } catch (Exception e) {
            logger.error("fail to start a new task for " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }
    }

    private void onIndexADTaskResponse(
        IndexResponse response,
        ADTask adTask,
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
            adTask.setTaskId(response.getId());
            listener.onResponse(anomalyDetectorJobResponse);
            // check if task exceeds limitation, if yes, return error to user directly
//            adBatchTaskCache.put(adTask);
            client
                    .execute(
                            ADBatchAnomalyResultAction.INSTANCE,
                            new ADBatchAnomalyResultRequest(adTask),
                            ActionListener
                                    .wrap(
                                            r -> {
                                                logger.info(r.getMessage());
                                            },
                                            exception -> {
                                                handleADTaskException(adTask, exception);
                                            }
                                    )
                    );
        }
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
    public void handleADTaskException(ADTask adTask, Exception exception) {
        // remove task execution from map if execution fails
        String state = ADTaskState.FAILED.name();
        Map<String, Object> updatedFields = new HashMap<>();
        if (exception instanceof ReceiveTimeoutTransportException) {
            //TODO: handle timeout exception
            logger.error("Timeout to execute AD task", exception);
            updatedFields.put(ERROR_FIELD, exception.getMessage());
        } else if (exception instanceof TaskCancelledException) {
            logger.error("AD task cancelled: " + adTask.getTaskId());
            state = ADTaskState.STOPPED.name();
        } else {
            logger.error("Fail to execute batch task action " + adTask.getTaskId(), exception);
            String error = (exception instanceof IllegalArgumentException
                    || exception instanceof AnomalyDetectionException) ?
                    exception.getMessage() : ExceptionUtils.getFullStackTrace(exception);
            updatedFields.put(ERROR_FIELD, error);
        }
        updatedFields.put(STATE_FIELD, state);
        updatedFields.put(EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli());
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

//    public void updateADTask(String taskId, Map<String, Object> updatedFields, Consumer<UpdateResponse> consumer,
//                             ActionListener listener) {
//        UpdateRequest updateRequest = new UpdateRequest(ADTask.DETECTOR_STATE_INDEX, taskId);
//        Map<String, Object> updatedContent = new HashMap<>();
//        updatedContent.putAll(updatedFields);
//        updatedContent.put(LAST_UPDATE_TIME_FIELD, Instant.now().toEpochMilli());
//        updateRequest.doc(updatedContent);
//        client
//                .update(
//                        updateRequest,
//                        ActionListener.wrap(response -> {
//                            if (response.status() == RestStatus.OK) {
//                                consumer.accept(response);
//                            } else {
//                                listener.onFailure(new ElasticsearchStatusException("Fail to update AD task state",
//                                        response.status()));
//                            }
//                        }, exception -> listener.onFailure(exception))
//                );
//    }

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

    public void deleteADTasks(String detectorId, Consumer consumer, ActionListener<DeleteResponse> listener) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(ADTask.DETECTOR_STATE_INDEX);

        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));

        request.setQuery(query);
        client.execute(DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(
                r -> {
                    logger.info("AD tasks deleted for detector {}", detectorId);
                    consumer.accept(r);
                }, e -> listener.onFailure(e)
        ));
    }

    public void getTaskProfile(String detectorId, ActionListener<DetectorProfile> listener) {
        getLatestADTask(detectorId, adTask -> {
            if (adTask.isPresent()) {
                String taskId = adTask.get().getTaskId();

                if (adBatchTaskCache.contains(taskId)) {
                    ADTaskProfile adTaskProfile = getTaskRuntimeInfo(taskId, adTask.get());
                    returnDetectorProfile(adTaskProfile, listener);
                } else {
                    DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
                    ADTaskProfileRequest adTaskProfileRequest = new ADTaskProfileRequest(taskId, dataNodes);
                    client.execute(ADTaskProfileAction.INSTANCE, adTaskProfileRequest,
                            ActionListener.wrap(response-> {
                                List<ADTaskProfile> nodeResponses = response.getNodes().stream()
                                        .filter(r -> r.getAdTaskProfile() != null)
                                        .map(ADTaskProfileNodeResponse::getAdTaskProfile)
                                        .collect(Collectors.toList());
                                if (nodeResponses.size() > 1) {
                                    listener.onFailure(new InternalFailure(detectorId, "Multiple tasks running"));
                                } else if (nodeResponses.size() > 0) {
                                    ADTaskProfile nodeResponse = nodeResponses.get(0);
                                    ADTaskProfile adTaskProfile = new ADTaskProfile(
                                            adTask.get(),
                                            nodeResponse.getShingleSize(),
                                            nodeResponse.getRcfTotalUpdates(),
                                            nodeResponse.getThresholdModelTrained(),
                                            nodeResponse.getThresholdNodelTrainingDataSize(),
                                            nodeResponse.getNodeId()
                                    );
                                    returnDetectorProfile(adTaskProfile, listener);
                                } else {
                                    ADTaskProfile adTaskProfile = new ADTaskProfile(adTask.get(), null, null, null, null, null);
                                    returnDetectorProfile(adTaskProfile, listener);
                                }
                            }, e -> {
                                listener.onFailure(e);
                            }));
                }


            } else {
                listener.onFailure(new ResourceNotFoundException(detectorId, "Can't find task for detector"));
            }
        }, listener);
    }

    private void returnDetectorProfile(ADTaskProfile adTaskProfile, ActionListener<DetectorProfile> listener) {
        DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
        profileBuilder.adTaskProfile(adTaskProfile);
        listener.onResponse(profileBuilder.build());
    }

    public ADTaskProfile getTaskRuntimeInfo(String taskId) {
        return getTaskRuntimeInfo(taskId,null);
    }

    private ADTaskProfile getTaskRuntimeInfo(String taskId, ADTask adTask) {
        ADTaskProfile adTaskProfile = null;
        if (adBatchTaskCache.contains(taskId)) {
            adTaskProfile = new ADTaskProfile(adTask,
                    adBatchTaskCache.get(taskId).getShingle() == null ? 0 : adBatchTaskCache.get(taskId).getShingle().size(),
                    adBatchTaskCache.get(taskId).getRcfModel() == null ? 0 : adBatchTaskCache.get(taskId).getRcfModel().getTotalUpdates(),
                    adBatchTaskCache.get(taskId).isThresholdModelTrained(),
                    adBatchTaskCache.get(taskId).getThresholdModelTrainingData() == null ? 0 : adBatchTaskCache.get(taskId).getThresholdModelTrainingData().size(),
                    clusterService.localNode().getId()
            );
        }
        return adTaskProfile;
    }
}
