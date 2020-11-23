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

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.AnomalyDetectorFunction;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyDetectionBatchTask;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyDetectorJobResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.BatchAnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.BatchAnomalyResultRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.BatchAnomalyResultResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.DetectionStateHandler;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;

import static com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin.AD_BATCh_TASK_THREAD_POOL_NAME;
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
    public static final String GET_TASK_RESPONSE = "getTaskResponse";
    private final Logger logger = LogManager.getLogger(this.getClass());
    private static final String TASK_ID_HEADER = "anomaly_detection_task_id";

    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final ThreadPool threadPool;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final Map<String, AnomalyDetectionBatchTask> adBatchTasks;
    private final DiscoveryNodeFilterer nodeFilter;
    // TODO: limit running tasks
    private final Integer MAX_RUNNING_TASK = 10;
    private final ClusterService clusterService;
    private final DetectionStateHandler detectorStateHandler;
    private final AnomalyDetectionIndices detectionIndices;

    public ADTaskManager(
            ThreadPool threadPool,
            ClusterService clusterService,
            Client client,
            AnomalyDetectionIndices anomalyDetectionIndices,
            NamedXContentRegistry xContentRegistry,
            DiscoveryNodeFilterer nodeFilter,
            AnomalyDetectionIndices detectionIndices, DetectionStateHandler detectorStateHandler) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.xContentRegistry = xContentRegistry;
        adBatchTasks = new ConcurrentHashMap<>();
        this.nodeFilter = nodeFilter;
        this.detectionIndices = detectionIndices;
        this.detectorStateHandler = detectorStateHandler;
    }

    public void startDetector(String detectorId, IndexAnomalyDetectorJobActionHandler handler,
                              ActionListener<AnomalyDetectorJobResponse> listener) {
        GetRequest getRequest = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).id(detectorId);
        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                listener.onFailure(new ElasticsearchStatusException("AnomalyDetector is not found with id: " + detectorId, RestStatus.NOT_FOUND));
                return;
            }
            try (XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());

                if (!detector.isRealTimeDetector()) {
                    // execute historical detector
                    createADTaskIndex(detector, listener);
                } else {
                    // create schedule job for realtime detector
                   handler.startAnomalyDetectorJob();
                }
            } catch (Exception e) {
                String message = "Failed to parse anomaly detector job " + detectorId;
                logger.error(message, e);
                listener.onFailure(new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> listener.onFailure(exception)));
    }

    public void createADTaskIndex(AnomalyDetector detector, ActionListener<AnomalyDetectorJobResponse> listener) throws IOException {
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
            }, e -> {
                listener.onFailure(e);
            }));
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
                listener.onFailure(new ElasticsearchStatusException("Detector is running, can't start a new run", RestStatus.BAD_REQUEST));
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

//        SearchRequest request = new SearchRequest();
//        SearchSourceBuilder sb = new SearchSourceBuilder();
//        sb.query(query);
//        String s = sb.toString();
//        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++");
//        System.out.println(s);
//        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++");
//        request.source(sb);
//        client.search(request, ActionListener.wrap(r -> {
//            SearchHit[] hits = r.getHits().getHits();
//            DocumentField is_latest = hits[0].field("is_latest");
//        }, e -> {
//            logger.error("aaa", e);
//        }));

        client.execute(UpdateByQueryAction.INSTANCE, updateByQueryRequest, ActionListener.wrap(r -> {
            BulkByScrollTask.Status status = r.getStatus();
            long updated = r.getUpdated();
            createNewADTask(detector, listener);
        }, e-> {
//            if (e instanceof IndexNotFoundException) {
//                createNewADTask(detector, listener);
//            } else {
//            }
            //Not check IndexNotFoundException here as we have created index
            listener.onFailure(e);
        }));
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
            client.index(request, ActionListener.wrap(r -> onIndexADTaskResponse(r, task,null, listener), e -> listener.onFailure(e)));
        } catch (Exception e) {
            logger.error("fail to start a new task for " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }
    }

    private void onIndexADTaskResponse(IndexResponse response, ADTask task, AnomalyDetectorFunction function,
                                       ActionListener<AnomalyDetectorJobResponse> listener) throws IOException {
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
            executeADTask(task, anomalyDetectorJobResponse, listener);
        }
    }

    private void executeADTask(ADTask task, AnomalyDetectorJobResponse response, ActionListener<AnomalyDetectorJobResponse> listener) {
        try {
            threadPool.executor(AD_BATCh_TASK_THREAD_POOL_NAME).submit(taskRunnable(task, task.getTaskId()));
            listener.onResponse(response);
        } catch (Exception e) {
            logger.error("Fail to start AD batch task " + task.getTaskId(), e);
            listener.onFailure(e);
            updateADTask(
                    task.getTaskId(),
                    ImmutableMap
                            .of(
                                    STATE_FIELD,
                                    ADTaskState.FAILED.name(),
                                    EXECUTION_END_TIME_FIELD,
                                    Instant.now(),
                                    ERROR_FIELD,
                                    ExceptionUtils.getFullStackTrace(e)
                            )
            );
        }
    }

    private Runnable taskRunnable(ADTask task, String taskExecutionId) {
         return () -> {
             executeADTaskLocally(task, taskExecutionId);
         };
//        return () -> {
//            getNodeTaskStats(taskExecution.getTaskId(), ActionListener.wrap(node -> {
//                if (clusterService.localNode().getId().equals(node.getId())) {
//                    executeTaskLocally(taskExecution, taskExecutionId, node.getId());
//                } else {
//                    executeTaskRemotely(taskExecution, taskExecutionId, node.getId());
//                }
//            }, exception -> { handleTaskExecutionException(taskExecution, taskExecutionId, exception); }));
//
//        };
    }

    private void executeADTaskLocally(ADTask task, String taskExecutionId) {
        try {
            BatchAnomalyResultRequest request = new BatchAnomalyResultRequest(
                    task
            );
            client.execute(BatchAnomalyResultAction.INSTANCE, request, taskExecutionListener(task));
        } catch (Exception e) {
            handleADTaskException(task, e);
        }
    }

    private ActionListener<BatchAnomalyResultResponse> taskExecutionListener(
            ADTask task
    ) {
        return ActionListener.wrap(response -> {
            logger.info("Task execution finished for {}, response: {}", task.getTaskId(), response.getMessage());
            // remove task execution from map when finish
            removeADTaskFromCache(task.getTaskId());
        }, exception -> { handleADTaskException(task, exception); });
    }

    private void handleADTaskException(ADTask task, Exception exception) {
        // remove task execution from map if execution fails
        logger.error("Fail to execute batch task action " + task.getTaskId(), exception);
        removeADTaskFromCache(task.getTaskId());
        String state = ADTaskState.FAILED.name();
        Map<String, Object> updatedFields = new HashMap<>();
        if (exception instanceof TaskCancelledException) {
            state = ADTaskState.STOPPED.name();
        } else {
            updatedFields.put(ERROR_FIELD, ExceptionUtils.getFullStackTrace(exception));
        }
        updatedFields.put(STATE_FIELD, state);
        updatedFields.put(EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli());
        updateADTask(task.getTaskId(), updatedFields);
    }

    public void removeADTaskFromCache(String taskId) {
        adBatchTasks.remove(taskId);
        // adStats.getStat(StatNames.AD_EXECUTING_TASK_COUNT.getName()).setValue((long) taskMap.size());
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

    public String checkLimitation() {
        if (adBatchTasks.size() >= MAX_RUNNING_TASK) {
            return "Can't run more than " + MAX_RUNNING_TASK + " per node";
        }
        return null;
    }

    public void isADTaskCancelled(String taskId) {
        if (adBatchTasks.containsKey(taskId) && adBatchTasks.get(taskId).isCancelled()) {
            removeADTaskFromCache(taskId);
            throw new TaskCancelledException("cancelled");
        }
    }

    /**
     * Start task. If task is already running, will not rerun.
     * @param detectorId detector id
     * @param listener action listener
     */
//    public void startTask(String detectorId, ActionListener<String> listener) {
//        GetRequest getTaskRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
//
//        // step1 get detector
//        client.get(getTaskRequest, ActionListener.wrap(response -> {
//            if (!response.isExists()) {
//                // TODO: add more exception types to support task id
//                listener.onFailure(new IllegalArgumentException("Anomaly detection task not found"));
//                return;
//            }
//            try (
//                    XContentParser parser = XContentType.JSON
//                            .xContent()
//                            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString())
//            ) {
//                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
//                AnomalyDetector detector = AnomalyDetectionTask.parse(parser, taskId);
//                // step2 check latest task execution state, if it's running, will not rerun.
//                getLatestTaskExecution(taskId, ActionListener.wrap(taskExecution -> {
//                            if (!AnomalyDetectionTaskState.RUNNING.name().equals(taskExecution.getState())) {
//                                prepareTask(taskId, task, listener);
//                            } else {
//                                listener.onFailure(new AnomalyDetectionException("Task is already running"));
//                            }
//                        },
//                        exception -> {
//                            // If task execution index not exist or task execution not found, we should start task
//                            if (exception instanceof IndexNotFoundException || exception instanceof ResourceNotFoundException) {
//                                prepareTask(taskId, task, listener);
//                            } else {
//                                log.error("Failed to get latest task execution", exception);
//                                listener.onFailure(exception);
//                            }
//                        }
//                ));
//            } catch (Exception e) {
//                log.error("Fail to start task " + taskId, e);
//                listener.onFailure(e);
//            }
//        }, exception -> {
//            log.error("Fail to get task " + taskId, exception);
//            listener.onFailure(exception);
//        }));
//    }
//
//    private void prepareTask(String taskId, AnomalyDetectionTask task, ActionListener<String> listener) {
//        try (ThreadContext.StoredContext context = threadPool.getThreadContext().stashContext()) {
//            assert context != null;
//            threadPool.getThreadContext().putHeader(Task.X_OPAQUE_ID, TASK_ID_HEADER + ":" + taskId);
//
//            if (!anomalyDetectionIndices.doesAnomalyDetectionTaskExecutionIndexExist()) {
//                anomalyDetectionIndices
//                        .initAnomalyDetectionTaskExecutionIndexDirectly(
//                                ActionListener.wrap(res -> onCreateTaskExecutionIndex(res, task, listener), e -> { listener.onFailure(e); })
//                        );
//            } else {
//                indexTaskExecution(task, listener);
//            }
//        } catch (Exception e) {
//            log.error("Failed to process the request for taskId: {}.", taskId);
//            listener.onFailure(new RuntimeException("Failed to start task"));
//        }
//    }
//
//    private void onCreateTaskExecutionIndex(CreateIndexResponse response, AnomalyDetectionTask task, ActionListener<String> listener)
//            throws IOException {
//        if (response.isAcknowledged()) {
//            indexTaskExecution(task, listener);
//        } else {
//            listener.onFailure(new RuntimeException("Create task execution index not acknowledged"));
//        }
//    }
//
//    private void indexTaskExecution(AnomalyDetectionTask task, ActionListener<String> listener) throws IOException {
//        AnomalyDetectionTaskExecution taskExecution = new AnomalyDetectionTaskExecution(
//                task.getTaskId(),
//                task,
//                null,
//                task.getDataStartTime(),
//                task.getDataEndTime(),
//                Instant.now(),
//                null,
//                task.getDataStartTime(),
//                0.0f,
//                AnomalyDetectionTaskState.INIT.name(),
//                null,
//                0,
//                Instant.now()
//        );
//
//        indexTaskExecution(taskExecution, null, ActionListener.wrap(response -> {
//            if (response.getShardInfo().getSuccessful() < 1) {
//                listener.onFailure(new RuntimeException("Fail to index anomaly detection task execution"));
//            }
//            // execute task
//            executeTask(taskExecution, response.getId(), listener);
//        }, exception -> { listener.onFailure(exception); }));
//    }
//
//    public void indexTaskExecution(
//            AnomalyDetectionTaskExecution taskExecution,
//            String taskExecutionId,
//            ActionListener<IndexResponse> listener
//    ) throws IOException {
//        IndexRequest indexRequest = new IndexRequest(ANOMALY_DETECTION_TASK_EXECUTION_INDEX)
//                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
//                .id(taskExecutionId)
//                .source(taskExecution.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), XCONTENT_WITH_TYPE));
//        client.index(indexRequest, listener);
//    }
//
//    public void updateTaskExecution(String taskExecutionId, Map<String, Object> updatedFields) {
//        updateTaskExecution(taskExecutionId, updatedFields, ActionListener.wrap(response -> {
//            if (response.status() == RestStatus.OK) {
//                log.info("Updated task execution result {}", response.status());
//            } else {
//                log.error("Failed to update task execution {}, status: {}", taskExecutionId, response.status());
//            }
//        }, exception -> { log.error("Failed to update task execution" + taskExecutionId, exception); }));
//    }
//
//    public void updateTaskExecution(String taskExecutionId, Map<String, Object> updatedFields, ActionListener<UpdateResponse> listener) {
//        UpdateRequest updateRequest = new UpdateRequest(ANOMALY_DETECTION_TASK_EXECUTION_INDEX, taskExecutionId);
//        Map<String, Object> updatedContent = new HashMap<>();
//        updatedContent.putAll(updatedFields);
//        updatedContent.put(LAST_UPDATE_TIME_FIELD, Instant.now().toEpochMilli());
//        updateRequest.doc(updatedContent);
//        client
//                .update(
//                        updateRequest,
//                        ActionListener.wrap(response -> listener.onResponse(response), exception -> listener.onFailure(exception))
//                );
//    }
//
//    private void executeTask(AnomalyDetectionTaskExecution taskExecution, String taskExecutionId, ActionListener<String> listener) {
//        try {
//            // getNodeTaskStats(taskExecution.getTaskId(), ActionListener.wrap(r -> {
//            // threadPool.executor(AD_BATCh_TASK_THREAD_POOL_NAME).submit(taskRunnable(taskExecution, taskExecutionId,
//            // r));
//            // listener.onResponse(taskExecutionId);
//            // }, e -> handleTaskExecutionException(taskExecution, taskExecutionId, e)));
//            threadPool.executor(AD_BATCh_TASK_THREAD_POOL_NAME).submit(taskRunnable(taskExecution, taskExecutionId));
//            listener.onResponse(taskExecutionId);
//        } catch (Exception e) {
//            log.error("Fail to start AD batch task " + taskExecution.getTaskId(), e);
//            listener.onFailure(e);
//            updateTaskExecution(
//                    taskExecutionId,
//                    ImmutableMap
//                            .of(
//                                    STATE_FIELD,
//                                    AnomalyDetectionTask.FAILED.name(),
//                                    EXECUTION_END_TIME_FIELD,
//                                    Instant.now(),
//                                    ERROR_FIELD,
//                                    ExceptionUtils.getFullStackTrace(e)
//                            )
//            );
//        }
//    }
//
//    private void getNodeTaskStats(String taskId, ActionListener<DiscoveryNode> listener) {
//        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
//        ADStatsRequest adStatsRequest = new ADStatsRequest(dataNodes);
//        adStatsRequest.addAll(ImmutableSet.of(StatNames.AD_EXECUTING_TASK_COUNT.getName(), StatNames.NODE_MEMORY_USAGE.getName()));
//
//        DiscoveryNode localNode = clusterService.localNode();
//        System.out.println(localNode.getId());
//        client.execute(ADStatsNodesAction.INSTANCE, adStatsRequest, ActionListener.wrap(adStatsResponse -> {
//            List<ADStatsNodeResponse> candidateNodeResponse = adStatsResponse
//                    .getNodes()
//                    .stream()
//                    // .filter(stat -> (short) stat.getStatsMap().get(StatNames.MEMORY_USAGE.name()) < 85 && (long)
//                    // stat.getStatsMap().get(StatNames.AD_EXECUTING_TASK_COUNT.name()) < 10)
//                    .filter(stat -> (int) stat.getStatsMap().get(StatNames.AD_EXECUTING_TASK_COUNT.getName()) < 10)
//                    // .sorted((ADStatsNodeResponse r1, ADStatsNodeResponse r2) -> ((Short)
//                    // r1.getStatsMap().get(StatNames.MEMORY_USAGE.name())).compareTo((short)
//                    // r2.getStatsMap().get(StatNames.MEMORY_USAGE.name())))
//                    .sorted(
//                            (ADStatsNodeResponse r1, ADStatsNodeResponse r2) -> ((Integer) r1
//                                    .getStatsMap()
//                                    .get(StatNames.AD_EXECUTING_TASK_COUNT.getName()))
//                                    .compareTo((Integer) r2.getStatsMap().get(StatNames.AD_EXECUTING_TASK_COUNT.getName()))
//                    )
//                    .collect(Collectors.toList());
//            if (candidateNodeResponse.size() > 0) {
//                listener.onResponse(candidateNodeResponse.get(0).getNode());
//                // for (ADStatsNodeResponse r : candidateNodeResponse) {
//                // if(!localNode.getId().equals(r.getNode().getId())) {
//                // listener.onResponse(r.getNode());
//                // break;
//                // }
//                // }
//
//            } else {
//                String errorMessage = "No eligible node to run AD task " + taskId;
//                log.warn(errorMessage);
//                listener.onFailure(new LimitExceededException(errorMessage));
//            }
//        }, exception -> {
//            log.error("Failed to get node's task stats", exception);
//            listener.onFailure(exception);
//        }));
//    }
//
//    private Runnable taskRunnable(AnomalyDetectionTaskExecution taskExecution, String taskExecutionId) {
//        // AnomalyDetectionTaskExecution taskExecution, String taskExecutionId, DiscoveryNode node) {
//        // return () -> {
//        // if (clusterService.localNode().getId().equals(node.getId())) {
//        // executeTaskLocally(taskExecution, taskExecutionId, node.getId());
//        // } else {
//        // executeTaskRemotely(taskExecution, taskExecutionId, node.getId());
//        // }
//        // };
//        return () -> {
//            getNodeTaskStats(taskExecution.getTaskId(), ActionListener.wrap(node -> {
//                if (clusterService.localNode().getId().equals(node.getId())) {
//                    executeTaskLocally(taskExecution, taskExecutionId, node.getId());
//                } else {
//                    executeTaskRemotely(taskExecution, taskExecutionId, node.getId());
//                }
//            }, exception -> { handleTaskExecutionException(taskExecution, taskExecutionId, exception); }));
//
//        };
//    }
//
//    private ActionListener<AnomalyResultBatchResponse> taskExecutionListener(
//            AnomalyDetectionTaskExecution taskExecution,
//            String taskExecutionId
//    ) {
//        return ActionListener.wrap(response -> {
//            log.info("Task execution finished for {}, response: {}", taskExecutionId, response.getMessage());
//            // remove task execution from map when finish
//            removeTaskExecution(taskExecutionId);
//        }, exception -> { handleTaskExecutionException(taskExecution, taskExecutionId, exception); });
//    }
//
//    private void handleTaskExecutionException(AnomalyDetectionTaskExecution taskExecution, String taskExecutionId, Exception exception) {
//        // remove task execution from map if execution fails
//        log.error("Fail to execute batch task action " + taskExecution.getTaskId(), exception);
//        removeTaskExecution(taskExecutionId);
//        String state = AnomalyDetectionTask.FAILED.name();
//        Map<String, Object> updatedFields = new HashMap<>();
//        if (exception instanceof TaskCancelledException) {
//            state = AnomalyDetectionTask.CANCELLED.name();
//        } else {
//            updatedFields.put(ERROR_FIELD, ExceptionUtils.getFullStackTrace(exception));
//        }
//        updatedFields.put(STATE_FIELD, state);
//        updatedFields.put(EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli());
//        updateTaskExecution(taskExecutionId, updatedFields);
//    }
//
//    private void executeTaskRemotely(AnomalyDetectionTaskExecution taskExecution, String taskExecutionId, String nodeId) {
//        try {
//            ADTaskExecutionRequest request = new ADTaskExecutionRequest(
//                    taskExecution.getTaskId(),
//                    taskExecutionId,
//                    taskExecution.getDataStartTime().toEpochMilli(),
//                    taskExecution.getDataEndTime().toEpochMilli(),
//                    nodeId
//            );
//            client.execute(ADTaskExecutionAction.INSTANCE, request, taskExecutionListener(taskExecution, taskExecutionId));
//        } catch (Exception e) {
//            handleTaskExecutionException(taskExecution, taskExecutionId, e);
//        }
//
//    }
//
//    private void executeTaskLocally(AnomalyDetectionTaskExecution taskExecution, String taskExecutionId, String nodeId) {
//        try {
//            AnomalyResultBatchRequest request = new AnomalyResultBatchRequest(
//                    taskExecution.getTaskId(),
//                    taskExecutionId,
//                    taskExecution.getDataStartTime().toEpochMilli(),
//                    taskExecution.getDataEndTime().toEpochMilli(),
//                    nodeId
//            );
//            client.execute(AnomalyResultBatchAction.INSTANCE, request, taskExecutionListener(taskExecution, taskExecutionId));
//            // String error = checkLimitation();
//            // if (error == null) {
//            // client.execute(AnomalyResultBatchAction.INSTANCE, request, ActionListener.wrap(response -> {
//            // log.info("Task execution finished for {}, response: {}", taskExecutionId, response.getMessage());
//            // // remove task execution from map when finish
//            // removeTaskExecution(taskExecutionId);
//            // }, exception -> {
//            // // remove task execution from map if execution fails
//            // removeTaskExecution(taskExecutionId);
//            // String state = AnomalyDetectionTaskState.FAILED.name();
//            // Map<String, Object> updatedFields = new HashMap<>();
//            // if (exception instanceof TaskCancelledException) {
//            // state = AnomalyDetectionTaskState.CANCELLED.name();
//            // } else {
//            // updatedFields.put(ERROR_FIELD, ExceptionUtils.getFullStackTrace(exception));
//            // }
//            // updatedFields.put(STATE_FIELD, state);
//            // updateTaskExecution(taskExecutionId, updatedFields);
//            // log.error("Fail to execute batch task action " + taskExecution.getTaskId(), exception);
//            // }));
//            // } else {
//            // log.warn("This node can't run more task, check other nodes");
//            // DiscoveryNode[] eligibleDataNodes = nodeFilterer.getEligibleDataNodes();
//            // MultiResponsesDelegateActionListener<ADStatsResponse> delegateListener = new MultiResponsesDelegateActionListener<>(
//            // ActionListener.wrap(response -> {}, excpetion -> {}),
//            // ,
//            // "Unable to return AD Stats"
//            // );
//            //
//            // transportService
//            // .sendRequest(
//            // rcfNode.get(),
//            // RCFResultAction.NAME,
//            // new RCFResultRequest(adID, rcfModelID, featureOptional.getProcessedFeatures().get()),
//            // option,
//            // new ActionListenerResponseHandler<>(rcfListener, RCFResultResponse::new)
//            // );
//            // }
//
//        } catch (Exception e) {
//            // log.error("Failed to execute task " + taskExecution.getTaskId(), e);
//            // // remove task execution from map if error happens
//            // removeTaskExecution(taskExecutionId);
//            // updateTaskExecution(
//            // taskExecutionId,
//            // ImmutableMap
//            // .of(
//            // STATE_FIELD,
//            // AnomalyDetectionTaskState.FAILED.name(),
//            // EXECUTION_END_TIME_FIELD,
//            // Instant.now(),
//            // ERROR_FIELD,
//            // ExceptionUtils.getFullStackTrace(e)
//            // )
//            // );
//            handleTaskExecutionException(taskExecution, taskExecutionId, e);
//        }
//    }
//
//    private void getNodeStats(
//            Client client,
//            MultiResponsesDelegateActionListener<ADStatsResponse> listener,
//            ADStatsRequest adStatsRequest
//    ) {
//        client.execute(ADStatsNodesAction.INSTANCE, adStatsRequest, ActionListener.wrap(adStatsResponse -> {
//            ADStatsResponse restADStatsResponse = new ADStatsResponse();
//            restADStatsResponse.setADStatsNodesResponse(adStatsResponse);
//            listener.onResponse(restADStatsResponse);
//        }, listener::onFailure));
//    }
//
//    /*public void indexTaskExecution(AnomalyDetectionTaskExecution taskExecution, String taskExecutionId) {
//        try {
//            indexTaskExecution(
//                taskExecution,
//                taskExecutionId,
//                ActionListener.wrap(r -> { log.info(r.status()); }, e -> { log.error("Fail to index task execution", e); })
//            );
//        } catch (IOException exception) {
//            log.error("Failed to index task execution" + taskExecutionId, exception);
//        }
//    }*/
//
//    public void stopTask(String taskId, ActionListener<String> listener) {
//        ListTasksRequest listTasksRequest = new ListTasksRequest();
//        listTasksRequest.setActions("*ad/batchResult*");
//        client.execute(ListTasksAction.INSTANCE, listTasksRequest, ActionListener.wrap(res -> {
//            log.info("AD batch tasks: {}", res.getPerNodeTasks());
//            List<TaskInfo> tasks = res.getTasks();
//            List<TaskInfo> infos = tasks
//                    .stream()
//                    .filter(
//                            taskInfo -> StringUtils.equals(taskInfo.getHeaders().get(Task.X_OPAQUE_ID), TASK_ID_HEADER + ":" + taskId)
//                                    && taskInfo.isCancellable()
//                    )
//                    .collect(Collectors.toList());
//            if (infos.size() > 0) {
//                log.info("Found {} tasks for taskId {}", infos.size(), taskId);
//                infos.forEach(info -> {
//                    CancelTasksRequest cancelTaskRequest = new CancelTasksRequest();
//                    cancelTaskRequest.setTaskId(infos.get(0).getTaskId());
//                    client.execute(CancelTasksAction.INSTANCE, cancelTaskRequest, ActionListener.wrap(r -> {
//                        log.info("Finished to cancel task {}", infos.get(0));
//                        listener.onResponse("Task cancelled successfully");
//                        // channel.sendResponse(new BytesRestResponse(RestStatus.OK, "Task cancelled successfully"));
//                    }, exception -> {
//                        log.error("Failed to cancel task " + taskId, exception);
//                        listener.onFailure(exception);
//                    }));
//                });
//            } else {
//                listener.onResponse("Task is not running");
//            }
//        }, exception -> {
//            log.error("Fail to stop task " + taskId, exception);
//            listener.onFailure(exception);
//        }));
//    }
//
//    public void deleteTask(String taskId, ActionListener<String> listener) {
//        ActionListener deleteTaskListener = ActionListener
//                .wrap(
//                        deleteTaskResponse -> { listener.onResponse("Task deleted successfully"); },
//                        deleteTaskException -> listener.onFailure(deleteTaskException)
//                );
//        getLatestTaskExecution(taskId, ActionListener.wrap(taskExecution -> {
//                    if (StringUtils.equals(taskExecution.getState(), AnomalyDetectionTaskState.RUNNING.name())) {
//                        listener.onFailure(new RuntimeException("Task is running, please stop task before deleting"));
//                        return;
//                    }
//                    deleteTaskExecution(
//                            taskId,
//                            ActionListener
//                                    .wrap(
//                                            deleteResponse -> { deleteTaskDoc(taskId, deleteTaskListener); },
//                                            deleteException -> { listener.onFailure(deleteException); }
//                                    )
//                    );
//                },
//                exception -> {
//                    // If task execution index not exist or task execution not found, we should delete task
//                    if (exception instanceof IndexNotFoundException || exception instanceof ResourceNotFoundException) {
//                        deleteTaskDoc(taskId, deleteTaskListener);
//                    } else {
//                        log.error("Fail to delete task " + taskId, exception);
//                        listener.onFailure(exception);
//                    }
//                }
//        ));
//    }
//
//    /**
//     * Get latest task execution.
//     * @param taskId task id
//     * @param listener action listener
//     */
//    private void getLatestTaskExecution(String taskId, ActionListener<AnomalyDetectionTaskExecution> listener) {
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(QueryBuilders.termQuery("task_id", taskId)).size(1).sort("execution_start_time", SortOrder.DESC);
//        SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTION_TASK_EXECUTION_INDEX).source(searchSourceBuilder);
//        client.search(searchRequest, ActionListener.wrap(r -> {
//            if (r.getHits().getHits().length > 0) {
//                SearchHit hit = r.getHits().getAt(0);
//                try (
//                        XContentParser parser = XContentType.JSON
//                                .xContent()
//                                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.getSourceAsString())
//                ) {
//                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
//                    AnomalyDetectionTaskExecution taskExecution = AnomalyDetectionTaskExecution.parse(parser, hit.getId());
//                    listener.onResponse(taskExecution);
//                } catch (Exception e) {
//                    listener.onFailure(e);
//                }
//            } else {
//                listener.onFailure(new ResourceNotFoundException("Find no task execution for task " + taskId));
//            }
//        }, e -> listener.onFailure(e)));
//    }
//
//    // TODO: add delete old task executions in hourly cron
//    private void deleteTaskExecution(String taskId, ActionListener<BulkByScrollResponse> listener) {
//        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(ANOMALY_DETECTION_TASK_EXECUTION_INDEX);
//        deleteRequest.setQuery(QueryBuilders.termQuery("task_id", taskId));
//        client.execute(DeleteByQueryAction.INSTANCE, deleteRequest, listener);
//    }
//
//    private void deleteTaskDoc(String taskId, ActionListener<DeleteResponse> listener) {
//        DeleteRequest deleteRequest = new DeleteRequest(ANOMALY_DETECTION_TASK_INDEX)
//                .id(taskId)
//                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
//        client.delete(deleteRequest, listener);
//    }
//
//    public void getTask(String taskId, boolean returnTaskExecution, ActionListener<Map<String, Object>> listener) {
//        Map<String, Object> result = new HashMap<>();
//        GetRequest getRequest = new GetRequest(ANOMALY_DETECTION_TASK_INDEX).id(taskId);
//        client.get(getRequest, ActionListener.wrap(getTaskResponse -> {
//            if (getTaskResponse.isExists()) {
//                result.put(GET_TASK_RESPONSE, getTaskResponse);
//                try (
//                        XContentParser parser = XContentType.JSON
//                                .xContent()
//                                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getTaskResponse.getSourceAsString())
//                ) {
//                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
//                    AnomalyDetectionTask task = AnomalyDetectionTask.parse(parser, getTaskResponse.getId());
//                    result.put(RestHandlerUtils.ANOMALY_DETECTION_TASK, task);
//
//                } catch (Exception e) {
//                    log.error("Fail to parse anomaly detection task " + taskId, e);
//                    listener.onFailure(e);
//                }
//
//                if (returnTaskExecution) {
//                    getLatestTaskExecution(taskId, ActionListener.wrap(taskExecution -> {
//                                result.put(RestHandlerUtils.ANOMALY_DETECTION_TASK_EXECUTION, taskExecution);
//                                listener.onResponse(result);
//                            },
//                            exception -> {
//                                // If task execution index not exist or task execution not found, we should just return task
//                                if (exception instanceof IndexNotFoundException || exception instanceof ResourceNotFoundException) {
//                                    listener.onResponse(result);
//                                } else {
//                                    log.error("Fail to get task execution for task " + taskId, exception);
//                                    listener.onFailure(exception);
//                                }
//                            }
//                    ));
//                } else {
//                    listener.onResponse(result);
//                }
//
//            } else {
//                listener.onFailure(new RuntimeException("Task not found"));
//            }
//        }, getTaskException -> {
//            if (getTaskException instanceof IndexNotFoundException) {
//                listener.onResponse(result);
//            }
//            listener.onFailure(getTaskException);
//        }));
//    }
//
//    public String checkLimitation() {
//        if (taskMap.size() >= MAX_RUNNING_TASK) {
//            return "Can't run more than " + MAX_RUNNING_TASK + " per node";
//        }
//        return null;
//    }
//
//    public void removeTaskExecution(String taskExecutionId) {
//        taskMap.remove(taskExecutionId);
//        // adStats.getStat(StatNames.AD_EXECUTING_TASK_COUNT.getName()).setValue((long) taskMap.size());
//    }
//
//    public void putTaskExecution(String taskExecutionId, AnomalyDetectionBatchTask batchTask) {
//        taskMap.put(taskExecutionId, batchTask);
//        // adStats.getStat(StatNames.AD_EXECUTING_TASK_COUNT.getName()).setValue((long) taskMap.size());
//    }
//
//    public void checkTaskCancelled(String taskExecutionId) {
//        if (taskMap.containsKey(taskExecutionId) && taskMap.get(taskExecutionId).isCancelled()) {
//            removeTaskExecution(taskExecutionId);
//            throw new TaskCancelledException("cancelled");
//        }
//    }
//
//    public int getTaskCount() {
//        return this.taskMap.size();
//    }
//
//    public List<Map<String, Object>> getTasks() {
//        return this.taskMap.values().stream().map(task -> task.getTaskInfo()).collect(Collectors.toList());
//    }

    /*public void createTaskFromDetector(AnomalyDetectionTaskCreationRequest request, ActionListener listener) {
        GetRequest getRequest = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).id(request.getDetectorId());
        client.get(getRequest, ActionListener.wrap(response -> {
            try (
                XContentParser parser = XContentType.JSON
                    .xContent()
                    .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId());
                String name = request.getName() == null ? detector.getName() : request.getName();
                String description = request.getDescription() == null ? detector.getDescription() : request.getDescription();

                AnomalyDetectionTask task = new AnomalyDetectionTask(
                    null,
                    null,
                    name,
                    description,
                    null,
                    request.getDataStartTime(),
                    request.getDataEndTime(),
                    null,
                    null,
                    null,
                    Instant.now(),
                    detector.getTimeField(),
                    detector.getIndices(),
                    detector.getFilterQuery(),
                    detector.getFeatureAttributes(),
                    detector.getDetectionInterval(),
                    detector.getWindowDelay(),
                    detector.getShingleSize()
                );

                IndexRequest indexRequest = new IndexRequest(ANOMALY_DETECTION_TASK_INDEX)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(task.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), XCONTENT_WITH_TYPE));
                client.index(indexRequest, ActionListener.wrap(r -> {
                    if (r.status() == RestStatus.CREATED) {
                        listener.onResponse(r);
                    } else {
                        listener.onFailure(new AnomalyDetectionException("Task creation not acknowledged"));
                    }
                }, e -> { listener.onFailure(e); }));
            } catch (Exception t) {
                log.error("Fail to parse detector {}", request.getDetectorId());
                listener.onFailure(new AnomalyDetectionException("Fail to parse detector"));
            }
        }, exception -> { listener.onFailure(new AnomalyDetectionException("Fail to get detector")); }));
    }*/
}
