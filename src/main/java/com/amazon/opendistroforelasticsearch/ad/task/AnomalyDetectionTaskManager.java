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

import static com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin.AD_BATCh_TASK_THREAD_POOL_NAME;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTask.ANOMALY_DETECTION_TASK_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTaskExecution.ANOMALY_DETECTION_TASK_EXECUTION_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTaskExecution;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultBatchAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultBatchRequest;

public class AnomalyDetectionTaskManager {
    private final Logger log = LogManager.getLogger(this.getClass());
    private static final String TASK_ID_HEADER = "anomaly_detection_task_id";

    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final ThreadPool threadPool;
    private final Client client;
    // private final HashRing hashRing;

    public AnomalyDetectionTaskManager(ThreadPool threadPool, Client client, AnomalyDetectionIndices anomalyDetectionIndices) {
        this.threadPool = threadPool;
        this.client = client;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        // this.hashRing = hashRing;
    }

    // TODO: check if task is running or not
    public void startTask(String taskId, ActionListener<String> listener) {
        GetRequest getTaskRequest = new GetRequest(ANOMALY_DETECTION_TASK_INDEX, taskId);

        // Step1 get task
        client.get(getTaskRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                // TODO: add more exception types to support task id
                listener.onFailure(new IllegalArgumentException("Anomaly detection task not found"));
                return;
            }
            try (
                XContentParser parser = XContentType.JSON
                    .xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                AnomalyDetectionTask task = AnomalyDetectionTask.parse(parser, taskId);

                try (ThreadContext.StoredContext context = threadPool.getThreadContext().stashContext()) {
                    assert context != null;
                    threadPool.getThreadContext().putHeader(Task.X_OPAQUE_ID, TASK_ID_HEADER + ":" + taskId);

                    if (!anomalyDetectionIndices.doesAnomalyDetectionTaskExecutionIndexExist()) {
                        anomalyDetectionIndices
                            .initAnomalyDetectionTaskExecutionIndexDirectly(
                                ActionListener.wrap(res -> onCreateTaskExecutionIndex(res, task, listener), e -> { listener.onFailure(e); })
                            );
                    } else {
                        indexTaskExecution(task, listener);
                    }
                } catch (Exception e) {
                    log.error("Failed to process the request for taskId: {}.", taskId);
                    listener.onFailure(new RuntimeException("Failed to start task"));
                }
            } catch (Exception e) {
                log.error("Fail to parse task " + taskId, e);
                listener.onFailure(e);
            }
        }, exception -> {
            log.error("Fail to get task " + taskId, exception);
            listener.onFailure(exception);
        }));

    }

    private void onCreateTaskExecutionIndex(CreateIndexResponse response, AnomalyDetectionTask task, ActionListener<String> listener)
        throws IOException {
        if (response.isAcknowledged()) {
            indexTaskExecution(task, listener);
        } else {
            listener.onFailure(new RuntimeException("Create task execution index not acknowledged"));
        }
    }

    private void indexTaskExecution(AnomalyDetectionTask task, ActionListener<String> listener) throws IOException {
        AnomalyDetectionTaskExecution taskExecution = new AnomalyDetectionTaskExecution(
            task.getTaskId(),
            task.getDetectorId(),
            null,
            task.getDataStartTime(),
            task.getDataEndTime(),
            Instant.now(),
            null,
            task.getDataStartTime(),
            AnomalyDetectionTaskState.INIT.name(),
            null,
            0,
            Instant.now()
        );

        indexTaskExecution(taskExecution, null, ActionListener.wrap(response -> {
            if (response.getShardInfo().getSuccessful() < 1) {
                listener.onFailure(new RuntimeException("Fail to index anomaly detection task execution"));
            }

            executeTask(taskExecution, response.getId(), listener);
        }, exception -> { listener.onFailure(exception); }));
    }

    public void indexTaskExecution(
        AnomalyDetectionTaskExecution taskExecution,
        String taskExecutionId,
        ActionListener<IndexResponse> listener
    ) throws IOException {
        IndexRequest indexRequest = new IndexRequest(ANOMALY_DETECTION_TASK_EXECUTION_INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .id(taskExecutionId)
            .source(taskExecution.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), XCONTENT_WITH_TYPE));
        client.index(indexRequest, listener);
    }

    private void executeTask(AnomalyDetectionTaskExecution task, String taskExecutionId, ActionListener<String> listener) {
        try {
            threadPool.executor(AD_BATCh_TASK_THREAD_POOL_NAME).submit(taskRunnable(task, taskExecutionId));
            listener.onResponse(taskExecutionId);
        } catch (Exception e) {
            log.error("Fail to start AD batch task " + task.getTaskId(), e);
            listener.onFailure(e);
            // TODO: catch exception, set task execution as failed.
            AnomalyDetectionTaskExecution taskExecution = new AnomalyDetectionTaskExecution(
                task.getTaskId(),
                task.getDetectorId(),
                task.getVersion(),
                task.getDataStartTime(),
                task.getDataEndTime(),
                task.getExecutionStartTime(),
                Instant.now(),
                task.getCurrentDetectionInterval(),
                AnomalyDetectionTaskState.FAILED.name(),
                ExceptionUtils.getFullStackTrace(e),
                task.getSchemaVersion(),
                Instant.now()
            );
            try {
                indexTaskExecution(
                    taskExecution,
                    taskExecutionId,
                    ActionListener
                        .wrap(r -> { log.info(r.status()); }, exception -> { log.error("Fail to index task execution", exception); })
                );
            } catch (IOException exception) {
                log.error("Fail to index task execution", exception);
            }
        }
    }

    // TODO, remove taskExecutionId?
    private Runnable taskRunnable(AnomalyDetectionTaskExecution taskExecution, String taskExecutionId) {
        return () -> {
            try {
                AnomalyResultBatchRequest request = new AnomalyResultBatchRequest(
                    taskExecution.getDetectorId(),
                    taskExecution.getTaskId(),
                    taskExecutionId,
                    taskExecution.getDataStartTime().toEpochMilli(),
                    taskExecution.getDataEndTime().toEpochMilli()
                );
                client
                    .execute(
                        AnomalyResultBatchAction.INSTANCE,
                        request,
                        ActionListener.wrap(response -> { log.info(response.getMessage()); }, exception -> {
                            // TODO: make state enum
                            String state = exception instanceof TaskCancelledException
                                ? AnomalyDetectionTaskState.CANCELLED.name()
                                : AnomalyDetectionTaskState.FAILED.name();
                            // TODO: log error message in task execution
                            AnomalyDetectionTaskExecution newTaskExecution = new AnomalyDetectionTaskExecution(
                                taskExecution.getTaskId(),
                                taskExecution.getDetectorId(),
                                taskExecution.getVersion(),
                                taskExecution.getDataStartTime(),
                                taskExecution.getDataEndTime(),
                                taskExecution.getExecutionStartTime(),
                                Instant.now(),
                                taskExecution.getCurrentDetectionInterval(),
                                state,
                                ExceptionUtils.getFullStackTrace(exception),
                                taskExecution.getSchemaVersion(),
                                Instant.now()
                            );
                            indexTaskExecution(newTaskExecution, taskExecutionId);
                            log.error("Fail to execute batch task action " + taskExecution.getTaskId(), exception);
                        })
                    );
            } catch (Exception e) {
                // TODO: log error message in task execution
                log.error("Failed to execute task " + taskExecution.getTaskId(), e);
            }
        };
    }

    public void indexTaskExecution(AnomalyDetectionTaskExecution taskExecution, String taskExecutionId) {
        try {
            indexTaskExecution(
                taskExecution,
                taskExecutionId,
                ActionListener.wrap(r -> { log.info(r.status()); }, e -> { log.error("Fail to index task execution", e); })
            );
        } catch (IOException exception) {
            log.error("Failed to index task execution" + taskExecutionId, exception);
        }

    }

    public void stopTask(String taskId, ActionListener<String> listener) {
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions("*ad/batchResult*");
        client.execute(ListTasksAction.INSTANCE, listTasksRequest, ActionListener.wrap(res -> {
            log.info("AD batch tasks: {}", res.getPerNodeTasks());
            List<TaskInfo> tasks = res.getTasks();
            List<TaskInfo> infos = tasks
                .stream()
                .filter(
                    taskInfo -> StringUtils.equals(taskInfo.getHeaders().get(Task.X_OPAQUE_ID), TASK_ID_HEADER + ":" + taskId)
                        && taskInfo.isCancellable()
                )
                .collect(Collectors.toList());
            if (infos.size() > 0) {
                log.info("Found {} tasks for taskId {}", infos.size(), taskId);
                infos.forEach(info -> {
                    CancelTasksRequest cancelTaskRequest = new CancelTasksRequest();
                    cancelTaskRequest.setTaskId(infos.get(0).getTaskId());
                    client.execute(CancelTasksAction.INSTANCE, cancelTaskRequest, ActionListener.wrap(r -> {
                        log.info("Finished to cancel task {}", infos.get(0));
                        listener.onResponse("Task cancelled successfully");
                        // channel.sendResponse(new BytesRestResponse(RestStatus.OK, "Task cancelled successfully"));
                    }, exception -> {
                        log.error("Failed to cancel task for task {} ", infos.get(0), exception);
                        listener.onFailure(exception);
                    }));
                });
            } else {
                listener.onResponse("No running task found");
            }
        }, exception -> {
            log.error("Fail to stop task " + taskId, exception);
            listener.onFailure(exception);
        }));
    }

    public void deleteTask(String taskId, ActionListener<String> listener) {
        getLatestTaskExecution(taskId, ActionListener.wrap(taskExecution -> {
            if (StringUtils.equals(taskExecution.getState(), AnomalyDetectionTaskState.RUNNING.name())) {
                listener.onFailure(new RuntimeException("Task is running, please stop task before deleting"));
                return;
            }
            deleteTaskExecution(taskId, ActionListener.wrap(deleteResponse -> {
                deleteTaskDoc(
                    taskId,
                    ActionListener
                        .wrap(
                            deleteTaskResponse -> { listener.onResponse("Task deleted successfully"); },
                            deleteTaskException -> listener.onFailure(deleteTaskException)
                        )
                );
                // if (deleteResponse.getStatus() == BulkByScrollTask.Status.)
            }, deleteException -> { listener.onFailure(deleteException); }));
        }, e -> {
            log.error("Fail to delete task " + taskId, e);
            listener.onFailure(e);
        }));

    }

    private void getLatestTaskExecution(String taskId, ActionListener<AnomalyDetectionTaskExecution> listener) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("task_id", taskId)).size(1).sort("execution_start_time", SortOrder.DESC);
        SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTION_TASK_EXECUTION_INDEX).source(searchSourceBuilder);
        client.search(searchRequest, ActionListener.wrap(r -> {
            if (r.getHits().getHits().length > 0) {
                SearchHit hit = r.getHits().getAt(0);
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, hit.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetectionTaskExecution taskExecution = AnomalyDetectionTaskExecution.parse(parser);
                    listener.onResponse(taskExecution);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        }, e -> listener.onFailure(e)));
    }

    // TODO: add delete old task executions in hourly cron
    private void deleteTaskExecution(String taskId, ActionListener<BulkByScrollResponse> listener) {
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(ANOMALY_DETECTION_TASK_EXECUTION_INDEX);
        deleteRequest.setQuery(QueryBuilders.termQuery("task_id", taskId));
        client.execute(DeleteByQueryAction.INSTANCE, deleteRequest, listener);
    }

    private void deleteTaskDoc(String taskId, ActionListener<DeleteResponse> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(ANOMALY_DETECTION_TASK_INDEX)
            .id(taskId)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.delete(deleteRequest, listener);
    }

    public void getTask(String taskId, boolean returnTaskExecution, ActionListener<Map<String, Object>> listener) {
        Map<String, Object> result = new HashMap<>();
        GetRequest getRequest = new GetRequest(ANOMALY_DETECTION_TASK_INDEX).id(taskId);
        client.get(getRequest, ActionListener.wrap(getTaskResponse -> {
            if (getTaskResponse.isExists()) {
                result.put("getTaskResponse", getTaskResponse);
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, getTaskResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetectionTask task = AnomalyDetectionTask.parse(parser);
                    result.put("AnomalyDetectionTask", task);

                } catch (Exception e) {
                    log.error("Fail to parse anomaly detection task " + taskId, e);
                    listener.onFailure(e);
                }

                if (returnTaskExecution) {
                    getLatestTaskExecution(taskId, ActionListener.wrap(taskExecution -> {
                        result.put("AnomalyDetectionTaskExecution", taskExecution);
                        listener.onResponse(result);
                    }, taskExecutionException -> {
                        if (taskExecutionException instanceof IndexNotFoundException) {
                            listener.onResponse(result);
                        }
                        log.error("Fail to get task execution for task " + taskId, taskExecutionException);
                        listener.onFailure(taskExecutionException);
                    }));
                } else {
                    listener.onResponse(result);
                }

            } else {
                listener.onFailure(new RuntimeException("Task not found"));
            }
        }, getTaskException -> {
            if (getTaskException instanceof IndexNotFoundException) {
                listener.onResponse(result);
            }
            listener.onFailure(getTaskException);
        }));
    }
}
