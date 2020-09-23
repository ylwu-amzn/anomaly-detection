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
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTaskExecution.ERROR_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTaskExecution.EXECUTION_END_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTaskExecution.LAST_UPDATE_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTaskExecution.STATE_FIELD;
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
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTaskExecution;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultBatchAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultBatchRequest;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.google.common.collect.ImmutableMap;

public class AnomalyDetectionTaskManager {
    public static final String GET_TASK_RESPONSE = "getTaskResponse";
    private final Logger log = LogManager.getLogger(this.getClass());
    private static final String TASK_ID_HEADER = "anomaly_detection_task_id";

    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final ThreadPool threadPool;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    // private final HashRing hashRing;

    public AnomalyDetectionTaskManager(
        ThreadPool threadPool,
        Client client,
        AnomalyDetectionIndices anomalyDetectionIndices,
        NamedXContentRegistry xContentRegistry
    ) {
        this.threadPool = threadPool;
        this.client = client;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.xContentRegistry = xContentRegistry;
        // this.hashRing = hashRing;
    }

    /**
     * Start task. If task is already running, will not rerun.
     * @param taskId task id
     * @param listener action listener
     */
    public void startTask(String taskId, ActionListener<String> listener) {
        GetRequest getTaskRequest = new GetRequest(ANOMALY_DETECTION_TASK_INDEX, taskId);

        // step1 get task
        client.get(getTaskRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                // TODO: add more exception types to support task id
                listener.onFailure(new IllegalArgumentException("Anomaly detection task not found"));
                return;
            }
            try (
                XContentParser parser = XContentType.JSON
                    .xContent()
                    .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                AnomalyDetectionTask task = AnomalyDetectionTask.parse(parser, taskId);
                // step2 check latest task execution state, if it's running, will not rerun.
                getLatestTaskExecution(taskId, ActionListener.wrap(taskExecution -> {
                    if (!AnomalyDetectionTaskState.RUNNING.name().equals(taskExecution.getState())) {
                        prepareTask(taskId, task, listener);
                    } else {
                        listener.onFailure(new AnomalyDetectionException("Task is already running"));
                    }
                },
                    exception -> {
                        // If task execution index not exist or task execution not found, we should start task
                        if (exception instanceof IndexNotFoundException || exception instanceof ResourceNotFoundException) {
                            prepareTask(taskId, task, listener);
                        } else {
                            log.error("Failed to get latest task execution", exception);
                            listener.onFailure(exception);
                        }
                    }
                ));
            } catch (Exception e) {
                log.error("Fail to start task " + taskId, e);
                listener.onFailure(e);
            }
        }, exception -> {
            log.error("Fail to get task " + taskId, exception);
            listener.onFailure(exception);
        }));
    }

    private void prepareTask(String taskId, AnomalyDetectionTask task, ActionListener<String> listener) {
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
            task,
            null,
            task.getDataStartTime(),
            task.getDataEndTime(),
            Instant.now(),
            null,
            task.getDataStartTime(),
            0.0f,
            AnomalyDetectionTaskState.INIT.name(),
            null,
            0,
            Instant.now()
        );

        indexTaskExecution(taskExecution, null, ActionListener.wrap(response -> {
            if (response.getShardInfo().getSuccessful() < 1) {
                listener.onFailure(new RuntimeException("Fail to index anomaly detection task execution"));
            }
            // execute task
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

    public void updateTaskExecution(String taskExecutionId, Map<String, Object> updatedFields) {
        updateTaskExecution(taskExecutionId, updatedFields, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                log.info("Updated task execution result {}", response.status());
            } else {
                log.error("Failed to update task execution {}, status: {}", taskExecutionId, response.status());
            }
        }, exception -> { log.error("Failed to update task execution" + taskExecutionId, exception); }));
    }

    public void updateTaskExecution(String taskExecutionId, Map<String, Object> updatedFields, ActionListener<UpdateResponse> listener) {
        UpdateRequest updateRequest = new UpdateRequest(ANOMALY_DETECTION_TASK_EXECUTION_INDEX, taskExecutionId);
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

    private void executeTask(AnomalyDetectionTaskExecution taskExecution, String taskExecutionId, ActionListener<String> listener) {
        try {
            threadPool.executor(AD_BATCh_TASK_THREAD_POOL_NAME).submit(taskRunnable(taskExecution, taskExecutionId));
            listener.onResponse(taskExecutionId);
        } catch (Exception e) {
            log.error("Fail to start AD batch task " + taskExecution.getTaskId(), e);
            listener.onFailure(e);
            updateTaskExecution(
                taskExecutionId,
                ImmutableMap
                    .of(
                        STATE_FIELD,
                        AnomalyDetectionTaskState.FAILED.name(),
                        EXECUTION_END_TIME_FIELD,
                        Instant.now(),
                        ERROR_FIELD,
                        ExceptionUtils.getFullStackTrace(e)
                    )
            );
        }
    }

    private Runnable taskRunnable(AnomalyDetectionTaskExecution taskExecution, String taskExecutionId) {
        return () -> {
            try {
                AnomalyResultBatchRequest request = new AnomalyResultBatchRequest(
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
                            String state = AnomalyDetectionTaskState.FAILED.name();
                            Map<String, Object> updatedFields = new HashMap<>();
                            if (exception instanceof TaskCancelledException) {
                                state = AnomalyDetectionTaskState.CANCELLED.name();
                            } else {
                                updatedFields.put(ERROR_FIELD, ExceptionUtils.getFullStackTrace(exception));
                            }
                            updatedFields.put(STATE_FIELD, state);
                            updateTaskExecution(taskExecutionId, updatedFields);
                            log.error("Fail to execute batch task action " + taskExecution.getTaskId(), exception);
                        })
                    );
            } catch (Exception e) {
                log.error("Failed to execute task " + taskExecution.getTaskId(), e);
                updateTaskExecution(
                    taskExecutionId,
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            AnomalyDetectionTaskState.FAILED.name(),
                            EXECUTION_END_TIME_FIELD,
                            Instant.now(),
                            ERROR_FIELD,
                            ExceptionUtils.getFullStackTrace(e)
                        )
                );
            }
        };
    }

    /*public void indexTaskExecution(AnomalyDetectionTaskExecution taskExecution, String taskExecutionId) {
        try {
            indexTaskExecution(
                taskExecution,
                taskExecutionId,
                ActionListener.wrap(r -> { log.info(r.status()); }, e -> { log.error("Fail to index task execution", e); })
            );
        } catch (IOException exception) {
            log.error("Failed to index task execution" + taskExecutionId, exception);
        }
    }*/

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
                        log.error("Failed to cancel task " + taskId, exception);
                        listener.onFailure(exception);
                    }));
                });
            } else {
                listener.onResponse("Task is not running");
            }
        }, exception -> {
            log.error("Fail to stop task " + taskId, exception);
            listener.onFailure(exception);
        }));
    }

    public void deleteTask(String taskId, ActionListener<String> listener) {
        ActionListener deleteTaskListener = ActionListener
            .wrap(
                deleteTaskResponse -> { listener.onResponse("Task deleted successfully"); },
                deleteTaskException -> listener.onFailure(deleteTaskException)
            );
        getLatestTaskExecution(taskId, ActionListener.wrap(taskExecution -> {
            if (StringUtils.equals(taskExecution.getState(), AnomalyDetectionTaskState.RUNNING.name())) {
                listener.onFailure(new RuntimeException("Task is running, please stop task before deleting"));
                return;
            }
            deleteTaskExecution(
                taskId,
                ActionListener
                    .wrap(
                        deleteResponse -> { deleteTaskDoc(taskId, deleteTaskListener); },
                        deleteException -> { listener.onFailure(deleteException); }
                    )
            );
        },
            exception -> {
                // If task execution index not exist or task execution not found, we should delete task
                if (exception instanceof IndexNotFoundException || exception instanceof ResourceNotFoundException) {
                    deleteTaskDoc(taskId, deleteTaskListener);
                } else {
                    log.error("Fail to delete task " + taskId, exception);
                    listener.onFailure(exception);
                }
            }
        ));
    }

    /**
     * Get latest task execution.
     * @param taskId task id
     * @param listener action listener
     */
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
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetectionTaskExecution taskExecution = AnomalyDetectionTaskExecution.parse(parser, hit.getId());
                    listener.onResponse(taskExecution);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            } else {
                listener.onFailure(new ResourceNotFoundException("Find no task execution for task " + taskId));
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
                result.put(GET_TASK_RESPONSE, getTaskResponse);
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getTaskResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetectionTask task = AnomalyDetectionTask.parse(parser, getTaskResponse.getId());
                    result.put(RestHandlerUtils.ANOMALY_DETECTION_TASK, task);

                } catch (Exception e) {
                    log.error("Fail to parse anomaly detection task " + taskId, e);
                    listener.onFailure(e);
                }

                if (returnTaskExecution) {
                    getLatestTaskExecution(taskId, ActionListener.wrap(taskExecution -> {
                        result.put(RestHandlerUtils.ANOMALY_DETECTION_TASK_EXECUTION, taskExecution);
                        listener.onResponse(result);
                    },
                        exception -> {
                            // If task execution index not exist or task execution not found, we should just return task
                            if (exception instanceof IndexNotFoundException || exception instanceof ResourceNotFoundException) {
                                listener.onResponse(result);
                            } else {
                                log.error("Fail to get task execution for task " + taskId, exception);
                                listener.onFailure(exception);
                            }
                        }
                    ));
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
