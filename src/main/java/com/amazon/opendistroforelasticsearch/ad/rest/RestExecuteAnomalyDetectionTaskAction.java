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

package com.amazon.opendistroforelasticsearch.ad.rest;

import static com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin.AD_BATCh_TASK_THREAD_POOL_NAME;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.START_JOB;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.STOP_JOB;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.TASK_ID;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTaskExecution;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultBatchAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultBatchRequest;
import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to handle request to start/stop AD job.
 */
public class RestExecuteAnomalyDetectionTaskAction extends BaseRestHandler {
    private static final Logger log = LogManager.getLogger(RestExecuteAnomalyDetectionTaskAction.class);
    public static final String AD_JOB_ACTION = "anomaly_detection_task_action";
    private static final String TASK_ID_HEADER = "task_id";
    private final Client client;
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private ThreadPool threadPool;

    public RestExecuteAnomalyDetectionTaskAction(Client client, ThreadPool threadPool, AnomalyDetectionIndices anomalyDetectionIndices) {
        this.client = client;
        this.threadPool = threadPool;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
    }

    @Override
    public String getName() {
        return AD_JOB_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String taskId = request.param(TASK_ID);
        String rawPath = request.rawPath();

        return channel -> {
            GetRequest getRequest = new GetRequest(AnomalyDetectionTask.ANOMALY_DETECTION_TASK_INDEX, taskId);
            client.get(getRequest, ActionListener.wrap(response -> { // if (response.isExists()){
                if (!response.isExists()) {
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, "Anomaly detection task not found"));
                    return;
                }
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetectionTask task = AnomalyDetectionTask.parse(parser, taskId);
                    log.info("Get task result: {}", task);

                    if (rawPath.endsWith(START_JOB)) {
                        try (ThreadContext.StoredContext context = threadPool.getThreadContext().stashContext()) {
                            assert context != null;
                            threadPool.getThreadContext().putHeader(Task.X_OPAQUE_ID, TASK_ID_HEADER + ":" + taskId);

                            if (!anomalyDetectionIndices.doesAnomalyDetectionTaskExecutionIndexExist()) {
                                anomalyDetectionIndices
                                    .initAnomalyDetectionTaskExecutionIndexDirectly(
                                        ActionListener.wrap(res -> onCreateMappingsResponse(res, channel, task), e -> {
                                            try {
                                                channel.sendResponse(new BytesRestResponse(channel, e));
                                            } catch (IOException ioException) {
                                                log.warn("Fail to send out failure message of exception", e);
                                            }
                                        })
                                    );
                            } else {
                                indexAnomalyDetectionTaskExecution(task, channel);
                            }
                        } catch (Exception e) {
                            log.error("Failed to process the request for taskId: {}.", taskId);
                            throw e;
                        }

                    } else if (rawPath.endsWith(STOP_JOB)) {
                        ListTasksRequest listTasksRequest = new ListTasksRequest();
                        listTasksRequest.setActions("*ad/batchResult*");
                        client.execute(ListTasksAction.INSTANCE, listTasksRequest, ActionListener.wrap(res -> {
                            log.info("AD batch tasks: {}", res.getPerNodeTasks());
                            List<TaskInfo> tasks = res.getTasks();
                            List<TaskInfo> infos = tasks
                                .stream()
                                .filter(
                                    taskInfo -> StringUtils
                                        .equals(taskInfo.getHeaders().get(Task.X_OPAQUE_ID), TASK_ID_HEADER + ":" + taskId)
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
                                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, "Task cancelled successfully"));
                                    }, exception -> {
                                        log.error("Failed to cancel task for task {} ", infos.get(0), exception);
                                        try {
                                            channel.sendResponse(new BytesRestResponse(channel, exception));
                                        } catch (IOException e) {
                                            log.warn("Fail to send out failure message of exception", e);
                                        }
                                    }));
                                });
                            } else {
                                channel.sendResponse(new BytesRestResponse(RestStatus.OK, "No running task found"));
                            }
                        }, exception -> { log.error("Fail to list tasks failed.", exception); }));
                    }
                } catch (Exception e) {
                    log.error("Fail to parse task " + taskId, e);
                    channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                }
            }, exception -> {
                log.error("Fail to get task " + taskId, exception);
                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, exception.getMessage()));
            }));

        };
    }

    private void onCreateMappingsResponse(CreateIndexResponse response, RestChannel channel, AnomalyDetectionTask task) throws IOException {
        if (response.isAcknowledged()) {
            indexAnomalyDetectionTaskExecution(task, channel);
        } else {
            channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "Create task execution index not acknowledged"));
        }
    }

    private void indexAnomalyDetectionTaskExecution(AnomalyDetectionTask task, RestChannel channel) throws IOException {
        AnomalyDetectionTaskExecution taskExecution = new AnomalyDetectionTaskExecution(
            task.getTaskId(),
            task.getDetectorId(),
            null,
            task.getDataStartTime(),
            task.getDataEndTime(),
            Instant.now(),
            null,
            task.getDataStartTime(),
            "running",
            null,
            0,
            Instant.now()
        );
        IndexRequest indexRequest = new IndexRequest(AnomalyDetectionTaskExecution.ANOMALY_DETECTION_TASK_EXECUTION_INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(taskExecution.toXContent(channel.newBuilder(), XCONTENT_WITH_TYPE));
        client.index(indexRequest, indexAnomalyDetectorResponse(taskExecution, channel));
    }

    private ActionListener<IndexResponse> indexAnomalyDetectorResponse(AnomalyDetectionTaskExecution taskExecution, RestChannel channel) {

        return new RestActionListener<IndexResponse>(channel) {
            @Override
            public void processResponse(IndexResponse response) throws Exception {
                if (response.getShardInfo().getSuccessful() < 1) {
                    channel
                        .sendResponse(
                            new BytesRestResponse(response.status(), response.toXContent(channel.newErrorBuilder(), EMPTY_PARAMS))
                        );
                }

                startTaskExecution(taskExecution, response.getId(), channel);
            }
        };
    }

    private void startTaskExecution(AnomalyDetectionTaskExecution task, String taskExecutionId, RestChannel channel) throws IOException {
        Runnable runnable = () -> { runAnomalyDetectionTask(task, taskExecutionId); };
        // TODO: catch exception, set task execution as failed.
        ExecutorService executor = threadPool.executor(AD_BATCh_TASK_THREAD_POOL_NAME);
        executor.submit(runnable);
        // TODO: need to return real task execution id
        XContentBuilder builder = channel.newBuilder().startObject().field("taskExecutionId", taskExecutionId).endObject();
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
    }

    protected void runAnomalyDetectionTask(AnomalyDetectionTaskExecution taskExecution, String taskExecutionId) {
        try {
            AnomalyResultBatchRequest request = new AnomalyResultBatchRequest(
                taskExecution.getDetectorId(),
                taskExecutionId,
                taskExecution.getDataStartTime().toEpochMilli(),
                taskExecution.getDataEndTime().toEpochMilli()
            );
            client
                .execute(
                    AnomalyResultBatchAction.INSTANCE,
                    request,
                    ActionListener
                        .wrap(
                            response -> { log.info(response.getMessage()); },
                            exception -> {
                                // TODO: log error message in task execution
                                log.error("Fail to execute batch task action " + taskExecution.getTaskId(), exception);
                            }
                        )
                );
        } catch (Exception e) {
            // TODO: log error message in task execution
            log.error("Failed to execute task " + taskExecution.getTaskId(), e);
        }
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTION_TASKS_URI, TASK_ID, START_JOB)
                ),

                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTION_TASKS_URI, TASK_ID, STOP_JOB)
                )
            );
    }
}
