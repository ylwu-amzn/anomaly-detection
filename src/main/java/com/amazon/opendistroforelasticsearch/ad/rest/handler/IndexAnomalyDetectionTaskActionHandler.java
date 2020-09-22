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

package com.amazon.opendistroforelasticsearch.ad.rest.handler;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTask.ANOMALY_DETECTION_TASK_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.START_JOB;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.STOP_JOB;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;

import java.io.IOException;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTask;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

/**
 * Anomaly detector job REST action handler to process POST/PUT request.
 */
public class IndexAnomalyDetectionTaskActionHandler extends AbstractActionHandler {

    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final Long seqNo;
    private final Long primaryTerm;
    private final WriteRequest.RefreshPolicy refreshPolicy;
    private final ClusterService clusterService;

    private final Logger logger = LogManager.getLogger(IndexAnomalyDetectionTaskActionHandler.class);
    private final TimeValue requestTimeout;
    private final AnomalyDetectorActionHandler handler = new AnomalyDetectorActionHandler();

    private final AnomalyDetectionTask task;
    private final Integer maxAnomalyDetectionTasks;
    private final IndexAnomalyDetectorJobActionHandler jobHandler;
    private final String rawPath;

    public IndexAnomalyDetectionTaskActionHandler(
        ClusterService clusterService,
        NodeClient client,
        RestChannel channel,
        AnomalyDetectionIndices anomalyDetectionIndices,
        Integer maxAnomalyDetectionTasks,
        AnomalyDetectionTask task,
        Long seqNo,
        Long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        TimeValue requestTimeout
    ) {
        super(client, channel);
        this.clusterService = clusterService;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.maxAnomalyDetectionTasks = maxAnomalyDetectionTasks;
        this.task = task;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.refreshPolicy = refreshPolicy;
        this.requestTimeout = requestTimeout;
        this.jobHandler = null;
        this.rawPath = null;
    }

    public IndexAnomalyDetectionTaskActionHandler(
        ClusterService clusterService,
        NodeClient client,
        RestChannel channel,
        AnomalyDetectionIndices anomalyDetectionIndices,
        Integer maxAnomalyDetectionTasks,
        AnomalyDetectionTask task,
        Long seqNo,
        Long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        TimeValue requestTimeout,
        IndexAnomalyDetectorJobActionHandler jobHandler,
        String rawPath
    ) {
        super(client, channel);
        this.clusterService = clusterService;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.maxAnomalyDetectionTasks = maxAnomalyDetectionTasks;
        this.task = task;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.refreshPolicy = refreshPolicy;
        this.requestTimeout = requestTimeout;
        this.jobHandler = jobHandler;
        this.rawPath = rawPath;
    }

    /**
     * Start anomaly detector job.
     * 1.If job not exists, create new job.
     * 2.If job exists: a). if job enabled, return error message; b). if job disabled, enable job.
     *
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectionTaskMappings()}
     */
    public void start() throws IOException {
        if (!anomalyDetectionIndices.doesAnomalyDetectionTaskIndexExist()) {
            anomalyDetectionIndices
                .initAnomalyDetectionTaskIndexDirectly(
                    ActionListener.wrap(response -> onCreateMappingsResponse(response), exception -> onFailure(exception))
                );
        } else {
            prepareAnomalyDetectionTaskIndexing();
        }
    }

    private void onCreateMappingsResponse(CreateIndexResponse response) throws IOException {
        if (response.isAcknowledged()) {
            logger.info("Created {} with mappings.", ANOMALY_DETECTION_TASK_INDEX);
            prepareAnomalyDetectionTaskIndexing();
        } else {
            logger.warn("Created {} with mappings call not acknowledged.", ANOMALY_DETECTION_TASK_INDEX);
            channel
                .sendResponse(
                    new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, response.toXContent(channel.newErrorBuilder(), EMPTY_PARAMS))
                );
        }
    }

    private void prepareAnomalyDetectionTaskIndexing() {
        if (channel.request().method() == RestRequest.Method.PUT) {
            updateAnomalyDetectionTask(client, task.getTaskId());
        } else {
            createAnomalyDetectionTask();
        }
    }

    private void createAnomalyDetectionTask() {
        logger.info("Start to create anomaly detection task {}", task);
        try {
            QueryBuilder query = QueryBuilders.matchAllQuery();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(0).timeout(requestTimeout);

            SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTION_TASK_INDEX).source(searchSourceBuilder);

            client
                .search(searchRequest, ActionListener.wrap(response -> onSearchTaskResponse(response), exception -> onFailure(exception)));
        } catch (Exception e) {
            onFailure(e);
        }
    }

    private void onSearchTaskResponse(SearchResponse response) throws IOException {
        if (response.getHits().getTotalHits().value >= maxAnomalyDetectionTasks) {
            String errorMsg = "Can't create anomaly detection tasks more than " + maxAnomalyDetectionTasks;
            logger.error(errorMsg);
            onFailure(new IllegalArgumentException(errorMsg));
        } else {
            indexAnomalyDetectionTask();
        }
    }

    private void updateAnomalyDetectionTask(NodeClient client, String taskId) {
        GetRequest request = new GetRequest(ANOMALY_DETECTION_TASK_INDEX, taskId);
        client
            .get(request, ActionListener.wrap(response -> onGetAnomalyDetectionTaskResponse(response), exception -> onFailure(exception)));
    }

    private void onGetAnomalyDetectionTaskResponse(GetResponse response) throws IOException {
        if (!response.isExists()) {
            XContentBuilder builder = channel
                .newErrorBuilder()
                .startObject()
                .field("Message", "AnomalyDetectionTask is not found with id: " + task.getTaskId())
                .endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, response.toXContent(builder, EMPTY_PARAMS)));
            return;
        }
        indexAnomalyDetectionTask();
    }

    public void indexAnomalyDetectionTask() throws IOException {
        // TODO: need to update lastUpdateTime to now.
        AnomalyDetectionTask newTask = task;
        logger.info("Index task: {}", newTask.toString());
        IndexRequest indexRequest = new IndexRequest(ANOMALY_DETECTION_TASK_INDEX)
            .setRefreshPolicy(refreshPolicy)
            .source(newTask.toXContent(channel.newBuilder(), XCONTENT_WITH_TYPE))
            .setIfSeqNo(seqNo)
            .setIfPrimaryTerm(primaryTerm)
            .timeout(requestTimeout);
        if (!Strings.isEmpty(task.getTaskId())) {
            indexRequest.id(task.getTaskId());
        }
        client.index(indexRequest, indexAnomalyDetectorResponse());
    }

    private ActionListener<IndexResponse> indexAnomalyDetectorResponse() {
        return new RestActionListener<IndexResponse>(channel) {
            @Override
            public void processResponse(IndexResponse response) throws Exception {
                if (response.getShardInfo().getSuccessful() < 1) {
                    channel
                        .sendResponse(
                            new BytesRestResponse(response.status(), response.toXContent(channel.newErrorBuilder(), EMPTY_PARAMS))
                        );
                }

                XContentBuilder builder = channel
                    .newBuilder()
                    .startObject()
                    .field(RestHandlerUtils._ID, response.getId())
                    .field(RestHandlerUtils._VERSION, response.getVersion())
                    .field(RestHandlerUtils._SEQ_NO, response.getSeqNo())
                    .field(RestHandlerUtils._PRIMARY_TERM, response.getPrimaryTerm())
                    .field("anomaly_detection_task", task)
                    .endObject();

                BytesRestResponse restResponse = new BytesRestResponse(response.status(), builder);
                if (response.status() == RestStatus.CREATED) {
                    String location = String
                        .format(Locale.ROOT, "%s/%s", AnomalyDetectorPlugin.AD_BASE_DETECTION_TASKS_URI, response.getId());
                    restResponse.addHeader("Location", location);
                }
                if (rawPath != null) {
                    if (rawPath.endsWith(START_JOB)) {
                        jobHandler.startAnomalyDetectorJob();
                    } else if (rawPath.endsWith(STOP_JOB)) {
                        jobHandler.stopAnomalyDetectorJob(task.getTaskId());
                    }
                } else {
                    channel.sendResponse(restResponse);
                }
            }
        };
    }

}
