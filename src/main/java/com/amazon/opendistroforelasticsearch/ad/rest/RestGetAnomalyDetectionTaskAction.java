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

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectionTask.ANOMALY_DETECTION_TASK_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.TASK_ID;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * This class consists of the REST handler to retrieve an anomaly detector.
 */
public class RestGetAnomalyDetectionTaskAction extends BaseRestHandler {

    private static final String GET_ANOMALY_DETECTION_TASK_ACTION = "get_anomaly_detection_task";
    private static final Logger logger = LogManager.getLogger(RestGetAnomalyDetectionTaskAction.class);

    @Override
    public String getName() {
        return GET_ANOMALY_DETECTION_TASK_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        String taskId = request.param(TASK_ID);
        MultiGetRequest.Item adItem = new MultiGetRequest.Item(ANOMALY_DETECTION_TASK_INDEX, taskId)
                .version(RestActions.parseVersion(request));
        MultiGetRequest multiGetRequest = new MultiGetRequest().add(adItem);

        return channel -> client.multiGet(multiGetRequest, onMultiGetResponse(channel, false, taskId));
    }

    private ActionListener<MultiGetResponse> onMultiGetResponse(RestChannel channel, boolean returnJob, String taskId) {
        return new RestResponseListener<MultiGetResponse>(channel) {
            @Override
            public RestResponse buildResponse(MultiGetResponse multiGetResponse) throws Exception {
                MultiGetItemResponse[] responses = multiGetResponse.getResponses();
                XContentBuilder builder = null;
                AnomalyDetectionTask task = null;
                //TODO: support get task execution info
                for (MultiGetItemResponse response : responses) {
                    if (ANOMALY_DETECTION_TASK_INDEX.equals(response.getIndex())) {
                        if (response.getResponse() == null || !response.getResponse().isExists()) {
                            return new BytesRestResponse(RestStatus.NOT_FOUND, "Can't find anomaly detection task with id: " + taskId);
                        }
                        builder = channel
                            .newBuilder()
                            .startObject()
                            .field(RestHandlerUtils._ID, response.getId())
                            .field(RestHandlerUtils._VERSION, response.getResponse().getVersion())
                            .field(RestHandlerUtils._PRIMARY_TERM, response.getResponse().getPrimaryTerm())
                            .field(RestHandlerUtils._SEQ_NO, response.getResponse().getSeqNo());
                        if (!response.getResponse().isSourceEmpty()) {
                            try (
                                XContentParser parser = RestHandlerUtils
                                    .createXContentParser(channel, response.getResponse().getSourceAsBytesRef())
                            ) {
                                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                                task = parser.namedObject(AnomalyDetectionTask.class, AnomalyDetectionTask.PARSE_FIELD_NAME, null);
                            } catch (Exception e) {
                                return buildInternalServerErrorResponse(e, "Failed to parse find anomaly detection task with id: " + taskId);
                            }
                        }
                    }
                }

                builder.field(RestHandlerUtils.ANOMALY_DETECTION_TASK, task);
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        };
    }

    private RestResponse buildInternalServerErrorResponse(Exception e, String errorMsg) {
        logger.error(errorMsg, e);
        return new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, errorMsg);
    }

    @Override
    public List<Route> routes() {
        String path = String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTION_TASKS_URI, TASK_ID);
        return ImmutableList
            .of(
                new Route(RestRequest.Method.GET, path),
                new Route(RestRequest.Method.HEAD, path)
            );
    }
}
