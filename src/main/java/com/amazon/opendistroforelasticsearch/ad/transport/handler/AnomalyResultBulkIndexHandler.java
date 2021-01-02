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

package com.amazon.opendistroforelasticsearch.ad.transport.handler;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.List;
import java.util.Locale;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

public class AnomalyResultBulkIndexHandler extends AnomalyIndexHandler<AnomalyResult> {
    private static final Logger LOG = LogManager.getLogger(AnomalyResultBulkIndexHandler.class);

    static final String CANNOT_SAVE_ERR_MSG = "Cannot save %s due to write block.";

    private AnomalyDetectionIndices anomalyDetectionIndices;

    public AnomalyResultBulkIndexHandler(
        Client client,
        Settings settings,
        ThreadPool threadPool,
        String indexName,
        Consumer<ActionListener<CreateIndexResponse>> createIndex,
        BooleanSupplier indexExists,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        ClusterService clusterService,
        AnomalyDetectionIndices anomalyDetectionIndices
    ) {
        super(client, settings, threadPool, indexName, createIndex, indexExists, clientUtil, indexUtils, clusterService);
        this.anomalyDetectionIndices = anomalyDetectionIndices;
    }

    public void bulkIndexAnomalyResult(List<AnomalyResult> anomalyResults, ActionListener<BulkResponse> listener) {
        try {
            if (super.indexUtils
                .checkIndicesBlocked(clusterService.state(), ClusterBlockLevel.WRITE, CommonName.ANOMALY_RESULT_INDEX_ALIAS)) {
                LOG.warn(CANNOT_SAVE_ERR_MSG);
                return;
            }
            if (!anomalyDetectionIndices.doesAnomalyResultIndexExist()) {
                anomalyDetectionIndices
                    .initAnomalyResultIndexDirectly(
                        ActionListener
                            .wrap(
                                initResponse -> onCreateAnomalyResultIndexResponse(
                                    initResponse,
                                    anomalyResults.get(0).getDetectorId(),
                                    anomalyResults,
                                    result -> bulkSaveDetectorResult(result, listener)
                                ),
                                exception -> {
                                    if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                                        // It is possible the index has been created while we sending the create request
                                        bulkSaveDetectorResult(anomalyResults, listener);
                                    } else {
                                        throw new AnomalyDetectionException(
                                            anomalyResults.get(0).getDetectorId(),
                                            "Unexpected error creating anomaly result index",
                                            exception
                                        );
                                    }
                                }
                            )
                    );
            } else {
                bulkSaveDetectorResult(anomalyResults, listener);
            }
        } catch (Exception e) {
            throw new AnomalyDetectionException(
                anomalyResults.get(0).getDetectorId(),
                String
                    .format(
                        Locale.ROOT,
                        "Error in saving anomaly index for ID %s from %s to %s",
                        anomalyResults.get(0).getDetectorId(),
                        anomalyResults.get(0).getDataStartTime(),
                        anomalyResults.get(0).getDataEndTime()
                    ),
                e
            );
        }
    }

    private void bulkSaveDetectorResult(List<AnomalyResult> anomalyResults, ActionListener<BulkResponse> listener) {
        LOG.info("Start to bulk save anomaly result...");
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        anomalyResults.forEach(anomalyResult -> {
            try {
                try (XContentBuilder builder = jsonBuilder()) {
                    IndexRequest indexRequest = new IndexRequest(CommonName.ANOMALY_RESULT_INDEX_ALIAS)
                        .source(anomalyResult.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
                    bulkRequestBuilder.add(indexRequest);
                } catch (Exception e) {
                    LOG.error("Failed to save anomaly result", e);
                    throw new AnomalyDetectionException(anomalyResults.get(0).getDetectorId(), "Cannot save result");
                }
            } catch (Exception e) {
                LOG.error("Failed to bulk save anomaly result", e);
            }
        });
        if (anomalyResults.size() > 1) {
            client.bulk(bulkRequestBuilder.request(), ActionListener.wrap(res -> {
                LOG.info("bulk index ad result successfully, took: {}", res.getTook().duration());
                listener.onResponse(res);
            }, e -> {
                LOG.error("bulk index ad result failed", e);
                // TODO: add error handling
                listener.onFailure(e);
            }));
        }
        LOG.info("Finish to bulk save anomaly result...");
    }

    private <T> void onCreateAnomalyResultIndexResponse(
        CreateIndexResponse response,
        String detectorId,
        T anomalyResult,
        Consumer<T> func
    ) {
        if (response.isAcknowledged()) {
            func.accept(anomalyResult);
        } else {
            throw new AnomalyDetectionException(detectorId, "Creating anomaly result index with mappings call not acknowledged.");
        }
    }

}
