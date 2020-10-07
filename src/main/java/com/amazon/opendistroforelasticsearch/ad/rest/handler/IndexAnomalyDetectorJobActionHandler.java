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

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.createXContentParser;
import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.action.DocWriteResponse.Result.UPDATED;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.transport.StopDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.StopDetectorRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.StopDetectorResponse;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule;

/**
 * Anomaly detector job REST action handler to process POST/PUT request.
 */
public class IndexAnomalyDetectorJobActionHandler extends AbstractActionHandler {

    static final String ONLY_ONE_CATEGORICAL_FIELD_ERR_MSG = "We can have only one categorical field.";
    public static final String CATEGORICAL_FIELD_TYPE_ERR_MSG = "A categorical field must be of type keyword or ip.";
    static final String NOT_FOUND_ERR_MSG = "Cannot found the categorical field %s";

    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final String detectorId;
    private final Long seqNo;
    private final Long primaryTerm;

    private final Logger logger = LogManager.getLogger(IndexAnomalyDetectorJobActionHandler.class);
    private final TimeValue requestTimeout;

    /**
     * Constructor function.
     *
     * @param client                  ES node client that executes actions on the local node
     * @param channel                 ES channel used to construct bytes / builder based outputs, and send responses
     * @param anomalyDetectionIndices anomaly detector index manager
     * @param detectorId              detector identifier
     * @param seqNo                   sequence number of last modification
     * @param primaryTerm             primary term of last modification
     * @param requestTimeout          request time out configuration
     */
    public IndexAnomalyDetectorJobActionHandler(
        NodeClient client,
        RestChannel channel,
        AnomalyDetectionIndices anomalyDetectionIndices,
        String detectorId,
        Long seqNo,
        Long primaryTerm,
        TimeValue requestTimeout
    ) {
        super(client, channel);
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.detectorId = detectorId;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.requestTimeout = requestTimeout;
    }

    /**
     * Start anomaly detector job.
     * 1.If job not exists, create new job.
     * 2.If job exists: a). if job enabled, return error message; b). if job disabled, enable job.
     *
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorJobMappings}
     */
    public void startAnomalyDetectorJob() throws IOException {
        if (!anomalyDetectionIndices.doesAnomalyDetectorJobIndexExist()) {
            anomalyDetectionIndices
                .initAnomalyDetectorJobIndex(
                    ActionListener.wrap(response -> onCreateMappingsResponse(response), exception -> onFailure(exception))
                );
        } else {
            prepareAnomalyDetectorJobIndexing();
        }
    }

    private void onCreateMappingsResponse(CreateIndexResponse response) throws IOException {
        if (response.isAcknowledged()) {
            logger.info("Created {} with mappings.", ANOMALY_DETECTORS_INDEX);
            prepareAnomalyDetectorJobIndexing();
        } else {
            logger.warn("Created {} with mappings call not acknowledged.", ANOMALY_DETECTORS_INDEX);
            channel
                .sendResponse(
                    new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, response.toXContent(channel.newErrorBuilder(), EMPTY_PARAMS))
                );
        }
    }

    private void prepareAnomalyDetectorJobIndexing() {
        GetRequest getRequest = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).id(detectorId);
        client.get(getRequest, ActionListener.wrap(response -> onGetAnomalyDetectorResponse(response), exception -> onFailure(exception)));
    }

    private void onGetAnomalyDetectorResponse(GetResponse response) throws IOException {
        if (!response.isExists()) {
            XContentBuilder builder = channel
                .newErrorBuilder()
                .startObject()
                .field("Message", "AnomalyDetector is not found with id: " + detectorId)
                .endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, response.toXContent(builder, EMPTY_PARAMS)));
            return;
        }
        try (XContentParser parser = RestHandlerUtils.createXContentParser(channel, response.getSourceAsBytesRef())) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());

            if (detector.getFeatureAttributes().size() == 0) {
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "Can't start detector job as no features configured"));
                return;
            }

            validateCategoricalField(detector);
        } catch (IOException e) {
            String message = "Failed to parse anomaly detector job " + detectorId;
            logger.error(message, e);
            channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, message));
        }
    }

    @SuppressWarnings("unchecked")
    private void validateCategoricalField(AnomalyDetector detector) {
        List<String> categoryField = detector.getCategoryField();
        if (categoryField == null) {
            writeJob(detector);
            return;
        }

        // we only support one categorical field
        // If there is more than 1 field or none, AnomalyDetector's constructor
        // throws IllegalArgumentException before reaching this line
        if (categoryField.size() != 1) {
            channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, ONLY_ONE_CATEGORICAL_FIELD_ERR_MSG));
            return;
        }

        String categoryField0 = categoryField.get(0);

        GetFieldMappingsRequest getMappingsRequest = new GetFieldMappingsRequest();
        getMappingsRequest.indices(detector.getIndices().toArray(new String[0])).fields(categoryField.toArray(new String[0]));
        getMappingsRequest.indicesOptions(IndicesOptions.strictExpand());

        ActionListener<GetFieldMappingsResponse> mappingsListener = ActionListener.wrap(getMappingsResponse -> {
            // example getMappingsResponse:
            // GetFieldMappingsResponse{mappings={server-metrics={_doc={service=FieldMappingMetadata{fullName='service',
            // source=org.elasticsearch.common.bytes.BytesArray@7ba87dbd}}}}}
            boolean foundField = false;
            Map<String, Map<String, Map<String, FieldMappingMetadata>>> mappingsByIndex = getMappingsResponse.mappings();

            for (Map<String, Map<String, FieldMappingMetadata>> mappingsByType : mappingsByIndex.values()) {
                for (Map<String, FieldMappingMetadata> mappingsByField : mappingsByType.values()) {
                    for (Map.Entry<String, FieldMappingMetadata> field2Metadata : mappingsByField.entrySet()) {
                        FieldMappingMetadata fieldMetadata = field2Metadata.getValue();

                        if (fieldMetadata != null) {
                            Object metadata = fieldMetadata.sourceAsMap().get(categoryField0);
                            if (metadata != null && metadata instanceof Map) {
                                foundField = true;
                                Map<String, Object> metadataMap = (Map<String, Object>) metadata;
                                String typeName = (String) metadataMap.get(CommonName.TYPE);
                                if (!typeName.equals(CommonName.KEYWORD_TYPE) && !typeName.equals(CommonName.IP_TYPE)) {
                                    channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, CATEGORICAL_FIELD_TYPE_ERR_MSG));
                                    return;
                                }
                            }
                        }
                    }
                }
            }

            if (foundField == false) {
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, String.format(NOT_FOUND_ERR_MSG, categoryField0)));
                return;
            }

            writeJob(detector);
        }, error -> {
            String message = String.format("Fail to get the index mapping of %s", detector.getIndices());
            logger.error(message, error);
            channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, message));
        });

        client.execute(GetFieldMappingsAction.INSTANCE, getMappingsRequest, mappingsListener);
    }

    private void writeJob(AnomalyDetector detector) {
        IntervalTimeConfiguration interval = (IntervalTimeConfiguration) detector.getDetectionInterval();
        Schedule schedule = new IntervalSchedule(Instant.now(), (int) interval.getInterval(), interval.getUnit());
        Duration duration = Duration.of(interval.getInterval(), interval.getUnit());

        AnomalyDetectorJob job = new AnomalyDetectorJob(
            detector.getDetectorId(),
            schedule,
            detector.getWindowDelay(),
            true,
            Instant.now(),
            null,
            Instant.now(),
            duration.getSeconds()
        );

        getAnomalyDetectorJobForWrite(job);
    }

    private void getAnomalyDetectorJobForWrite(AnomalyDetectorJob job) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);

        client
            .get(
                getRequest,
                ActionListener.wrap(response -> onGetAnomalyDetectorJobForWrite(response, job), exception -> onFailure(exception))
            );
    }

    private void onGetAnomalyDetectorJobForWrite(GetResponse response, AnomalyDetectorJob job) throws IOException {
        if (response.isExists()) {
            try (XContentParser parser = createXContentParser(channel, response.getSourceAsBytesRef())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                AnomalyDetectorJob currentAdJob = AnomalyDetectorJob.parse(parser);
                if (currentAdJob.isEnabled()) {
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, "Anomaly detector job is already running: " + detectorId));
                    return;
                } else {
                    AnomalyDetectorJob newJob = new AnomalyDetectorJob(
                        job.getName(),
                        job.getSchedule(),
                        job.getWindowDelay(),
                        job.isEnabled(),
                        Instant.now(),
                        currentAdJob.getDisabledTime(),
                        Instant.now(),
                        job.getLockDurationSeconds()
                    );
                    indexAnomalyDetectorJob(newJob, null);
                }
            } catch (IOException e) {
                String message = "Failed to parse anomaly detector job " + job.getName();
                logger.error(message, e);
                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, message));
            }
        } else {
            indexAnomalyDetectorJob(job, null);
        }
    }

    private void indexAnomalyDetectorJob(AnomalyDetectorJob job, AnomalyDetectorFunction function) throws IOException {
        IndexRequest indexRequest = new IndexRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(job.toXContent(channel.newBuilder(), XCONTENT_WITH_TYPE))
            .setIfSeqNo(seqNo)
            .setIfPrimaryTerm(primaryTerm)
            .timeout(requestTimeout)
            .id(detectorId);
        client
            .index(
                indexRequest,
                ActionListener.wrap(response -> onIndexAnomalyDetectorJobResponse(response, function), exception -> onFailure(exception))
            );
    }

    private void onIndexAnomalyDetectorJobResponse(IndexResponse response, AnomalyDetectorFunction function) throws IOException {
        if (response == null || (response.getResult() != CREATED && response.getResult() != UPDATED)) {
            channel.sendResponse(new BytesRestResponse(response.status(), response.toXContent(channel.newErrorBuilder(), EMPTY_PARAMS)));
            return;
        }
        if (function != null) {
            function.execute();
        } else {
            XContentBuilder builder = channel
                .newBuilder()
                .startObject()
                .field(RestHandlerUtils._ID, response.getId())
                .field(RestHandlerUtils._VERSION, response.getVersion())
                .field(RestHandlerUtils._SEQ_NO, response.getSeqNo())
                .field(RestHandlerUtils._PRIMARY_TERM, response.getPrimaryTerm())
                .endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * Stop anomaly detector job.
     * 1.If job not exists, return error message
     * 2.If job exists: a).if job state is disabled, return error message; b).if job state is enabled, disable job.
     *
     * @param detectorId detector identifier
     */
    public void stopAnomalyDetectorJob(String detectorId) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);

        client.get(getRequest, ActionListener.wrap(response -> {
            if (response.isExists()) {
                try (XContentParser parser = createXContentParser(channel, response.getSourceAsBytesRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
                    if (!job.isEnabled()) {
                        channel
                            .sendResponse(new BytesRestResponse(RestStatus.OK, "Anomaly detector job is already stopped: " + detectorId));
                        return;
                    } else {
                        AnomalyDetectorJob newJob = new AnomalyDetectorJob(
                            job.getName(),
                            job.getSchedule(),
                            job.getWindowDelay(),
                            false,
                            job.getEnabledTime(),
                            Instant.now(),
                            Instant.now(),
                            job.getLockDurationSeconds()
                        );
                        indexAnomalyDetectorJob(
                            newJob,
                            () -> client
                                .execute(
                                    StopDetectorAction.INSTANCE,
                                    new StopDetectorRequest(detectorId),
                                    stopAdDetectorListener(channel, detectorId)
                                )
                        );
                    }
                } catch (IOException e) {
                    String message = "Failed to parse anomaly detector job " + detectorId;
                    logger.error(message, e);
                    channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, message));
                }
            } else {
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "Anomaly detector job not exist: " + detectorId));
            }
        }, exception -> onFailure(exception)));
    }

    private ActionListener<StopDetectorResponse> stopAdDetectorListener(RestChannel channel, String detectorId) {
        return new ActionListener<StopDetectorResponse>() {
            @Override
            public void onResponse(StopDetectorResponse stopDetectorResponse) {
                if (stopDetectorResponse.success()) {
                    logger.info("AD model deleted successfully for detector {}", detectorId);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, "Stopped detector: " + detectorId));
                } else {
                    logger.error("Failed to delete AD model for detector {}", detectorId);
                    channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "Failed to delete AD model"));
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to delete AD model for detector " + detectorId, e);
                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "Failed to execute stop detector action"));
            }
        };
    }

}
