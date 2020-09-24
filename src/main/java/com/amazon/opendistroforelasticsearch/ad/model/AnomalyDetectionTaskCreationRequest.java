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

package com.amazon.opendistroforelasticsearch.ad.model;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;

import org.elasticsearch.common.xcontent.XContentParser;

import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;

/**
 * An AnomalyDetectionTask is used to represent anomaly detection task related parameters.
 */
public class AnomalyDetectionTaskCreationRequest {

    private static final String NAME_FIELD = "name";
    private static final String DESCRIPTION_FIELD = "description";
    private static final String DETECTOR_ID_FIELD = "detector_id";
    private static final String DATA_START_TIME_FIELD = "data_start_time";
    public static final String DATA_END_TIME_FIELD = "data_end_time";

    private final String name;
    private final String description;
    private final String detectorId;
    private final Instant dataStartTime;
    private final Instant dataEndTime;

    /**
     * Constructor function.
     * @param name task name
     * @param description description of detection task
     * @param detectorId detector id
     * @param dataStartTime data start time
     * @param dataEndTime data end time
     */
    public AnomalyDetectionTaskCreationRequest(
        String name,
        String description,
        String detectorId,
        Instant dataStartTime,
        Instant dataEndTime
    ) {
        this.name = name;
        this.description = description;
        this.detectorId = detectorId;
        this.dataStartTime = dataStartTime;
        this.dataEndTime = dataEndTime;
    }

    /**
     * Parse raw json content and given detector id into anomaly detector instance.
     *
     * @param parser                      json based content parser
     * @return anomaly detection task creation request instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AnomalyDetectionTaskCreationRequest parse(XContentParser parser) throws IOException {
        String name = null;
        String description = null;
        String detectorId = null;
        Instant dataStartTime = null;
        Instant dataEndTime = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case NAME_FIELD:
                    name = parser.text();
                    break;
                case DESCRIPTION_FIELD:
                    description = parser.text();
                    break;
                case DETECTOR_ID_FIELD:
                    detectorId = parser.text();
                    break;
                case DATA_START_TIME_FIELD:
                    dataStartTime = ParseUtils.toInstant(parser);
                    break;
                case DATA_END_TIME_FIELD:
                    dataEndTime = ParseUtils.toInstant(parser);
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new AnomalyDetectionTaskCreationRequest(name, description, detectorId, dataStartTime, dataEndTime);
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public Instant getDataStartTime() {
        return dataStartTime;
    }

    public Instant getDataEndTime() {
        return dataEndTime;
    }
}
