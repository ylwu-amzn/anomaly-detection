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
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.amazon.opendistroforelasticsearch.ad.annotation.Generated;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;
import com.google.common.base.Objects;

/**
 * An AnomalyDetectionTask is used to represent anomaly detection task related parameters.
 */
public class AnomalyDetectionTask implements ToXContentObject {

    public static final String PARSE_FIELD_NAME = "AnomalyDetectionTask";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        AnomalyDetectionTask.class,
        new ParseField(PARSE_FIELD_NAME),
        it -> parse(it)
    );
    public static final String NO_ID = "";
    public static final String ANOMALY_DETECTION_TASK_INDEX = ".opendistro-anomaly-detection-tasks";
    public static final String TYPE = "_doc";

    private static final String ID_FIELD = "id";
    private static final String NAME_FIELD = "name";
    private static final String DESCRIPTION_FIELD = "description";
    // private static final String CHECKPOINT_ID = "checkpoint_id";
    private static final String SCHEMA_VERSION_FIELD = "schema_version";
    private static final String SCHEDULE_FIELD = "schedule";
    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    private static final String DATA_START_TIME_FIELD = "data_start_time";
    public static final String DATA_END_TIME_FIELD = "data_end_time";
    public static final String START_TIME_FIELD = "start_time";
    public static final String UI_METADATA_FIELD = "ui_metadata";

    // fields from detector
    private static final String TIMEFIELD_FIELD = "time_field";
    private static final String INDICES_FIELD = "indices";
    private static final String FILTER_QUERY_FIELD = "filter_query";
    private static final String FEATURE_ATTRIBUTES_FIELD = "feature_attributes";
    private static final String DETECTION_INTERVAL_FIELD = "detection_interval";
    private static final String WINDOW_DELAY_FIELD = "window_delay";
    private static final String SHINGLE_SIZE_FIELD = "shingle_size";

    private static Logger logger = LogManager.getLogger(AnomalyDetectionTask.class);

    private final String taskId;
    private final Long version;
    private final String name;
    private final String description;
    private final Instant dataStartTime;
    private final Instant dataEndTime;
    private final Instant startTime;
    private final TimeConfiguration schedule;
    private final Map<String, Object> uiMetadata;
    private final Integer schemaVersion;
    private final Instant lastUpdateTime;

    // fields from detector
    private final String timeField;
    private final List<String> indices;
    private final QueryBuilder filterQuery;
    private final List<Feature> featureAttributes;
    private final TimeConfiguration detectionInterval;
    private final TimeConfiguration windowDelay;
    private final Integer shingleSize;

    /**
     * Constructor function.
     * @param version           detector document version
     * @param name              detector name
     * @param description       description of detector
     * @param uiMetadata        metadata used by Kibana
     * @param schemaVersion     anomaly detector index mapping version
     * @param lastUpdateTime    detector's last update time
     * @param timeField  time field
     * @param indices indices used as detector input
     * @param filterQuery detector filter query
     * @param featureAttributes detector feature attributes
     * @param detectionInterval detecting interval
     * @param windowDelay max delay window for realtime data
     * @param shingleSize number of the most recent time intervals to form a shingled data point
     */
    public AnomalyDetectionTask(
        String taskId,
        Long version,
        String name,
        String description,
        TimeConfiguration schedule,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant startTime,
        Map<String, Object> uiMetadata,
        Integer schemaVersion,
        Instant lastUpdateTime,
        String timeField,
        List<String> indices,
        QueryBuilder filterQuery,
        List<Feature> featureAttributes,
        TimeConfiguration detectionInterval,
        TimeConfiguration windowDelay,
        Integer shingleSize
    ) {
        this.timeField = timeField;
        this.indices = indices;
        this.filterQuery = filterQuery;
        this.featureAttributes = featureAttributes;
        this.detectionInterval = detectionInterval;
        this.windowDelay = windowDelay;
        this.shingleSize = shingleSize;
        if (Strings.isBlank(name)) {
            throw new IllegalArgumentException("Detection task name should be set");
        }
        this.taskId = taskId;
        this.version = version;
        this.name = name;
        this.description = description;
        this.schedule = schedule;
        this.dataStartTime = dataStartTime;
        this.dataEndTime = dataEndTime;
        this.startTime = startTime;
        this.uiMetadata = uiMetadata;
        this.schemaVersion = schemaVersion;
        this.lastUpdateTime = lastUpdateTime;
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(NAME_FIELD, name)
            .field(DESCRIPTION_FIELD, description)
            .field(SCHEMA_VERSION_FIELD, schemaVersion)
            .field(TIMEFIELD_FIELD, timeField)
            .field(INDICES_FIELD, indices.toArray())
            .field(FILTER_QUERY_FIELD, filterQuery)
            .field(DETECTION_INTERVAL_FIELD, detectionInterval)
            .field(WINDOW_DELAY_FIELD, windowDelay)
            .field(SHINGLE_SIZE_FIELD, shingleSize);
        if (featureAttributes != null) {
            xContentBuilder.field(FEATURE_ATTRIBUTES_FIELD, featureAttributes.toArray());
        }
        if (schedule != null) {
            xContentBuilder.field(SCHEDULE_FIELD, schedule);
        }
        if (dataStartTime != null) {
            xContentBuilder.field(DATA_START_TIME_FIELD, dataStartTime.toEpochMilli());
        }
        if (dataEndTime != null) {
            xContentBuilder.field(DATA_END_TIME_FIELD, dataEndTime.toEpochMilli());
        }
        if (startTime != null) {
            xContentBuilder.field(START_TIME_FIELD, startTime.toEpochMilli());
        }
        if (uiMetadata != null && !uiMetadata.isEmpty()) {
            xContentBuilder.field(UI_METADATA_FIELD, uiMetadata);
        }
        if (params.param(LAST_UPDATE_TIME_FIELD) != null) {
            xContentBuilder.timeField(LAST_UPDATE_TIME_FIELD, LAST_UPDATE_TIME_FIELD, Long.parseLong(params.param(LAST_UPDATE_TIME_FIELD)));
        } else if (lastUpdateTime != null) {
            xContentBuilder.timeField(LAST_UPDATE_TIME_FIELD, LAST_UPDATE_TIME_FIELD, lastUpdateTime.toEpochMilli());
        }

        return xContentBuilder.endObject();
    }

    /**
     * Parse raw json content into anomaly detector instance.
     *
     * @param parser json based content parser
     * @return anomaly detector instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AnomalyDetectionTask parse(XContentParser parser) throws IOException {
        return parse(parser, null);
    }

    public static AnomalyDetectionTask parse(XContentParser parser, String taskId) throws IOException {
        return parse(parser, taskId, null);
    }

    /**
     * Parse raw json content and given detector id into anomaly detector instance.
     *
     * @param parser     json based content parser
     * @param taskId     task id
     * @param version    detector document version
     * @return anomaly detector instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AnomalyDetectionTask parse(XContentParser parser, String taskId, Long version) throws IOException {
        return parse(parser, taskId, version, null, null, null);
    }

    /**
     * Parse raw json content and given detector id into anomaly detector instance.
     *
     * @param parser                      json based content parser
     * @param version                     detector document version
     * @param defaultDetectionInterval    default detection interval
     * @return anomaly detector instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AnomalyDetectionTask parse(
        XContentParser parser,
        String taskId,
        Long version,
        TimeValue defaultDetectionInterval,
        TimeValue defaultDetectionWindowDelay,
        Integer defaultShingleSize
    ) throws IOException {
        String id = null;
        String name = null;
        String description = null;
        TimeConfiguration schedule = defaultDetectionInterval == null
            ? null
            : new IntervalTimeConfiguration(defaultDetectionInterval.getMinutes(), ChronoUnit.MINUTES);
        Instant dataStartTime = null;
        Instant dataEndTime = null;
        Instant startTime = null;
        int schemaVersion = 0;
        Map<String, Object> uiMetadata = null;
        Instant lastUpdateTime = null;

        // fields from detector
        String timeField = null;
        List<String> indices = new ArrayList<>();
        QueryBuilder filterQuery = QueryBuilders.matchAllQuery();
        TimeConfiguration detectionInterval = defaultDetectionInterval == null
            ? null
            : new IntervalTimeConfiguration(defaultDetectionInterval.getMinutes(), ChronoUnit.MINUTES);
        TimeConfiguration windowDelay = defaultDetectionWindowDelay == null
            ? null
            : new IntervalTimeConfiguration(defaultDetectionWindowDelay.getSeconds(), ChronoUnit.SECONDS);
        Integer shingleSize = defaultShingleSize;
        List<Feature> features = new ArrayList<>();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case ID_FIELD:
                    id = parser.text();
                    break;
                case NAME_FIELD:
                    name = parser.text();
                    break;
                case DESCRIPTION_FIELD:
                    description = parser.text();
                    break;
                case UI_METADATA_FIELD:
                    uiMetadata = parser.map();
                    break;
                case SCHEMA_VERSION_FIELD:
                    schemaVersion = parser.intValue();
                    break;
                case SCHEDULE_FIELD:
                    schedule = TimeConfiguration.parse(parser);
                    break;
                case DATA_START_TIME_FIELD:
                    dataStartTime = ParseUtils.toInstant(parser);
                    break;
                case DATA_END_TIME_FIELD:
                    dataEndTime = ParseUtils.toInstant(parser);
                    break;
                case START_TIME_FIELD:
                    startTime = ParseUtils.toInstant(parser);
                    break;
                case LAST_UPDATE_TIME_FIELD:
                    lastUpdateTime = ParseUtils.toInstant(parser);
                    break;
                // detector's fields
                case TIMEFIELD_FIELD:
                    timeField = parser.text();
                    break;
                case INDICES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser::getTokenLocation);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        indices.add(parser.text());
                    }
                    break;
                case FILTER_QUERY_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
                    try {
                        filterQuery = parseInnerQueryBuilder(parser);
                    } catch (IllegalArgumentException e) {
                        if (!e.getMessage().contains("empty clause")) {
                            throw e;
                        }
                    }
                    break;
                case DETECTION_INTERVAL_FIELD:
                    detectionInterval = TimeConfiguration.parse(parser);
                    break;
                case FEATURE_ATTRIBUTES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser::getTokenLocation);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        features.add(Feature.parse(parser));
                    }
                    break;
                case WINDOW_DELAY_FIELD:
                    windowDelay = TimeConfiguration.parse(parser);
                    break;
                case SHINGLE_SIZE_FIELD:
                    shingleSize = parser.intValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        String parsedTaskId = Strings.isNotBlank(taskId) ? taskId : id;
        return new AnomalyDetectionTask(
            parsedTaskId,
            version,
            name,
            description,
            schedule,
            dataStartTime,
            dataEndTime,
            startTime,
            uiMetadata,
            schemaVersion,
            lastUpdateTime,
            timeField,
            indices,
            filterQuery,
            features,
            detectionInterval,
            windowDelay,
            shingleSize
        );
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AnomalyDetectionTask detector = (AnomalyDetectionTask) o;
        return Objects.equal(getName(), detector.getName())
            && Objects.equal(getDescription(), detector.getDescription())
            && Objects.equal(getSchedule(), detector.getSchedule())
            && Objects.equal(getDataStartTime(), detector.getDataStartTime())
            && Objects.equal(getDataEndTime(), detector.getDataEndTime())
            && Objects.equal(getSchemaVersion(), detector.getSchemaVersion());
    }

    public String getTaskId() {
        return taskId;
    }

    public Long getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public TimeConfiguration getSchedule() {
        return schedule;
    }

    public Instant getDataStartTime() {
        return dataStartTime;
    }

    public Instant getDataEndTime() {
        return dataEndTime;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Map<String, Object> getUiMetadata() {
        return uiMetadata;
    }

    public Integer getSchemaVersion() {
        return schemaVersion;
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    public String getTimeField() {
        return timeField;
    }

    public List<String> getIndices() {
        return indices;
    }

    public QueryBuilder getFilterQuery() {
        return filterQuery;
    }

    public List<Feature> getFeatureAttributes() {
        return featureAttributes;
    }

    public TimeConfiguration getDetectionInterval() {
        return detectionInterval;
    }

    public TimeConfiguration getWindowDelay() {
        return windowDelay;
    }

    public Integer getShingleSize() {
        return shingleSize;
    }

    public List<String> getEnabledFeatureIds() {
        return featureAttributes.stream().filter(Feature::getEnabled).map(Feature::getId).collect(Collectors.toList());
    }

    public List<String> getEnabledFeatureNames() {
        return featureAttributes.stream().filter(Feature::getEnabled).map(Feature::getName).collect(Collectors.toList());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(taskId, name, description, schedule, dataStartTime, dataEndTime, uiMetadata, schemaVersion, lastUpdateTime);
    }

    @Override
    public String toString() {
        return "AnomalyDetectionTask{"
            + "taskId='"
            + taskId
            + '\''
            + ", version="
            + version
            + ", name='"
            + name
            + '\''
            + ", description='"
            + description
            + '\''
            + ", dataStartTime="
            + dataStartTime
            + ", dataEndTime="
            + dataEndTime
            + ", startTime="
            + startTime
            + ", schedule="
            + schedule
            + ", uiMetadata="
            + uiMetadata
            + ", schemaVersion="
            + schemaVersion
            + ", lastUpdateTime="
            + lastUpdateTime
            + ", timeField='"
            + timeField
            + '\''
            + ", indices="
            + indices
            + ", filterQuery="
            + filterQuery
            + ", featureAttributes="
            + featureAttributes
            + ", detectionInterval="
            + detectionInterval
            + ", windowDelay="
            + windowDelay
            + ", shingleSize="
            + shingleSize
            + '}';
    }
}
