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

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.amazon.opendistroforelasticsearch.ad.annotation.Generated;
import com.google.common.base.Objects;

/**
 * One anomaly detection task means one detector starts to run until stopped.
 */
public class ADTaskProfile implements ToXContentObject, Writeable {

    public static final String AD_Task_FIELD = "ad_task";
    public static final String SHINGLE_SIZE_FIELD = "shingle_size";
    public static final String SHINGLE_FIELD = "shingle";
    public static final String RCF_TOTAL_UPDATES_FIELD = "rcf_total_updates";
    public static final String THRESHOLD_MODEL_TRAINED_FIELD = "threshold_model_trained";
    public static final String THRESHOLD_MODEL_TRAINING_DATA_SIZE_FIELD = "threshold_model_training_data_size";
    public static final String NODE_ID_FIELD = "node_id";

    private ADTask adTask = null;
    private Integer shingleSize = null;
    private Long rcfTotalUpdates = null;
    private Boolean thresholdModelTrained = null;
    private Integer thresholdNodelTrainingDataSize = null;
    private String nodeId;

    public ADTaskProfile(
        ADTask adTask,
        Integer shingleSize,
        Long rcfTotalUpdates,
        Boolean thresholdModelTrained,
        Integer thresholdNodelTrainingDataSize,
        String nodeId
    ) {
        this.adTask = adTask;
        this.shingleSize = shingleSize;
        this.rcfTotalUpdates = rcfTotalUpdates;
        this.thresholdModelTrained = thresholdModelTrained;
        this.thresholdNodelTrainingDataSize = thresholdNodelTrainingDataSize;
        this.nodeId = nodeId;
    }

    public ADTaskProfile(StreamInput input) throws IOException {
        if (input.readBoolean()) {
            this.adTask = new ADTask(input);
        } else {
            this.adTask = null;
        }
        this.shingleSize = input.readOptionalInt();
        this.rcfTotalUpdates = input.readOptionalLong();
        this.thresholdModelTrained = input.readOptionalBoolean();
        this.thresholdNodelTrainingDataSize = input.readOptionalInt();
        this.nodeId = input.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (adTask != null) {
            out.writeBoolean(true);
            adTask.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

        out.writeOptionalInt(shingleSize);
        out.writeOptionalLong(rcfTotalUpdates);
        out.writeOptionalBoolean(thresholdModelTrained);
        out.writeOptionalInt(thresholdNodelTrainingDataSize);
        out.writeOptionalString(nodeId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        if (adTask != null) {
            xContentBuilder.field(AD_Task_FIELD, adTask);
        }
        if (shingleSize != null) {
            xContentBuilder.field(SHINGLE_SIZE_FIELD, shingleSize);
        }
        if (rcfTotalUpdates != null) {
            xContentBuilder.field(RCF_TOTAL_UPDATES_FIELD, rcfTotalUpdates);
        }
        if (thresholdModelTrained != null) {
            xContentBuilder.field(THRESHOLD_MODEL_TRAINED_FIELD, thresholdModelTrained);
        }
        if (thresholdNodelTrainingDataSize != null) {
            xContentBuilder.field(THRESHOLD_MODEL_TRAINING_DATA_SIZE_FIELD, thresholdNodelTrainingDataSize);
        }
        if (nodeId != null) {
            xContentBuilder.field(NODE_ID_FIELD, nodeId);
        }
        return xContentBuilder.endObject();
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ADTaskProfile that = (ADTaskProfile) o;
        return Objects.equal(getAdTask(), that.getAdTask())
            && Objects.equal(getShingleSize(), that.getShingleSize())
            && Objects.equal(getRcfTotalUpdates(), that.getRcfTotalUpdates())
            && Objects.equal(getThresholdModelTrained(), that.getThresholdModelTrained())
            && Objects.equal(getNodeId(), that.getNodeId())
            && Objects.equal(getThresholdNodelTrainingDataSize(), that.getThresholdNodelTrainingDataSize());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(adTask, shingleSize, rcfTotalUpdates, thresholdModelTrained, thresholdNodelTrainingDataSize, nodeId);
    }

    public ADTask getAdTask() {
        return adTask;
    }

    public void setAdTask(ADTask adTask) {
        this.adTask = adTask;
    }

    public Integer getShingleSize() {
        return shingleSize;
    }

    public void setShingleSize(Integer shingleSize) {
        this.shingleSize = shingleSize;
    }

    public Long getRcfTotalUpdates() {
        return rcfTotalUpdates;
    }

    public void setRcfTotalUpdates(Long rcfTotalUpdates) {
        this.rcfTotalUpdates = rcfTotalUpdates;
    }

    public Boolean getThresholdModelTrained() {
        return thresholdModelTrained;
    }

    public void setThresholdModelTrained(Boolean thresholdModelTrained) {
        this.thresholdModelTrained = thresholdModelTrained;
    }

    public Integer getThresholdNodelTrainingDataSize() {
        return thresholdNodelTrainingDataSize;
    }

    public void setThresholdNodelTrainingDataSize(Integer thresholdNodelTrainingDataSize) {
        this.thresholdNodelTrainingDataSize = thresholdNodelTrainingDataSize;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public String toString() {
        return "ADTaskProfile{"
            + "adTask="
            + adTask
            + ", shingleSize="
            + shingleSize
            + ", rcfTotalUpdates="
            + rcfTotalUpdates
            + ", thresholdModelTrained="
            + thresholdModelTrained
            + ", thresholdNodelTrainingDataSize="
            + thresholdNodelTrainingDataSize
            + ", nodeId='"
            + nodeId
            + '\''
            + '}';
    }
}
