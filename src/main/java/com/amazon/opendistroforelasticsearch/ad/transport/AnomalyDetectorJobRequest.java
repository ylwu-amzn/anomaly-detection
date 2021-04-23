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

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.amazon.opendistroforelasticsearch.ad.model.DetectionDateRange;

public class AnomalyDetectorJobRequest extends ActionRequest {

    private String detectorID;
    private DetectionDateRange detectionDateRange;
    private long seqNo;
    private long primaryTerm;
    private String rawPath;

    public AnomalyDetectorJobRequest(StreamInput in) throws IOException {
        super(in);
        detectorID = in.readString();
        if (in.readBoolean()) {
            detectionDateRange = new DetectionDateRange(in);
        }
        seqNo = in.readLong();
        primaryTerm = in.readLong();
        rawPath = in.readString();
    }

    public AnomalyDetectorJobRequest(String detectorID, long seqNo, long primaryTerm, String rawPath) {
        this(detectorID, null, seqNo, primaryTerm, rawPath);
    }

    public AnomalyDetectorJobRequest(
            String detectorID,
            DetectionDateRange detectionDateRange,
            long seqNo,
            long primaryTerm,
            String rawPath
    ) {
        super();
        this.detectorID = detectorID;
        this.detectionDateRange = detectionDateRange;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.rawPath = rawPath;
    }

    public String getDetectorID() {
        return detectorID;
    }

    public DetectionDateRange getDetectionDateRange() {
        return detectionDateRange;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public String getRawPath() {
        return rawPath;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(detectorID);
        if (detectionDateRange != null) {
            out.writeBoolean(true);
            detectionDateRange.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeLong(seqNo);
        out.writeLong(primaryTerm);
        out.writeString(rawPath);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
