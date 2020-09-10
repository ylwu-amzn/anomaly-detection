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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class AnomalyResultBatchResponse extends ActionResponse implements ToXContentObject {
    public static final String MESSAGE_KEY = "message";

    private String message;

    public AnomalyResultBatchResponse(String message) {
        this.message = message;
    }

    public AnomalyResultBatchResponse(StreamInput in) throws IOException {
        super(in);
        message = in.readString();
    }

    public String getMessage() {
        return message;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(message);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MESSAGE_KEY, message);
        builder.endObject();
        return builder;
    }

}
