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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;

import com.amazon.opendistroforelasticsearch.ad.ADUnitTestCase;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;

public class ADCancelTaskTests extends ADUnitTestCase {

    public void testADCancelTaskRequest() throws IOException {
        ADCancelTaskRequest request = new ADCancelTaskRequest(randomAlphaOfLength(5), randomAlphaOfLength(5), mockDiscoveryNode());

        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ADCancelTaskRequest parsedRequest = new ADCancelTaskRequest(input);
        assertEquals(request.getDetectorId(), parsedRequest.getDetectorId());
        assertEquals(request.getUserName(), parsedRequest.getUserName());
    }

    public void testInvalidADCancelTaskRequest() {
        DiscoveryNode node = new DiscoveryNode(UUIDs.randomBase64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        ADCancelTaskRequest request = new ADCancelTaskRequest(null, null, node);
        ActionRequestValidationException validationException = request.validate();
        assertTrue(validationException.getMessage().contains(CommonErrorMessages.AD_ID_MISSING_MSG));
    }
}
