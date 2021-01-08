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
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskProfile;

public class ADTaskProfileTests extends ADUnitTestCase {

    public void testADTaskProfileRequest() throws IOException {
        ADTaskProfileRequest request = new ADTaskProfileRequest(randomAlphaOfLength(5), mockDiscoveryNode());

        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ADTaskProfileRequest parsedRequest = new ADTaskProfileRequest(input);
        assertEquals(request.getAdTaskId(), parsedRequest.getAdTaskId());
    }

    public void testInvalidADTaskProfileRequest() {
        DiscoveryNode node = new DiscoveryNode(UUIDs.randomBase64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        ADTaskProfileRequest request = new ADTaskProfileRequest(null, node);
        ActionRequestValidationException validationException = request.validate();
        assertTrue(validationException.getMessage().contains(CommonErrorMessages.AD_TASK_ID_MISSING));
    }

    public void testADTaskProfileNodeResponse() throws IOException {
        ADTaskProfile adTaskProfile = new ADTaskProfile(randomInt(), randomLong(), randomBoolean(), randomInt(), randomAlphaOfLength(5));
        ADTaskProfileNodeResponse response = new ADTaskProfileNodeResponse(mockDiscoveryNode(), adTaskProfile);
        testADTaskProfileResponse(response);
    }

    public void testADTaskProfileNodeResponseWithNullProfile() throws IOException {
        ADTaskProfileNodeResponse response = new ADTaskProfileNodeResponse(mockDiscoveryNode(), null);
        testADTaskProfileResponse(response);
    }

    public void testADTaskProfileNodeResponseReadMethod() throws IOException {
        ADTaskProfile adTaskProfile = new ADTaskProfile(randomInt(), randomLong(), randomBoolean(), randomInt(), randomAlphaOfLength(5));
        ADTaskProfileNodeResponse response = new ADTaskProfileNodeResponse(mockDiscoveryNode(), adTaskProfile);
        testADTaskProfileResponse(response);
    }

    public void testADTaskProfileNodeResponseReadMethodWithNullProfile() throws IOException {
        ADTaskProfileNodeResponse response = new ADTaskProfileNodeResponse(mockDiscoveryNode(), null);
        testADTaskProfileResponse(response);
    }

    private void testADTaskProfileResponse(ADTaskProfileNodeResponse response) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ADTaskProfileNodeResponse parsedResponse = ADTaskProfileNodeResponse.readNodeResponse(input);
        if (response.getAdTaskProfile() != null) {
            assertTrue(response.getAdTaskProfile().equals(parsedResponse.getAdTaskProfile()));
        } else {
            assertNull(parsedResponse.getAdTaskProfile());
        }
    }
}
