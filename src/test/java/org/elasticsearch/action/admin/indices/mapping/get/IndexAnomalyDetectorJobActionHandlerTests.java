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

package org.elasticsearch.action.admin.indices.mapping.get;

import static com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler.CATEGORICAL_FIELD_TYPE_ERR_MSG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;

public class IndexAnomalyDetectorJobActionHandlerTests extends AbstractADTest {
    static ThreadPool threadPool;

    NodeClient clientMock;
    IndexAnomalyDetectorJobActionHandler handler;
    String detectorId;
    RestChannel channel;
    AnomalyDetectionIndices anomalyDetectionIndices;
    Long seqNo;
    Long primaryTerm;
    TimeValue requestTimeout;
    AnomalyDetectorJob job;
    AnomalyDetector detector;

    /**
     * Mockito does not allow mock final methods.  Make my own delegates and mock them.
     *
     */
    class NodeClientDelegate extends NodeClient {

        NodeClientDelegate(Settings settings, ThreadPool threadPool) {
            super(settings, threadPool);
        }

        public <Request extends ActionRequest, Response extends ActionResponse> void execute2(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            super.execute(action, request, listener);
        }

    }

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("IndexAnomalyDetectorJobActionHandlerTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clientMock = spy(new NodeClient(Settings.EMPTY, null));

        channel = mock(RestChannel.class);

        // used during the call to RestHandlerUtils.createXContentParser in onGetAnomalyDetectorResponse
        RestRequest restRequest = mock(RestRequest.class);
        when(channel.request()).thenReturn(restRequest);
        when(restRequest.getXContentRegistry()).thenReturn(xContentRegistry());

        anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        when(anomalyDetectionIndices.doesAnomalyDetectorJobIndexExist()).thenReturn(true);

        detectorId = "123";
        seqNo = 0L;
        primaryTerm = 0L;
        requestTimeout = new TimeValue(1000L);
        handler = new IndexAnomalyDetectorJobActionHandler(
            clientMock,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            requestTimeout
        );

        // return an enabled job
        job = TestHelpers.randomAnomalyDetectorJob(true);

        detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, true);
    }

    public void testTwoCategoricalFields() throws IOException {
        expectThrows(
            IllegalArgumentException.class,
            () -> TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList("a", "b"))
        );
        ;
    }

    @SuppressWarnings("unchecked")
    private void mockGetResponse(ActionListener<? extends ActionResponse> origListener, AnomalyDetector detector, ActionRequest request)
        throws IOException {
        ActionListener<GetResponse> listener = (ActionListener<GetResponse>) origListener;
        GetRequest getRequest = (GetRequest) request;
        if (getRequest.index().equals(AnomalyDetector.ANOMALY_DETECTORS_INDEX)) {
            listener.onResponse(TestHelpers.createGetResponse(detector, detectorId, AnomalyDetector.ANOMALY_DETECTORS_INDEX));
        } else {
            // returns a GetResponse whose isExists() returns true, which would skip the following indexing
            listener.onResponse(TestHelpers.createGetResponse(job, detectorId, AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX));
        }
    }

    @SuppressWarnings("unchecked")
    public void testNoCategoricalField() throws IOException {

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 2);

            assertTrue(args[0] instanceof GetRequest);
            assertTrue(args[1] instanceof ActionListener);

            mockGetResponse((ActionListener<GetResponse>) args[1], detector, (GetRequest) args[0]);

            return null;
        }).when(clientMock).get(any(GetRequest.class), any());

        handler.startAnomalyDetectorJob();
        ArgumentCaptor<BytesRestResponse> response = ArgumentCaptor.forClass(BytesRestResponse.class);
        verify(clientMock, never()).execute(eq(GetMappingsAction.INSTANCE), any(), any());
        verify(channel).sendResponse(response.capture());
        BytesRestResponse value = response.getValue();
        assertEquals(RestStatus.OK, value.status());
    }

    @SuppressWarnings("unchecked")
    public void testTextField() throws IOException {
        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        // extend NodeClient since its execute method is final and mockito does not allow to mock final methods
        // we can also use spy to overstep the final methods
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                try {
                    if (action.equals(GetAction.INSTANCE)) {
                        listener
                            .onResponse(
                                (Response) TestHelpers.createGetResponse(detector, detectorId, AnomalyDetector.ANOMALY_DETECTORS_INDEX)
                            );
                    } else {
                        // we need to put the test in the same package of GetFieldMappingsResponse since its constructor is package private
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(detector.getIndices().get(0), field, "text")
                        );
                        listener.onResponse((Response) response);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        handler = new IndexAnomalyDetectorJobActionHandler(
            client,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            requestTimeout
        );

        ArgumentCaptor<BytesRestResponse> response = ArgumentCaptor.forClass(BytesRestResponse.class);

        handler.startAnomalyDetectorJob();

        verify(channel).sendResponse(response.capture());
        BytesRestResponse value = response.getValue();
        assertEquals(RestStatus.BAD_REQUEST, value.status());
        assertEquals(new BytesArray(CATEGORICAL_FIELD_TYPE_ERR_MSG), value.content());
    }

    @SuppressWarnings("unchecked")
    private void testValidTypeTepmlate(String filedTypeName) throws IOException {
        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        // extend NodeClient since its execute method is final and mockito does not allow to mock final methods
        // we can also use spy to overstep the final methods
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                try {
                    if (action.equals(GetAction.INSTANCE)) {
                        mockGetResponse(listener, detector, request);
                    } else {
                        // we need to put the test in the same package of GetFieldMappingsResponse since its constructor is package private
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(detector.getIndices().get(0), field, filedTypeName)
                        );
                        listener.onResponse((Response) response);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        NodeClient clientSpy = spy(client);

        handler = new IndexAnomalyDetectorJobActionHandler(
            clientSpy,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            requestTimeout
        );

        ArgumentCaptor<BytesRestResponse> response = ArgumentCaptor.forClass(BytesRestResponse.class);

        handler.startAnomalyDetectorJob();

        verify(clientSpy, times(1)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        verify(channel).sendResponse(response.capture());
        BytesRestResponse value = response.getValue();
        assertEquals(RestStatus.OK, value.status());
    }

    public void testIpField() throws IOException {
        testValidTypeTepmlate(CommonName.IP_TYPE);
    }

    public void testKeywordField() throws IOException {
        testValidTypeTepmlate(CommonName.KEYWORD_TYPE);
    }
}
