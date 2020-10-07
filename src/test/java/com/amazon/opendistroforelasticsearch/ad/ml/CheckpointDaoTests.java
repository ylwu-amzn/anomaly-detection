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

package com.amazon.opendistroforelasticsearch.ad.ml;

import static com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao.FIELD_MODEL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.randomcutforest.serialize.RandomCutForestSerDe;
import com.google.gson.Gson;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Gson.class })
public class CheckpointDaoTests {

    private CheckpointDao checkpointDao;

    // dependencies
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Client client;

    @Mock
    private ClientUtil clientUtil;

    @Mock
    private GetResponse getResponse;

    @Mock
    private RandomCutForestSerDe rcfSerde;

    @Mock
    private Clock clock;

    @Mock
    private AnomalyDetectionIndices indexUtil;

    // configuration
    private String indexName;

    // test data
    private String modelId;
    private String model;
    private Map<String, Object> docSource;

    private Gson gson;
    private Class<? extends ThresholdingModel> thresholdingModelClass;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        indexName = "testIndexName";

        gson = PowerMockito.mock(Gson.class);

        thresholdingModelClass = HybridThresholdingModel.class;

        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            indexName,
            gson,
            rcfSerde,
            thresholdingModelClass,
            clock,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            indexUtil
        );

        when(indexUtil.doesCheckpointIndexExist()).thenReturn(true);

        modelId = "testModelId";
        model = "testModel";
        docSource = new HashMap<>();
        docSource.put(FIELD_MODEL, model);
    }

    @Test
    public void putModelCheckpoint_getIndexRequest() {
        checkpointDao.putModelCheckpoint(modelId, model);

        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(clientUtil)
            .timedRequest(
                indexRequestCaptor.capture(),
                anyObject(),
                Matchers.<BiConsumer<IndexRequest, ActionListener<IndexResponse>>>anyObject()
            );
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertEquals(indexName, indexRequest.index());
        assertEquals(modelId, indexRequest.id());
        Set<String> expectedSourceKeys = new HashSet<String>(Arrays.asList(FIELD_MODEL, CheckpointDao.TIMESTAMP));
        assertEquals(expectedSourceKeys, indexRequest.sourceAsMap().keySet());
        assertEquals(model, indexRequest.sourceAsMap().get(FIELD_MODEL));
        assertNotNull(indexRequest.sourceAsMap().get(CheckpointDao.TIMESTAMP));
    }

    @Test
    public void getModelCheckpoint_returnExpected() {
        ArgumentCaptor<GetRequest> getRequestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        doReturn(Optional.of(getResponse))
            .when(clientUtil)
            .timedRequest(
                getRequestCaptor.capture(),
                anyObject(),
                Matchers.<BiConsumer<GetRequest, ActionListener<GetResponse>>>anyObject()
            );
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(docSource);

        Optional<String> result = checkpointDao.getModelCheckpoint(modelId);

        assertTrue(result.isPresent());
        assertEquals(model, result.get());
        GetRequest getRequest = getRequestCaptor.getValue();
        assertEquals(indexName, getRequest.index());
        assertEquals(modelId, getRequest.id());
    }

    @Test
    public void getModelCheckpoint_returnEmpty_whenDocNotFound() {
        doReturn(Optional.of(getResponse))
            .when(clientUtil)
            .timedRequest(anyObject(), anyObject(), Matchers.<BiConsumer<GetRequest, ActionListener<GetResponse>>>anyObject());
        when(getResponse.isExists()).thenReturn(false);

        Optional<String> result = checkpointDao.getModelCheckpoint(modelId);

        assertFalse(result.isPresent());
    }

    @Test
    public void deleteModelCheckpoint_getDeleteRequest() {
        checkpointDao.deleteModelCheckpoint(modelId);

        ArgumentCaptor<DeleteRequest> deleteRequestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(clientUtil)
            .timedRequest(
                deleteRequestCaptor.capture(),
                anyObject(),
                Matchers.<BiConsumer<DeleteRequest, ActionListener<DeleteResponse>>>anyObject()
            );
        DeleteRequest deleteRequest = deleteRequestCaptor.getValue();
        assertEquals(indexName, deleteRequest.index());
        assertEquals(modelId, deleteRequest.id());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void putModelCheckpoint_callListener_whenCompleted() {
        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        checkpointDao.putModelCheckpoint(modelId, model, listener);

        IndexRequest indexRequest = requestCaptor.getValue();
        assertEquals(indexName, indexRequest.index());
        assertEquals(modelId, indexRequest.id());
        Set<String> expectedSourceKeys = new HashSet<String>(Arrays.asList(FIELD_MODEL, CheckpointDao.TIMESTAMP));
        assertEquals(expectedSourceKeys, indexRequest.sourceAsMap().keySet());
        assertEquals(model, indexRequest.sourceAsMap().get(FIELD_MODEL));
        assertNotNull(indexRequest.sourceAsMap().get(CheckpointDao.TIMESTAMP));

        ArgumentCaptor<Void> responseCaptor = ArgumentCaptor.forClass(Void.class);
        verify(listener).onResponse(responseCaptor.capture());
        Void response = responseCaptor.getValue();
        assertEquals(null, response);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getModelCheckpoint_returnExpectedToListener() {
        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(getResponse);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(docSource);

        ActionListener<Optional<String>> listener = mock(ActionListener.class);
        checkpointDao.getModelCheckpoint(modelId, listener);

        GetRequest getRequest = requestCaptor.getValue();
        assertEquals(indexName, getRequest.index());
        assertEquals(modelId, getRequest.id());
        ArgumentCaptor<Optional<String>> responseCaptor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(responseCaptor.capture());
        Optional<String> result = responseCaptor.getValue();
        assertTrue(result.isPresent());
        assertEquals(model, result.get());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getModelCheckpoint_returnEmptyToListener_whenModelNotFound() {
        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(getResponse);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));
        when(getResponse.isExists()).thenReturn(false);

        ActionListener<Optional<String>> listener = mock(ActionListener.class);
        checkpointDao.getModelCheckpoint(modelId, listener);

        GetRequest getRequest = requestCaptor.getValue();
        assertEquals(indexName, getRequest.index());
        assertEquals(modelId, getRequest.id());
        ArgumentCaptor<Optional<String>> responseCaptor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(responseCaptor.capture());
        Optional<String> result = responseCaptor.getValue();
        assertFalse(result.isPresent());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void deleteModelCheckpoint_callListener_whenCompleted() {
        ArgumentCaptor<DeleteRequest> requestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        checkpointDao.deleteModelCheckpoint(modelId, listener);

        DeleteRequest deleteRequest = requestCaptor.getValue();
        assertEquals(indexName, deleteRequest.index());
        assertEquals(modelId, deleteRequest.id());

        ArgumentCaptor<Void> responseCaptor = ArgumentCaptor.forClass(Void.class);
        verify(listener).onResponse(responseCaptor.capture());
        Void response = responseCaptor.getValue();
        assertEquals(null, response);
    }
}
