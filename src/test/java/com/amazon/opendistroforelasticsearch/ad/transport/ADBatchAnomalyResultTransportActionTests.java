/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting.AD_PLUGIN_ENABLED;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import com.amazon.opendistroforelasticsearch.ad.HistoricalAnalysisIntegTestCase;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.DetectionDateRange;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class ADBatchAnomalyResultTransportActionTests extends HistoricalAnalysisIntegTestCase {

    private String testIndex;
    private Instant startTime;
    private Instant endTime;
    private String type = "error";
    private int detectionIntervalInMinutes = 1;
    private DetectionDateRange dateRange;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        testIndex = "test_historical_data";
        startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        endTime = Instant.now();
        dateRange = new DetectionDateRange(endTime, endTime.plus(10, ChronoUnit.DAYS));
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type);
        createDetectionStateIndex();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings
            .builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(MAX_BATCH_TASK_PER_NODE.getKey(), 1)
            .build();
    }

    public void testAnomalyDetectorWithNullDetector() {
        ADTask task = randomCreatedADTask(randomAlphaOfLength(5), null, dateRange);
        ADBatchAnomalyResultRequest request = new ADBatchAnomalyResultRequest(task);
        ActionRequestValidationException exception = expectThrows(
            ActionRequestValidationException.class,
            () -> client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(30_000)
        );
        assertTrue(exception.getMessage().contains("Detector can't be null"));
    }

    // public void testRealtimeAnomalyDetector() throws IOException {
    // AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)));
    // ADTask task = randomCreatedADTask(randomAlphaOfLength(5), detector, dateRange);
    // ADBatchAnomalyResultRequest request = new ADBatchAnomalyResultRequest(task);
    // ActionRequestValidationException exception = expectThrows(
    // ActionRequestValidationException.class,
    // () -> client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(30_000)
    // );
    // assertTrue(exception.getMessage().contains("Can't run batch task for realtime detector"));
    // }

    // public void testAnomalyAnalysisWithNullTaskId() throws IOException {
    // AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)));
    // ADTask task = randomCreatedADTask(null, detector, dateRange);
    // ADBatchAnomalyResultRequest request = new ADBatchAnomalyResultRequest(task);
    // ActionRequestValidationException exception = expectThrows(
    // ActionRequestValidationException.class,
    // () -> client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(30_000)
    // );
    // assertTrue(exception.getMessage().contains("Can't run batch task for realtime detector"));
    // assertTrue(exception.getMessage().contains("Task id can't be null"));
    // }

    public void testHistoricalAnalysisWithFutureDateRange() throws IOException, InterruptedException {
        DetectionDateRange dateRange = new DetectionDateRange(endTime, endTime.plus(10, ChronoUnit.DAYS));
        testInvalidDetectionDateRange(dateRange);
    }

    public void testHistoricalAnalysisWithInvalidHistoricalDateRange() throws IOException, InterruptedException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime.minus(10, ChronoUnit.DAYS), startTime);
        testInvalidDetectionDateRange(dateRange);
    }

    public void testHistoricalAnalysisWithSmallHistoricalDateRange() throws IOException, InterruptedException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime, startTime.plus(10, ChronoUnit.MINUTES));
        testInvalidDetectionDateRange(dateRange, "There is no enough data to train model");
    }

    public void testHistoricalAnalysisWithValidDateRange() throws IOException, InterruptedException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        ADBatchAnomalyResultRequest request = adBatchAnomalyResultRequest(dateRange);
        client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(5000);
        Thread.sleep(10000);
        GetResponse doc = getDoc(CommonName.DETECTION_STATE_INDEX, request.getAdTask().getTaskId());
        assertEquals(ADTaskState.FINISHED.name(), doc.getSourceAsMap().get(ADTask.STATE_FIELD));
    }

    public void testHistoricalAnalysisWithNonExistingIndex() throws IOException {
        ADBatchAnomalyResultRequest request = adBatchAnomalyResultRequest(
            new DetectionDateRange(startTime, endTime),
            randomAlphaOfLength(5)
        );
        client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(5000);
    }

    public void testHistoricalAnalysisExceedsMaxRunningTaskLimit() throws IOException, InterruptedException {
        updateTransientSettings(ImmutableMap.of(MAX_BATCH_TASK_PER_NODE.getKey(), 1));
        updateTransientSettings(ImmutableMap.of(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 5));
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        int totalDataNodes = getDataNodes().size();
        for (int i = 0; i < totalDataNodes; i++) {
            client().execute(ADBatchAnomalyResultAction.INSTANCE, adBatchAnomalyResultRequest(dateRange)).actionGet(5000);
        }
        waitUntil(() -> countDocs(CommonName.DETECTION_STATE_INDEX) >= totalDataNodes, 10, TimeUnit.SECONDS);

        ADBatchAnomalyResultRequest request = adBatchAnomalyResultRequest(dateRange);
        RuntimeException exception = expectThrowsAnyOf(
            ImmutableList.of(LimitExceededException.class, NotSerializableExceptionWrapper.class),
            () -> client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(5000)
        );
        assertTrue(exception.getMessage().contains("No eligible node to run detector"));
    }

    public void testDisableADPlugin() throws IOException {
        updateTransientSettings(ImmutableMap.of(AD_PLUGIN_ENABLED, false));

        ADBatchAnomalyResultRequest request = adBatchAnomalyResultRequest(new DetectionDateRange(startTime, endTime));
        RuntimeException exception = expectThrowsAnyOf(
            ImmutableList.of(NotSerializableExceptionWrapper.class, EndRunException.class),
            () -> client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(10000)
        );
        assertTrue(exception.getMessage().contains("AD plugin is disabled"));
        updateTransientSettings(ImmutableMap.of(AD_PLUGIN_ENABLED, true));
    }

    public void testMultipleTasks() throws IOException, InterruptedException {
        updateTransientSettings(ImmutableMap.of(MAX_BATCH_TASK_PER_NODE.getKey(), 2));

        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        for (int i = 0; i < getDataNodes().size(); i++) {
            client().execute(ADBatchAnomalyResultAction.INSTANCE, adBatchAnomalyResultRequest(dateRange));
        }

        ADBatchAnomalyResultRequest request = adBatchAnomalyResultRequest(
            new DetectionDateRange(startTime, startTime.plus(2000, ChronoUnit.MINUTES))
        );
        client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(5000);
        Thread.sleep(10000);
        GetResponse doc = getDoc(CommonName.DETECTION_STATE_INDEX, request.getAdTask().getTaskId());
        assertEquals(ADTaskState.FINISHED.name(), doc.getSourceAsMap().get(ADTask.STATE_FIELD));
        updateTransientSettings(ImmutableMap.of(MAX_BATCH_TASK_PER_NODE.getKey(), 1));
    }

    private ADBatchAnomalyResultRequest adBatchAnomalyResultRequest(DetectionDateRange dateRange) throws IOException {
        return adBatchAnomalyResultRequest(dateRange, testIndex);
    }

    private ADBatchAnomalyResultRequest adBatchAnomalyResultRequest(DetectionDateRange dateRange, String indexName) throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), indexName, detectionIntervalInMinutes, timeField);
        ADTask adTask = randomCreatedADTask(randomAlphaOfLength(5), detector, dateRange);
        adTask.setTaskId(createADTask(adTask));
        return new ADBatchAnomalyResultRequest(adTask);
    }

    private void testInvalidDetectionDateRange(DetectionDateRange dateRange) throws IOException, InterruptedException {
        testInvalidDetectionDateRange(dateRange, "There is no data in the detection date range");
    }

    private void testInvalidDetectionDateRange(DetectionDateRange dateRange, String error) throws IOException, InterruptedException {
        ADBatchAnomalyResultRequest request = adBatchAnomalyResultRequest(dateRange);
        client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(5000);
        Thread.sleep(5000);
        GetResponse doc = getDoc(CommonName.DETECTION_STATE_INDEX, request.getAdTask().getTaskId());
        assertEquals(error, doc.getSourceAsMap().get(ADTask.ERROR_FIELD));
    }
}
