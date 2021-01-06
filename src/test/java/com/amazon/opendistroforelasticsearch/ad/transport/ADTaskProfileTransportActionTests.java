package com.amazon.opendistroforelasticsearch.ad.transport;

import com.amazon.opendistroforelasticsearch.ad.HistoricalDetectorIntegTestCase;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;

public class ADTaskProfileTransportActionTests extends HistoricalDetectorIntegTestCase {

    private Instant startTime;
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, "error", 2000);
        createDetectorIndex();
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

    public void testProfile() {
        ADTaskProfileRequest request
    }
}
