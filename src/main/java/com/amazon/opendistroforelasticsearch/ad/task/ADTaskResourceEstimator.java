package com.amazon.opendistroforelasticsearch.ad.task;

public class ADTaskResourceEstimator {

//    public long calculateADTaskCacheSize(ADTask adTask) {
//        return memoryTracker.estimateModelSize(adTask.getDetector(), NUM_TREES) + maxTrainingDataCacheSize();
//    }

    public static long maxTrainingDataMemorySize(int size) {
        return (16 + 8) * size + 24; // suppose runs on 64 bit, obj header consumes 24 bytes
    }

    public static long maxShingleMemorySize(int shingleSize) {
        //TODO: test and verify, 16: max, min
        return (88 + 8) * shingleSize + 16 + 24;
    }

}
