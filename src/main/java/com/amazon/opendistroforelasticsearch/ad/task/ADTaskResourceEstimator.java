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

package com.amazon.opendistroforelasticsearch.ad.task;

public class ADTaskResourceEstimator {

    // public long calculateADTaskCacheSize(ADTask adTask) {
    // return memoryTracker.estimateModelSize(adTask.getDetector(), NUM_TREES) + maxTrainingDataCacheSize();
    // }

    public static long maxTrainingDataMemorySize(int size) {
        return (16 + 8) * size + 24; // suppose runs on 64 bit, obj header consumes 24 bytes
    }

    public static long maxShingleMemorySize(int shingleSize) {
        // TODO: test and verify, 16: max, min
        return (88 + 8) * shingleSize + 16 + 24;
    }

}
