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

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.randomcutforest.RandomCutForest;

public class ADBatchTaskCacheEntity {
    private final String detectorId;
    private RandomCutForest rcfModel;
    private Deque<Map.Entry<Long, Optional<double[]>>> shingle;
    private ThresholdingModel thresholdModel;
    private boolean thresholdModelTrained;
    private List<Double> thresholdModelTrainingData;
    private AtomicBoolean cancelled = new AtomicBoolean(false);
    private AtomicLong cacheMemorySize = new AtomicLong(0);
    private String cancelReason;
    private String cancelledBy;

    public ADBatchTaskCacheEntity(String detectorId) {
        this.detectorId = detectorId;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public RandomCutForest getRcfModel() {
        return rcfModel;
    }

    public void setRcfModel(RandomCutForest rcfModel) {
        this.rcfModel = rcfModel;
    }

    public Deque<Map.Entry<Long, Optional<double[]>>> getShingle() {
        return shingle;
    }

    public void setShingle(Deque<Map.Entry<Long, Optional<double[]>>> shingle) {
        this.shingle = shingle;
    }

    public ThresholdingModel getThresholdModel() {
        return thresholdModel;
    }

    public void setThresholdModel(ThresholdingModel thresholdModel) {
        this.thresholdModel = thresholdModel;
    }

    public void setThresholdModelTrained(boolean thresholdModelTrained) {
        this.thresholdModelTrained = thresholdModelTrained;
    }

    public boolean isThresholdModelTrained() {
        return thresholdModelTrained;
    }

    public List<Double> getThresholdModelTrainingData() {
        return thresholdModelTrainingData;
    }

    public void setThresholdModelTrainingData(List<Double> thresholdModelTrainingData) {
        this.thresholdModelTrainingData = thresholdModelTrainingData;
    }

    public AtomicLong getCacheMemorySize() {
        return cacheMemorySize;
    }

    public boolean isCancelled() {
        return cancelled.get();
    }

    public String getCancelReason() {
        return cancelReason;
    }

    public String getCancelledBy() {
        return cancelledBy;
    }

    public void setCancelReason(String cancelReason) {
        this.cancelReason = cancelReason;
    }

    public void cancel(String reason, String userName) {
        this.cancelled.compareAndSet(false, true);
        this.cancelReason = reason;
        this.cancelledBy = userName;
    }
}
