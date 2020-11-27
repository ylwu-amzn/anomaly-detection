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

import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTranspoertTask;
import com.amazon.randomcutforest.RandomCutForest;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ADBatchTaskModel {
    private RandomCutForest rcfModel;
    private Deque<Map.Entry<Long, Optional<double[]>>> shingle; //TODO, clean when finish/fail/stop
    private ThresholdingModel thresholdModel;
    private boolean thresholdModelTrained;
    private List<Double> thresholdModelTrainingData; // TODO, check if this class is singleton or not
    private ADTranspoertTask adTranspoertTask;

    public ADBatchTaskModel() {}

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

    public ADTranspoertTask getAdTranspoertTask() {
        return adTranspoertTask;
    }

    public void setAdTranspoertTask(ADTranspoertTask adTranspoertTask) {
        this.adTranspoertTask = adTranspoertTask;
    }
}
