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

import com.amazon.opendistroforelasticsearch.ad.ml.HybridThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTranspoertTask;
import com.amazon.randomcutforest.RandomCutForest;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_MIN_SAMPLES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_TREES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.TIME_DECAY;

public class ADBatchTaskCache {
    private static ADBatchTaskCache INSTANCE;

    private final Map<String, ADBatchTaskModel> taskModels;

    private ADBatchTaskCache() {
        taskModels = new ConcurrentHashMap<>();
    }

    public static ADBatchTaskCache getInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (ADBatchTaskCache.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new ADBatchTaskCache();
            return INSTANCE;
        }
    }

    public RandomCutForest getOrCreateRcfModel(String taskId, int shingleSize, int enabledFeatureSize) {
        ADBatchTaskModel taskModel = taskModels.computeIfAbsent(taskId, id -> new ADBatchTaskModel());
        if (taskModel.getRcfModel() == null) {
            RandomCutForest rcf = RandomCutForest
                    .builder()
                    .dimensions(shingleSize * enabledFeatureSize)
                    .numberOfTrees(NUM_TREES)
                    .lambda(TIME_DECAY)
                    .sampleSize(NUM_SAMPLES_PER_TREE)
                    .outputAfter(NUM_MIN_SAMPLES)
                    .parallelExecutionEnabled(false)
                    .build();
            taskModel.setRcfModel(rcf);
            taskModel.setShingle(new ArrayDeque<>(shingleSize));
        }
        return taskModel.getRcfModel();
    }

    public RandomCutForest getRcfModel(String taskId) {
        if (!taskModels.containsKey(taskId)) {
            return null;
        }
        return taskModels.get(taskId).getRcfModel();
    }

    public ThresholdingModel getOrCreateThresholdModel(String taskId) {
        ADBatchTaskModel taskModel = taskModels.computeIfAbsent(taskId, id -> new ADBatchTaskModel());
        if (taskModel.getThresholdModel() == null) {
            ThresholdingModel thresholdModel = new HybridThresholdingModel(
                    AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
                    AnomalyDetectorSettings.THRESHOLD_MAX_RANK_ERROR,
                    AnomalyDetectorSettings.THRESHOLD_MAX_SCORE,
                    AnomalyDetectorSettings.THRESHOLD_NUM_LOGNORMAL_QUANTILES,
                    AnomalyDetectorSettings.THRESHOLD_DOWNSAMPLES,
                    AnomalyDetectorSettings.THRESHOLD_MAX_SAMPLES
            );
            taskModel.setThresholdModel(thresholdModel);
            taskModel.setThresholdModelTrainingData(new ArrayList<>());
            taskModel.setThresholdModelTrained(false);
        }
        return taskModel.getThresholdModel();
    }

    public ThresholdingModel getThresholdModel(String taskId) {
        if (!taskModels.containsKey(taskId)) {
            return null;
        }
        return taskModels.get(taskId).getThresholdModel();
    }

    public List<Double> getThresholdTrainingData(String taskId) {
        ADBatchTaskModel taskModel = taskModels.computeIfAbsent(taskId, id -> new ADBatchTaskModel());
        if (taskModel.getThresholdModelTrainingData() == null) {
            taskModel.setThresholdModelTrainingData(new ArrayList<>());
        }
        return taskModel.getThresholdModelTrainingData();
    }

    public boolean isThresholdModelTrained(String taskId) {
        if (!taskModels.containsKey(taskId)) {
            return false;
        }
        return taskModels.get(taskId).isThresholdModelTrained();
    }

    public void setThresholdModelTrained(String taskId, boolean trained) {
        if (!taskModels.containsKey(taskId)) {
            throw new IllegalArgumentException("Task not in cache");
        }
        ADBatchTaskModel taskModel = taskModels.get(taskId);
        taskModel.setThresholdModelTrained(trained);
        if (trained) {
            taskModel.getThresholdModelTrainingData().clear();
            taskModel.setThresholdModelTrainingData(null);
        }
    }

    public Deque<Map.Entry<Long, Optional<double[]>>> getShingle(String taskId) {
        if (!taskModels.containsKey(taskId)) {
            return null;
        }
        return taskModels.get(taskId).getShingle();
    }

    public void putAdTransportTask(String taskId, ADTranspoertTask task) {
        ADBatchTaskModel taskModel = taskModels.computeIfAbsent(taskId, id -> new ADBatchTaskModel());
        taskModel.setAdTranspoertTask(task);
    }

    public ADTranspoertTask getAdTransportTask(String taskId) {
        if (!taskModels.containsKey(taskId)) {
            return null;
        }
        return taskModels.get(taskId).getAdTranspoertTask();
    }

    public int getTaskNumber() {
        return taskModels.size();
    }

    public void remove(String taskId) {
        taskModels.remove(taskId);
    }
}
