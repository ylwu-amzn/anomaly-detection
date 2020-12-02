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

import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.ml.HybridThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTranspoertTask;
import com.amazon.randomcutforest.RandomCutForest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_MIN_SAMPLES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_TREES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.TIME_DECAY;

public class ADTaskCache {

    private final Map<String, ADBatchTaskCacheEntity> taskCaches;
    private volatile Integer maxAdBatchTaskPerNode;

    public ADTaskCache(Settings settings, ClusterService clusterService) {
        this.maxAdBatchTaskPerNode = MAX_BATCH_TASK_PER_NODE.get(settings);
        clusterService
                .getClusterSettings()
                .addSettingsUpdateConsumer(MAX_BATCH_TASK_PER_NODE, it -> maxAdBatchTaskPerNode = it);
        taskCaches = new ConcurrentHashMap<>();
    }

    public RandomCutForest getOrCreateRcfModel(String taskId, int shingleSize, int enabledFeatureSize) {
        ADBatchTaskCacheEntity taskCache = getOrThrow(taskId);
        if (taskCache.getRcfModel() == null) {
            RandomCutForest rcf = RandomCutForest
                    .builder()
                    .dimensions(shingleSize * enabledFeatureSize)
                    .numberOfTrees(NUM_TREES)
                    .lambda(TIME_DECAY)
                    .sampleSize(NUM_SAMPLES_PER_TREE)
                    .outputAfter(NUM_MIN_SAMPLES)
                    .parallelExecutionEnabled(false)
                    .build();
            taskCache.setRcfModel(rcf);
            taskCache.setShingle(new ArrayDeque<>(shingleSize));
        }
        return taskCache.getRcfModel();
    }

    public RandomCutForest getRcfModel(String taskId) {
        if (!contains(taskId)) {
            return null;
        }
        return get(taskId).getRcfModel();
    }

    public ThresholdingModel getOrCreateThresholdModel(String taskId) {
        ADBatchTaskCacheEntity taskCache = getOrThrow(taskId);
        if (taskCache.getThresholdModel() == null) {
            ThresholdingModel thresholdModel = new HybridThresholdingModel(
                    AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
                    AnomalyDetectorSettings.THRESHOLD_MAX_RANK_ERROR,
                    AnomalyDetectorSettings.THRESHOLD_MAX_SCORE,
                    AnomalyDetectorSettings.THRESHOLD_NUM_LOGNORMAL_QUANTILES,
                    AnomalyDetectorSettings.THRESHOLD_DOWNSAMPLES,
                    AnomalyDetectorSettings.THRESHOLD_MAX_SAMPLES
            );
            taskCache.setThresholdModel(thresholdModel);
            taskCache.setThresholdModelTrainingData(new ArrayList<>());
            taskCache.setThresholdModelTrained(false);
        }
        return taskCache.getThresholdModel();
    }

    public List<Double> getThresholdTrainingData(String taskId) {
        ADBatchTaskCacheEntity taskCache = getOrThrow(taskId);
        if (taskCache.getThresholdModelTrainingData() == null) {
            taskCache.setThresholdModelTrainingData(new ArrayList<>());
        }
        return taskCache.getThresholdModelTrainingData();
    }

    public boolean isThresholdModelTrained(String taskId) {
        if (!contains(taskId)) {
            return false;
        }
        return get(taskId).isThresholdModelTrained();
    }

    public void setThresholdModelTrained(String taskId, boolean trained) {
        if (!contains(taskId)) {
            throw new IllegalArgumentException("Task not in cache");
        }
        ADBatchTaskCacheEntity taskCache = get(taskId);
        taskCache.setThresholdModelTrained(trained);
        if (trained) {
            taskCache.getThresholdModelTrainingData().clear();
            taskCache.setThresholdModelTrainingData(null);
        }
    }

    public Deque<Map.Entry<Long, Optional<double[]>>> getShingle(String taskId) {
        if (!contains(taskId)) {
            return null;
        }
        return get(taskId).getShingle();
    }

    public void putAdTransportTask(String taskId, ADTranspoertTask task) {
        ADBatchTaskCacheEntity taskCache = getOrThrow(taskId);
        taskCache.setAdTranspoertTask(task);
    }

    public ADTranspoertTask getAdTransportTask(String taskId) {
        if (!contains(taskId)) {
            return null;
        }
        return get(taskId).getAdTranspoertTask();
    }

    public int getTaskNumber() {
        return taskCaches.size();
    }

    public boolean contains(String taskId) {
        return taskCaches.containsKey(taskId);
    }

    public boolean containsTaskOfDetector(String detectorId) {
        long count = taskCaches.entrySet().stream().filter(entry -> Objects.equals(detectorId, entry.getValue().getDetectorId())).count();
        return count > 0;
    }

    public ADBatchTaskCacheEntity get(String taskId) {
        return taskCaches.get(taskId);
    }

    private ADBatchTaskCacheEntity getOrThrow(String taskId) {
        ADBatchTaskCacheEntity taskCache = taskCaches.get(taskId);
        if (taskCache == null) {
            throw new IllegalArgumentException("Task not in cache");
        }
        return taskCache;
    }

    public ADBatchTaskCacheEntity put(ADTask adTask) {
        String taskId = adTask.getTaskId();
        if (contains(taskId)) {
            throw new IllegalArgumentException("AD task is already running");
        }
        allowToPutNewTask();
        return taskCaches.put(taskId, new ADBatchTaskCacheEntity(adTask.getDetectorId()));
    }

//    public ADBatchTaskModel putIfAbsent(String taskId) {
//        if (!contains(taskId)) {
//            return put(taskId);
//        }
//        return get(taskId);
//    }

    public void remove(String taskId) {
        taskCaches.remove(taskId);
    }

    /**
     * check if current running batch task on current node exceeds max running task limitation.
     */
    public void allowToPutNewTask() {
        checkTaskCount(maxAdBatchTaskPerNode);
    }

    public void checkLimitation() {
        checkTaskCount(maxAdBatchTaskPerNode + 1);
    }

    private void checkTaskCount(int maxTasks) {
        if (this.getTaskNumber() >= maxTasks) {
            String error = "Can't run more than " + maxAdBatchTaskPerNode + " historical detector per node";
            throw new LimitExceededException(error);
        }
    }

    public boolean isCancelled(String taskId) {
        ADBatchTaskCacheEntity taskCache = getOrThrow(taskId);
        return taskCache.isCancelled();
    }

    public void cancel(String taskId) {
        getOrThrow(taskId).cancel();
    }
}
