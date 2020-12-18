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

import static com.amazon.opendistroforelasticsearch.ad.MemoryTracker.Origin.HISTORICAL_SINGLE_ENTITY_DETECTOR;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_MIN_SAMPLES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_TREES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.THRESHOLD_MODEL_TRAINING_SIZE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.TIME_DECAY;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;

import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.ml.HybridThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.randomcutforest.RandomCutForest;

public class ADTaskCacheManager {

    private final Map<String, ADBatchTaskCache> taskCaches;
    private volatile Integer maxAdBatchTaskPerNode;
    private final MemoryTracker memoryTracker;
    private final Logger logger = LogManager.getLogger(ADTaskCacheManager.class);

    public ADTaskCacheManager(Settings settings, ClusterService clusterService, MemoryTracker memoryTracker) {
        this.maxAdBatchTaskPerNode = MAX_BATCH_TASK_PER_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_BATCH_TASK_PER_NODE, it -> maxAdBatchTaskPerNode = it);
        taskCaches = new ConcurrentHashMap<>();
        this.memoryTracker = memoryTracker;
    }

    public RandomCutForest getOrCreateRcfModel(String taskId, int shingleSize, int enabledFeatureSize) {
        ADBatchTaskCache taskCache = getOrThrow(taskId);
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
        ADBatchTaskCache taskCache = getOrThrow(taskId);
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
            taskCache.setThresholdModelTrainingData(new ArrayList<>(THRESHOLD_MODEL_TRAINING_SIZE));
            taskCache.setThresholdModelTrained(false);
        }
        return taskCache.getThresholdModel();
    }

    public List<Double> getThresholdTrainingData(String taskId) {
        ADBatchTaskCache taskCache = getOrThrow(taskId);
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
        ADBatchTaskCache taskCache = get(taskId);
        taskCache.setThresholdModelTrained(trained);
        if (trained) {
            int size = taskCache.getThresholdModelTrainingData().size();
            long cacheSize = ADTaskResourceEstimator.maxTrainingDataMemorySize(size);
            taskCache.getThresholdModelTrainingData().clear();
            taskCache.setThresholdModelTrainingData(null);
            taskCache.getCacheMemorySize().getAndAdd(-cacheSize);
            memoryTracker.releaseMemory(ADTaskResourceEstimator.maxTrainingDataMemorySize(size), false, HISTORICAL_SINGLE_ENTITY_DETECTOR);
        }
    }

    public Deque<Map.Entry<Long, Optional<double[]>>> getShingle(String taskId) {
        if (!contains(taskId)) {
            return null;
        }
        return get(taskId).getShingle();
    }

    // public void putAdTransportTask(String taskId, ADTranspoertTask task) {
    // ADBatchTaskCacheEntity taskCache = getOrThrow(taskId);
    // taskCache.setAdTranspoertTask(task);
    // }

    // public ADTranspoertTask getAdTransportTask(String taskId) {
    // if (!contains(taskId)) {
    // return null;
    // }
    // return get(taskId).getAdTranspoertTask();
    // }

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

    public ADBatchTaskCache get(String taskId) {
        return taskCaches.get(taskId);
    }

    private ADBatchTaskCache getOrThrow(String taskId) {
        ADBatchTaskCache taskCache = taskCaches.get(taskId);
        if (taskCache == null) {
            throw new IllegalArgumentException("Task not in cache");
        }
        return taskCache;
    }

    public ADBatchTaskCache put(ADTask adTask) {
        String taskId = adTask.getTaskId();
        if (contains(taskId)) {
            throw new IllegalArgumentException("AD task is already running");
        }
        checkRunningTaskLimit();
        long neededCacheSize = calculateADTaskCacheSize(adTask);
        if (!memoryTracker.canAllocate(neededCacheSize)) {
            // TODO: tune the error message
            throw new LimitExceededException("AD can't consume more memory than 10%");
        }
        memoryTracker.consumeMemory(neededCacheSize, false, HISTORICAL_SINGLE_ENTITY_DETECTOR);
        ADBatchTaskCache cacheEntity = new ADBatchTaskCache(adTask.getDetectorId());
        cacheEntity.getCacheMemorySize().getAndSet(neededCacheSize);
        return taskCaches.put(taskId, cacheEntity);
    }

    private long calculateADTaskCacheSize(ADTask adTask) {
        return memoryTracker.estimateModelSize(adTask.getDetector(), NUM_TREES) + ADTaskResourceEstimator
            .maxTrainingDataMemorySize(THRESHOLD_MODEL_TRAINING_SIZE) + ADTaskResourceEstimator
                .maxShingleMemorySize(adTask.getDetector().getShingleSize());
    }

    // public ADBatchTaskModel putIfAbsent(String taskId) {
    // if (!contains(taskId)) {
    // return put(taskId);
    // }
    // return get(taskId);
    // }

    public void remove(String taskId) {
        if (contains(taskId)) {
            memoryTracker.releaseMemory(get(taskId).getCacheMemorySize().get(), false, HISTORICAL_SINGLE_ENTITY_DETECTOR);
            taskCaches.remove(taskId);
        }
    }

    /**
     * check if current running batch task on current node exceeds max running task limitation.
     */
    public void checkRunningTaskLimit() {
        checkTaskCount(maxAdBatchTaskPerNode);
    }

    public void checkLimitation() {
        checkTaskCount(maxAdBatchTaskPerNode + 1);
    }

    private void checkTaskCount(int maxTasks) {
        if (this.getTaskNumber() >= maxTasks) {
            String error = "Can't run more than " + maxAdBatchTaskPerNode + " historical detectors per data node";
            throw new LimitExceededException(error);
        }
    }

    public boolean isCancelled(String taskId) {
        ADBatchTaskCache taskCache = getOrThrow(taskId);
        return taskCache.isCancelled();
    }

    public String getCancelReason(String taskId) {
        ADBatchTaskCache taskCache = getOrThrow(taskId);
        return taskCache.getCancelReason();
    }

    public String getCancelledBy(String taskId) {
        ADBatchTaskCache taskCache = getOrThrow(taskId);
        return taskCache.getCancelledBy();
    }

    public void cancel(String taskId, String reason, String userName) {
        ADBatchTaskCache taskCache = getOrThrow(taskId);
        taskCache.cancel(reason, userName);
    }

    public int size() {
        return taskCaches.size();
    }

    public Iterator<Map.Entry<String, ADBatchTaskCache>> iterator() {
        return taskCaches.entrySet().iterator();
    }
}
