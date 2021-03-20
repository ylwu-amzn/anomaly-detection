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

package com.amazon.opendistroforelasticsearch.ad.task;

import com.google.common.util.concurrent.RateLimiter;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ADHCTaskCache {
    private Queue<String> pendingEntities;
    private Queue<String> runningEntities;
    private AtomicInteger entityTaskLanes;

    private Integer topEntityCount;
    private boolean detectorTaskUpdating;
    private boolean topEntitiesInited;
    private Map<String, AtomicInteger> taskRetryTimes;
    private Map<String, RateLimiter> rateLimiters;

    public ADHCTaskCache() {
        this.pendingEntities = new ConcurrentLinkedQueue<>();
        this.runningEntities = new ConcurrentLinkedQueue<>();
        this.taskRetryTimes = new ConcurrentHashMap<>();
        this.rateLimiters = new ConcurrentHashMap<>();
    }

    public void setTopEntityCount(Integer topEntityCount) {
        this.topEntityCount = topEntityCount;
    }

    public Queue<String> getPendingEntities() {
        return pendingEntities;
    }

    public String[] getRunningEntities() {
        return runningEntities.toArray(new String[0]);
    }

    public Integer getTopEntityCount() {
        return topEntityCount;
    }

    public boolean getDetectorTaskUpdating() {
        return detectorTaskUpdating;
    }

    public void setDetectorTaskUpdating(boolean detectorTaskUpdating) {
        this.detectorTaskUpdating = detectorTaskUpdating;
    }

    public boolean getTopEntitiesInited() {
        return topEntitiesInited;
    }

    public void setEntityTaskLanes(int entityTaskLanes) {
        this.entityTaskLanes = new AtomicInteger(entityTaskLanes);
    }

    public int getAndDecreaseEntityTaskLanes() {
        return this.entityTaskLanes.getAndDecrement();
    }

    public void setTopEntitiesInited(boolean inited) {
        this.topEntitiesInited = inited;
    }

    public int getTaskRetryTimes(String taskId) {
        return taskRetryTimes.computeIfAbsent(taskId, id -> new AtomicInteger(0)).get();
    }

    public void addEntities(List<String> entities) {
        if (entities == null || entities.size() == 0) {
            return;
        }
        for (String entity : entities) {
            if (entity != null && !pendingEntities.contains(entity)) {
                pendingEntities.add(entity);
            }
        }

    }

    public void moveToRunningEntity(String entity) {
        if (!this.runningEntities.contains(entity)) {
            this.runningEntities.add(entity);
        }
    }

    public int getPendingEntityCount() {
        return this.pendingEntities.size();
    }

    public int getRunningEntityCount() {
        return this.runningEntities.size();
    }

    public void removeRunningEntity(String entity) {
        this.runningEntities.remove(entity);
    }

    public RateLimiter getRateLimiter(String taskId) {
        return this.rateLimiters.computeIfAbsent(taskId, id -> RateLimiter.create(1));
    }

    public void clear() {
        this.pendingEntities.clear();
        this.runningEntities.clear();
        this.taskRetryTimes.clear();
        this.rateLimiters.clear();
    }

    public String pollEntity() {
        return this.pendingEntities.poll();
    }

    public void clearPendingEntities() {
        this.pendingEntities.clear();
    }

    public int increaseTaskRetry(String taskId) {
        return this.taskRetryTimes.computeIfAbsent(taskId, id -> new AtomicInteger(0)).getAndIncrement();
    }
}
