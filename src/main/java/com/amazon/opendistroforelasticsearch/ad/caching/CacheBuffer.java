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

package com.amazon.opendistroforelasticsearch.ad.caching;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistroforelasticsearch.ad.MaintenanceState;
import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.MemoryTracker.Origin;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.model.InitProgressProfile;
import com.amazon.opendistroforelasticsearch.ad.util.ExpiringState;

/**
 * We use a layered cache to manage active entities’ states.  We have a two-level
 * cache that stores active entity states in each node.  Each detector has its
 * dedicated cache that stores ten (dynamically adjustable) entities’ states per
 * node.  A detector’s hottest entities load their states in the dedicated cache.
 * If less than 10 entities use the dedicated cache, the secondary cache can use
 * the rest of the free memory available to AD.  The secondary cache is a shared
 * memory among all detectors for the long tail.  The shared cache size is 10%
 * heap minus all of the dedicated cache consumed by single-entity and multi-entity
 * detectors.  The shared cache’s size shrinks as the dedicated cache is filled
 * up or more detectors are started.
 */
public class CacheBuffer implements ExpiringState, MaintenanceState {
    private static final Logger LOG = LogManager.getLogger(CacheBuffer.class);

    static class PriorityNode {
        private String key;
        private float priority;

        PriorityNode(String key, float priority) {
            this.priority = priority;
            this.key = key;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            if (obj instanceof PriorityNode) {
                PriorityNode other = (PriorityNode) obj;

                EqualsBuilder equalsBuilder = new EqualsBuilder();
                equalsBuilder.append(key, other.key);

                return equalsBuilder.isEquals();
            }
            return false;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(key).toHashCode();
        }
    }

    static class PriorityNodeComparator implements Comparator<PriorityNode> {

        @Override
        public int compare(PriorityNode priority, PriorityNode priority2) {
            return Double.compare(priority.priority, priority2.priority);
        }
    }

    private final int minimumCapacity;
    // key -> Priority node
    private final ConcurrentHashMap<String, PriorityNode> key2Priority;
    private final ConcurrentSkipListSet<PriorityNode> priorityList;
    // key -> value
    private final ConcurrentHashMap<String, ModelState<EntityModel>> items;
    // when detector is created.  Can be reset.  Unit: seconds
    private long landmarkSecs;
    // length of seconds in one interval.  Used to compute elapsed periods
    // since the detector has been enabled.
    private long intervalSecs;
    // memory consumption per entity
    private final long memoryConsumptionPerEntity;
    private final MemoryTracker memoryTracker;
    private final Clock clock;
    private final CheckpointDao checkpointDao;
    private final Duration modelTtl;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;
    private final String detectorId;
    private Instant lastUsedTime;

    public CacheBuffer(
        int minimumCapacity,
        long intervalSecs,
        CheckpointDao checkpointDao,
        long memoryConsumptionPerEntity,
        MemoryTracker memoryTracker,
        Clock clock,
        Duration modelTtl,
        String detectorId
    ) {
        this.minimumCapacity = minimumCapacity;
        this.key2Priority = new ConcurrentHashMap<>();
        this.priorityList = new ConcurrentSkipListSet<>(new PriorityNodeComparator());
        this.items = new ConcurrentHashMap<>();
        this.landmarkSecs = Instant.now().getEpochSecond();
        this.intervalSecs = intervalSecs;
        this.memoryConsumptionPerEntity = memoryConsumptionPerEntity;
        this.memoryTracker = memoryTracker;
        this.clock = clock;
        this.checkpointDao = checkpointDao;
        this.modelTtl = modelTtl;
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
        this.detectorId = detectorId;
        this.lastUsedTime = Instant.now();
    }

    /**
     * Update step at period t_k:
     * new priority = old priority + log(1+e^{\log(g(t_k-L))-old priority}) where g(n) = e^{0.125n},
     * and n is the period.
     * @param entityId model Id
     */
    private void update(String entityId) {
        PriorityNode node = key2Priority.computeIfAbsent(entityId, k -> new PriorityNode(entityId, 0f));

        node.priority = getUpdatedPriority(node.priority);

        // reposition this node
        this.priorityList.remove(node);
        this.priorityList.add(node);

        Instant now = clock.instant();
        items.get(entityId).setLastUsedTime(now);
        lastUsedTime = now;
    }

    public float getUpdatedPriority(float oldPriority) {
        long increment = computeWeightedCountIncrement();
        // if overflowed, we take the short cut from now on

        oldPriority += Math.log(1 + Math.exp(increment - oldPriority));
        // if overflow happens, using \log(g(t_k-L)) instead.
        if (oldPriority == Float.POSITIVE_INFINITY) {
            oldPriority = increment;
        }
        return oldPriority;
    }

    /**
     * Compute periods relative to landmark and the weighted count increment using 0.125n.
     * Multiply by 0.125 is implemented using right shift for efficiency.
     * @return the weighted count increment used in the priority update step.
     */
    private long computeWeightedCountIncrement() {
        long periods = (Instant.now().getEpochSecond() - landmarkSecs) / intervalSecs;
        return periods >> 3;
    }

    /**
     * Compute the weighted total count by considering landmark
     * \log(C)=\log(\sum_{i=1}^{n} (g(t_i-L)/g(t-L)))=\log(\sum_{i=1}^{n} (g(t_i-L))-\log(g(t-L))
     * @return the minimum priority
     */
    public float getMinimumPriority() {
        PriorityNode smallest = priorityList.first();
        long periods = (Instant.now().getEpochSecond() - landmarkSecs) / intervalSecs;
        float detectorWeight = periods >> 1;
        return smallest.priority - detectorWeight;
    }

    /**
     * Insert the model state associated with a model Id to the cache
     * @param entityId the model Id
     * @param value the ModelState
     */
    public void put(String entityId, ModelState<EntityModel> value) {
        try {
            // don't add if the lock is held. As our inserts are single-threaded
            // per entity, we won't fail to acquire the lock during normal cache
            // running. We can fail to acquire the lock when we are deleting the
            // detector associated with the cache buffer and an insert happens.
            writeLock.lock();
            put(entityId, value, getUpdatedPriority(value.getPriority()));
        } finally {
            if (writeLock.isHeldByCurrentThread()) {
                writeLock.unlock();
            }
        }
    }

    /**
    * Insert the model state associated with a model Id to the cache.  Update priority.
    * @param entityId the model Id
    * @param value the ModelState
    * @param priority the priority
    */
    private void put(String entityId, ModelState<EntityModel> value, float priority) {
        if (minimumCapacity <= 0) {
            return;
        }
        ModelState<EntityModel> contentNode = items.get(entityId);
        if (contentNode == null) {
            PriorityNode node = new PriorityNode(entityId, priority);
            key2Priority.put(entityId, node);
            priorityList.add(node);
            items.put(entityId, value);
            Instant now = clock.instant();
            value.setLastUsedTime(now);
            lastUsedTime = now;
            // shared cache empty means we are consuming reserved cache.
            // Since we have already considered them while allocating CacheBuffer,
            // skip bookkeeping.
            if (!sharedCacheEmpty()) {
                memoryTracker.consumeMemory(memoryConsumptionPerEntity, false, Origin.MULTI_ENTITY_DETECTOR);
            }
        } else {
            update(entityId);
            items.put(entityId, value);
        }
    }

    /**
     * Retrieve the ModelState associated with the model Id or null if the CacheBuffer
     * contains no mapping for the model Id
     * @param key the model Id
     * @return the Model state to which the specified model Id is mapped, or null
     * if this CacheBuffer contains no mapping for the model Id
     */
    public ModelState<EntityModel> get(String key) {
        readLock.lock();
        ModelState<EntityModel> node = items.get(key);
        readLock.unlock();
        if (node == null) {
            return null;
        }
        update(key);
        return node;
    }

    /**
     *
     * @return whether there is one item that can be removed from shared cache
     */
    public boolean canRemove() {
        return !items.isEmpty() && items.size() > minimumCapacity;
    }

    /**
     * remove the smallest priority item.
     */
    public void remove() {
        try {
            // stop evicting if the lock is held. The entries will be eventually
            // evicted.
            writeLock.lock();
            PriorityNode smallest = priorityList.pollFirst();
            if (smallest != null) {
                String keyToRemove = smallest.key;
                ModelState<EntityModel> valueRemoved = remove(keyToRemove);
                checkpointDao.write(valueRemoved, keyToRemove);
            }

        } finally {
            if (writeLock.isHeldByCurrentThread()) {
                writeLock.unlock();
            }
        }
    }

    /**
     * Remove everything associated with the key.
     *
     * @param keyToRemove The key to remove
     * @return the associated ModelState associated with the key, or null if there
     * is no associated ModelState for the key
     */
    private ModelState<EntityModel> remove(String keyToRemove) {
        // if shared cache is empty, we are using reserved memory
        boolean reserved = sharedCacheEmpty();

        key2Priority.remove(keyToRemove);
        ModelState<EntityModel> valueRemoved = items.remove(keyToRemove);

        // if we releasing a shared cache item, release memory as well.
        if (valueRemoved != null && !reserved) {
            memoryTracker.releaseMemory(memoryConsumptionPerEntity, false, Origin.MULTI_ENTITY_DETECTOR);
        }

        return valueRemoved;
    }

    /**
     * @return whether dedicated cache is available or not
     */
    public boolean dedicatedCacheAvailable() {
        return items.size() < minimumCapacity;
    }

    /**
     * @return whether shared cache is empty or not
     */
    public boolean sharedCacheEmpty() {
        return items.size() <= minimumCapacity;
    }

    /**
     *
     * @return the estimated number of bytes per entity state
     */
    public long getMemoryConsumptionPerEntity() {
        return memoryConsumptionPerEntity;
    }

    /**
     *
     * If the cache is not full, check if some other items can replace internal entities.
     * @param priority another entity's priority
     * @return whether one entity can be replaced by another entity with a certain priority
     */
    public boolean canReplace(float priority) {
        return !items.isEmpty() && priority > getMinimumPriority();
    }

    /**
     * Replace the smallest priority entity with the input entity
     * @param entityId the Model Id
     * @param value the model State
     */
    public void replace(String entityId, ModelState<EntityModel> value) {
        remove();
        put(entityId, value);
    }

    @Override
    public void maintenance() {
        List<PriorityNode> toRemove = new ArrayList<>();
        items.entrySet().stream().forEach(entry -> {
            String entityId = entry.getKey();
            try {
                ModelState<EntityModel> modelState = entry.getValue();
                Instant now = clock.instant();

                // we can have ConcurrentModificationException when serializing
                // and updating rcf model at the same time. To prevent this,
                // we need to have a deep copy of models or have a lock. Both
                // options are costly.
                // As we are gonna retry serializing either when the entity is
                // evicted out of cache or during the next maintenance period,
                // don't do anything when the exception happens.
                checkpointDao.write(modelState, entityId);

                if (modelState.getLastUsedTime().plus(modelTtl).isBefore(now)) {
                    toRemove.add(new PriorityNode(entityId, modelState.getPriority()));
                }
            } catch (Exception e) {
                LOG.warn("Failed to finish maintenance for model id " + entityId, e);
            }
        });
        // We don't remove inside the above forEach loop to lock as few places as possible.
        try {
            // stop evicting if the lock is held. The entries will be eventually
            // evicted.
            writeLock.lock();
            toRemove.forEach(item -> {
                priorityList.remove(item);
                remove(item.key);
            });
        } finally {
            if (writeLock.isHeldByCurrentThread()) {
                writeLock.unlock();
            }
        }
    }

    /**
     *
     * @return the number of active entities
     */
    public int getActiveEntities() {
        return items.size();
    }

    /**
     *
     * @param entityId Model Id
     * @return Whether the model is active or not
     */
    public boolean isActive(String entityId) {
        return items.containsKey(entityId);
    }

    /**
     *
     * @return Get the model of highest priority entity
     */
    public Optional<String> getHighestPriorityEntityId() {
        return Optional.of(priorityList).map(list -> list.last()).map(node -> node.key);
    }

    /**
     *
     * @param entityId entity Id
     * @return Get the model of an entity
     */
    public Optional<EntityModel> getModel(String entityId) {
        return Optional.of(items).map(map -> map.get(entityId)).map(state -> state.getModel());
    }

    /**
     * Clear associated memory.  Used when we are removing an detector.
     * @return true if the clear is successful; and false otherwise (e.g., the
     * insert lock was already held by the current thread)
     */
    public boolean clear() {
        try {
            // don't clear if the lock is held.
            // We can fail to acquire the lock when we are an insert is running
            // and a clear is called.
            if (!writeLock.tryLock()) {
                return false;
            }
            memoryTracker.releaseMemory(getReservedBytes(), true, Origin.MULTI_ENTITY_DETECTOR);
            if (!sharedCacheEmpty()) {
                memoryTracker.releaseMemory(getBytesInSharedCache(), false, Origin.MULTI_ENTITY_DETECTOR);
            }
            return true;
        } finally {
            if (writeLock.isHeldByCurrentThread()) {
                writeLock.unlock();
            }
        }
    }

    /**
     *
     * @return reserved bytes by the CacheBuffer
     */
    public long getReservedBytes() {
        return memoryConsumptionPerEntity * minimumCapacity;
    }

    /**
     *
     * @return bytes consumed in the shared cache by the CacheBuffer
     */
    public long getBytesInSharedCache() {
        int sharedCacheEntries = items.size() - minimumCapacity;
        return memoryConsumptionPerEntity * sharedCacheEntries;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        if (obj instanceof InitProgressProfile) {
            CacheBuffer other = (CacheBuffer) obj;

            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(detectorId, other.detectorId);

            return equalsBuilder.isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(detectorId).toHashCode();
    }

    @Override
    public boolean expired(Duration stateTtl) {
        return expired(lastUsedTime, stateTtl, clock.instant());
    }

    public String getDetectorId() {
        return detectorId;
    }
}
