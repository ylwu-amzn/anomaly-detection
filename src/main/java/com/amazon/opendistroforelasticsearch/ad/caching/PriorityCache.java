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

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.COOLDOWN_MINUTES;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.MemoryTracker.Origin;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager.ModelType;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.RateLimiter;

public class PriorityCache implements EntityCache {
    private final Logger LOG = LogManager.getLogger(PriorityCache.class);

    // detector id -> CacheBuffer, weight based
    private final Map<String, CacheBuffer> activeEnities;
    private final CheckpointDao checkpointDao;
    private final int dedicatedCacheSize;
    // LRU Cache
    private Cache<String, ModelState<EntityModel>> inActiveEntities;
    private final MemoryTracker memoryTracker;
    private final ModelManager modelManager;
    private final ReentrantLock writeLock;
    private final int numberOfTrees;
    private final Clock clock;
    private final Duration modelTtl;
    private final int numMinSamples;
    private Map<String, DoorKeeper> doorKeepers;
    private final RateLimiter restoreRateLimiter;
    private Instant lastThrottledRestoreTime;
    private int coolDownMinutes;
    private ThreadPool threadPool;
    private Random random;

    public PriorityCache(
        CheckpointDao checkpointDao,
        int dedicatedCacheSize,
        Duration inactiveEntityTtl,
        int maxInactiveStates,
        MemoryTracker memoryTracker,
        ModelManager modelManager,
        int numberOfTrees,
        Clock clock,
        ClusterService clusterService,
        Duration modelTtl,
        int numMinSamples,
        Settings settings,
        ThreadPool threadPool
    ) {
        this.checkpointDao = checkpointDao;
        this.dedicatedCacheSize = dedicatedCacheSize;

        this.activeEnities = new ConcurrentHashMap<>();
        this.memoryTracker = memoryTracker;
        this.modelManager = modelManager;
        this.writeLock = new ReentrantLock();
        this.numberOfTrees = numberOfTrees;
        this.clock = clock;
        this.modelTtl = modelTtl;
        this.numMinSamples = numMinSamples;
        this.doorKeepers = new ConcurrentHashMap<>();
        // 1 restore from checkpoint per second allowed.
        this.restoreRateLimiter = RateLimiter.create(1);

        this.inActiveEntities = CacheBuilder
            .newBuilder()
            .expireAfterAccess(inactiveEntityTtl.toHours(), TimeUnit.HOURS)
            .maximumSize(maxInactiveStates)
            .concurrencyLevel(1)
            .build();

        this.lastThrottledRestoreTime = Instant.MIN;
        this.coolDownMinutes = (int) (COOLDOWN_MINUTES.get(settings).getMinutes());
        this.threadPool = threadPool;
        this.random = new Random(42);
    }

    @Override
    public ModelState<EntityModel> get(String modelId, AnomalyDetector detector, double[] datapoint, String entityName) {
        String detectorId = detector.getDetectorId();
        CacheBuffer buffer = computeBufferIfAbsent(detector, detectorId);
        ModelState<EntityModel> modelState = buffer.get(modelId);
        if (modelState == null) {
            DoorKeeper doorKeeper = doorKeepers
                .computeIfAbsent(
                    detectorId,
                    id -> {
                        // reset every 60 intervals
                        return new DoorKeeper(
                            AnomalyDetectorSettings.DOOR_KEEPER_MAX_INSERTION,
                            AnomalyDetectorSettings.DOOR_KEEPER_FAULSE_POSITIVE_RATE,
                            detector.getDetectionIntervalDuration().multipliedBy(60),
                            clock
                        );
                    }
                );

            // first hit, ignore
            if (doorKeeper.mightContain(modelId) == false) {
                doorKeeper.put(modelId);
                return null;
            }

            ModelState<EntityModel> state = inActiveEntities.getIfPresent(modelId);

            // compute updated priority
            float priority = 0;
            if (state != null) {
                priority = state.getPriority();
            }
            priority = buffer.getUpdatedPriority(priority);

            // update state using new priority or create a new one
            if (state != null) {
                state.setPriority(priority);
            } else {
                EntityModel model = new EntityModel(modelId, new ArrayDeque<>(), null, null);
                state = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);
            }

            if (hostIfPossible(buffer, detectorId, modelId, entityName, detector, state, priority)) {
                addSample(state, datapoint);
            } else {
                // only keep weights in inactive cache to keep it small.
                // It can be dangerous to exceed a few dozen kilobytes, especially
                // in small heap machine like t2.
                inActiveEntities.put(modelId, state);
            }
        }

        return modelState;
    }

    /**
     * Whether host an entity is possible
     * @param buffer the destination buffer for the given entity
     * @param detectorId Detector Id
     * @param modelId Model Id
     * @param entityName Entity's name
     * @param detector Detector Config
     * @param state State to host
     * @param priority The entity's priority
     * @return true if possible; false otherwise
     */
    private boolean hostIfPossible(
        CacheBuffer buffer,
        String detectorId,
        String modelId,
        String entityName,
        AnomalyDetector detector,
        ModelState<EntityModel> state,
        float priority
    ) {
        // current buffer's dedicated cache has free slots
        if (buffer.dedicatedCacheAvailable()) {
            buffer.put(modelId, state);
        } else if (buffer.canReplace(priority)) {
            // can replace an entity in the same CacheBuffer living in reserved
            // or shared cache
            buffer.replace(modelId, state);
        } else {
            try {
                if (writeLock.tryLock()) {
                    // put inside a lock in case that other threads are checking the condition too
                    // use tryLock to avoid blocking. It's fine to skip if we cannot acquire a lock.
                    // Next time, we can retry.
                    if (memoryTracker.canAllocate(detectorId, detector, numberOfTrees)) {
                        buffer.put(modelId, state);
                    } else {
                        CacheBuffer bufferToRemoveEntity = canReplaceInSharedCache(buffer, priority);
                        if (bufferToRemoveEntity != null) {
                            bufferToRemoveEntity.remove();
                            buffer.put(modelId, state);
                        } else {
                            return false;
                        }
                    }
                }
            } finally {
                if (writeLock.isHeldByCurrentThread()) {
                    writeLock.unlock();
                }
            }
        }

        maybeRestoreOrTrainModel(modelId, entityName, state);
        return true;
    }

    private void addSample(ModelState<EntityModel> stateToPromote, double[] datapoint) {
        // add samples
        Queue<double[]> samples = stateToPromote.getModel().getSamples();
        samples.add(datapoint);
        // only keep the recent numMinSamples
        if (samples.size() > this.numMinSamples) {
            samples.remove();
        }
    }

    private void maybeRestoreOrTrainModel(String modelId, String entityName, ModelState<EntityModel> state) {
        EntityModel entityModel = state.getModel();
        // rate limit in case of EsRejectedExecutionException from get threadpool whose queue capacity is 1k
        if (entityModel != null
            && (entityModel.getRcf() == null || entityModel.getThreshold() == null)
            && lastThrottledRestoreTime.plus(Duration.ofMinutes(coolDownMinutes)).isBefore(clock.instant())
            && restoreRateLimiter.tryAcquire()) {
            checkpointDao
                .restoreModelCheckpoint(
                    modelId,
                    ActionListener
                        .wrap(checkpoint -> modelManager.processEntityCheckpoint(checkpoint, modelId, entityName, state), exception -> {
                            if (exception instanceof IndexNotFoundException) {
                                modelManager.processEntityCheckpoint(Optional.empty(), modelId, entityName, state);
                            } else if (exception instanceof EsRejectedExecutionException
                                || exception instanceof RejectedExecutionException) {
                                LOG.error("too many requests");
                                lastThrottledRestoreTime = Instant.now();
                            } else {
                                LOG.error("Fail to restore models for " + modelId, exception);
                            }
                        })
                );
        }
    }

    private CacheBuffer computeBufferIfAbsent(AnomalyDetector detector, String detectorId) {
        return activeEnities.computeIfAbsent(detectorId, k -> {
            long requiredBytes = getReservedDetectorMemory(detector);
            if (memoryTracker.canAllocateReserved(detectorId, requiredBytes)) {
                memoryTracker.consumeMemory(requiredBytes, true, Origin.MULTI_ENTITY_DETECTOR);
                long intervalSecs = detector.getDetectorIntervalInSeconds();
                return new CacheBuffer(
                    dedicatedCacheSize,
                    intervalSecs,
                    checkpointDao,
                    memoryTracker.estimateModelSize(detector, numberOfTrees),
                    memoryTracker,
                    clock,
                    modelTtl,
                    detectorId
                );
            }
            // if hosting not allowed, exception will be thrown by isHostingAllowed
            throw new EndRunException(detectorId, "Unexpected bug", true);
        });
    }

    private long getReservedDetectorMemory(AnomalyDetector detector) {
        return dedicatedCacheSize * memoryTracker.estimateModelSize(detector, numberOfTrees);
    }

    /**
     * Whether the candidate entity can replace any entity in the shared cache.
     * We can have race conditions when multiple threads try to evaluate this
     * function. The result is that we can have multiple threads thinks they
     * can replace entities in the cache.
     *
     * @param originBuffer the CacheBuffer that the entity belongs to (with the same detector Id)
     * @param candicatePriority the candidate entity's priority
     * @return the CacheBuffer if we can find a CacheBuffer to make room for the candidate entity
     */
    private CacheBuffer canReplaceInSharedCache(CacheBuffer originBuffer, float candicatePriority) {
        CacheBuffer minPriorityBuffer = null;
        float minPriority = Float.MAX_VALUE;
        for (Map.Entry<String, CacheBuffer> entry : activeEnities.entrySet()) {
            CacheBuffer buffer = entry.getValue();
            if (buffer != originBuffer) {
                float priority = buffer.getMinimumPriority();
                if (buffer.canRemove() && candicatePriority > priority && priority < minPriority) {
                    minPriority = priority;
                    minPriorityBuffer = buffer;
                }
            }
        }
        return minPriorityBuffer;
    }

    private void clearUpMemoryIfNecessary() {
        try {
            if (!writeLock.tryLock()) {
                return;
            }
            recalculateUsedMemory();
            long memoryToShed = memoryTracker.memoryToShed();
            float minPriority = Float.MAX_VALUE;
            CacheBuffer minPriorityBuffer = null;
            while (memoryToShed > 0) {
                for (Map.Entry<String, CacheBuffer> entry : activeEnities.entrySet()) {
                    CacheBuffer buffer = entry.getValue();
                    float priority = buffer.getMinimumPriority();
                    if (buffer.canRemove() && priority < minPriority) {
                        minPriority = priority;
                        minPriorityBuffer = buffer;
                    }
                }
                if (minPriorityBuffer != null) {
                    minPriorityBuffer.remove();
                    long memoryReleased = minPriorityBuffer.getMemoryConsumptionPerEntity();
                    memoryTracker.releaseMemory(memoryReleased, false, Origin.MULTI_ENTITY_DETECTOR);
                    memoryToShed -= memoryReleased;
                } else {
                    break;
                }
            }
        } finally {
            if (writeLock.isHeldByCurrentThread()) {
                writeLock.unlock();
            }
        }
    }

    /**
     * Recalculate memory consumption in case of bugs/errors when allocating/releasing memory
     */
    private void recalculateUsedMemory() {
        long reserved = 0;
        long shared = 0;
        for (Map.Entry<String, CacheBuffer> entry : activeEnities.entrySet()) {
            CacheBuffer buffer = entry.getValue();
            reserved += buffer.getReservedBytes();
            shared += buffer.getBytesInSharedCache();
        }
        memoryTracker.syncMemoryState(Origin.MULTI_ENTITY_DETECTOR, reserved + shared, reserved);
    }

    @Override
    public void maintenance() {
        // clean up memory if we allocate more memory than we should
        clearUpMemoryIfNecessary();
        activeEnities.entrySet().stream().forEach(cacheBufferEntry -> {
            String detectorId = cacheBufferEntry.getKey();
            CacheBuffer cacheBuffer = cacheBufferEntry.getValue();
            // remove expired cache buffer
            if (cacheBuffer.expired(modelTtl)) {
                activeEnities.remove(detectorId);
                clear(detectorId);
            } else {
                cacheBuffer.maintenance();
            }
        });
        doorKeepers.entrySet().stream().forEach(doorKeeperEntry -> {
            String detectorId = doorKeeperEntry.getKey();
            DoorKeeper doorKeeper = doorKeeperEntry.getValue();
            if (doorKeeper.expired(modelTtl)) {
                doorKeepers.remove(detectorId);
            } else {
                doorKeeper.maintenance();
            }
        });
    }

    /**
     * Permanently deletes models hosted in memory and persisted in index.
     *
     * @param detectorId id the of the detector for which models are to be permanently deleted
     */
    @Override
    public void clear(String detectorId) {
        if (detectorId == null) {
            return;
        }
        CacheBuffer buffer = activeEnities.remove(detectorId);

        // retry in one minute if we cannot get the lock to clear memory
        if (buffer != null && !buffer.clear()) {
            AbstractRunnable clearMemoryRunnable = new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    buffer.clear();
                }

                @Override
                public void onFailure(Exception e) {
                    LOG.error("Fail to release memory taken by CacheBuffer.  Will retry during maintenance.");
                }
            };
            threadPool
                .schedule(
                    () -> clearMemoryRunnable.run(),
                    new TimeValue(random.nextInt(90), TimeUnit.SECONDS),
                    AnomalyDetectorPlugin.AD_THREAD_POOL_NAME
                );
        }
        checkpointDao.deleteModelCheckpointByDetectorId(detectorId);
        doorKeepers.remove(detectorId);
    }

    /**
     * Compute init progress of a detector.  We use the highest priority entity's
     * init progress as the detector's init progress.
     * @param detectorId Detector Id
     * @return init progress for an detector.  Return 0 if we cannot find such
     * detector in the active entity cache.
     */
    @Override
    public float getInitProgress(String detectorId) {
        return Optional
            .of(activeEnities)
            .map(entities -> entities.get(detectorId))
            .map(buffer -> buffer.getHighestPriorityEntityId())
            .map(entityIdOptional -> entityIdOptional.get())
            .map(entityId -> getInitProgress(detectorId, entityId))
            .orElse(0f);
    }

    /**
     * Compute init progress for an active entity.
     * @param detectorId Detector Id
     * @param entityId Entity Id
     * @return a number between [0,1].  0 means there is zero init progress or
     * we cannot find the entity in the active entity cache.
     */
    @Override
    public float getInitProgress(String detectorId, String entityId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            Optional<EntityModel> modelOptional = cacheBuffer.getModel(entityId);
            // TODO: make it work for shingles. samples.size() is not the real shingle
            long accumulatedShingles = modelOptional
                .map(model -> model.getRcf())
                .map(rcf -> rcf.getTotalUpdates())
                .orElseGet(
                    () -> modelOptional.map(model -> model.getSamples()).map(samples -> samples.size()).map(Long::valueOf).orElse(0L)
                );
            return Math.min(1.0f, (float) accumulatedShingles / numMinSamples);
        }
        return 0f;
    }

    /**
     * Get the number of active entities of a detector
     * @param detectorId Detector Id
     * @return The number of active entities
     */
    @Override
    public int getActiveEntities(String detectorId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            return cacheBuffer.getActiveEntities();
        }
        return 0;
    }

    /**
     * Whether an entity is active or not
     * @param detectorId The Id of the detector that an entity belongs to
     * @param entityId Entity Id
     * @return Whether an entity is active or not
     */
    @Override
    public boolean isActive(String detectorId, String entityId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            return cacheBuffer.isActive(entityId);
        }
        return false;
    }

    @Override public long getTotalUpdates(String detectorId) {
        return Optional
            .of(activeEnities)
            .map(entities -> entities.get(detectorId))
            .map(buffer -> buffer.getHighestPriorityEntityId())
            .map(entityIdOptional -> entityIdOptional.get())
            .map(entityId -> getTotalUpdates(detectorId, entityId))
            .orElse(0l);
    }

    @Override
    public long getTotalUpdates(String detectorId, String entityId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            Optional<EntityModel> modelOptional = cacheBuffer.getModel(entityId);
            // TODO: make it work for shingles. samples.size() is not the real shingle
            long accumulatedShingles = modelOptional
                .map(model -> model.getRcf())
                .map(rcf -> rcf.getTotalUpdates())
                .orElseGet(
                    () -> modelOptional.map(model -> model.getSamples()).map(samples -> samples.size()).map(Long::valueOf).orElse(0L)
                );
            return accumulatedShingles;
        }
        return 0l;
    }
}
