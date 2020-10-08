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
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;

import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager.ModelType;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class LRUCache implements EntityCache {
    private static final Logger LOG = LogManager.getLogger(ModelManager.class);

    private final Cache<String, ModelState<EntityModel>> activeEntityStates;

    private final CheckpointDao checkpointDao;
    private final ModelManager modelManager;
    private final Clock clock;

    public LRUCache(CheckpointDao checkpointDao, Duration modelTtl, int maxActiveStates, ModelManager modelManager, Clock clock) {
        this.checkpointDao = checkpointDao;
        RemovalListener<String, ModelState<EntityModel>> listener = new RemovalListener<String, ModelState<EntityModel>>() {
            @Override
            public void onRemoval(RemovalNotification<String, ModelState<EntityModel>> n) {
                if (n.wasEvicted()) {
                    checkpointDao.write(n.getValue(), n.getKey());
                }
            }
        };

        this.activeEntityStates = CacheBuilder
            .newBuilder()
            .expireAfterAccess(modelTtl.toHours(), TimeUnit.HOURS)
            .maximumSize(maxActiveStates)
            .removalListener(listener)
            .concurrencyLevel(1)
            .build();

        this.modelManager = modelManager;
        this.clock = clock;
    }

    @Override
    public ModelState<EntityModel> get(String modelId, AnomalyDetector detector, double[] datapoint, String entityName) {
        ModelState<EntityModel> state = activeEntityStates.getIfPresent(modelId);
        // We can experience thrashing if the unique number of entities are large
        if (state == null) {
            EntityModel model = new EntityModel(modelId, new ArrayDeque<>(), null, null);
            ModelState<EntityModel> modelState = ModelState
                .createSingleEntityModelState(model, modelId, detector.getDetectorId(), ModelType.ENTITY.getName(), clock);
            model.addSample(datapoint);
            activeEntityStates.put(modelId, modelState);
            checkpointDao
                .restoreModelCheckpoint(
                    modelId,
                    ActionListener
                        .wrap(
                            checkpoint -> modelManager.processEntityCheckpoint(checkpoint, modelId, entityName, modelState),
                            e -> modelManager.processEntityCheckpoint(Optional.empty(), modelId, entityName, modelState)

                        )
                );
        }

        return state;
    }

    // nothing to do
    @Override
    public void maintenance() {}

    @Override
    public void clear(String detectorId) {}

    @Override
    public int getActiveEntities(String detector) {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public boolean isActive(String detectorId, String entityId) {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public float getInitProgress(String detectorId) {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public float getInitProgress(String detectorId, String entityId) {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public long getTotalUpdates(String detectorId) {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public long getTotalUpdates(String detectorId, String entityId) {
        throw new UnsupportedOperationException("not supported");
    }
}
