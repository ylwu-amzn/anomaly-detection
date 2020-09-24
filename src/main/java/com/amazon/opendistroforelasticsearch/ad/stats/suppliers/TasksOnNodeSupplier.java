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

package com.amazon.opendistroforelasticsearch.ad.stats.suppliers;

import static com.amazon.opendistroforelasticsearch.ad.ml.ModelState.DETECTOR_ID_KEY;
import static com.amazon.opendistroforelasticsearch.ad.ml.ModelState.MODEL_ID_KEY;
import static com.amazon.opendistroforelasticsearch.ad.ml.ModelState.MODEL_TYPE_KEY;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.amazon.opendistroforelasticsearch.ad.task.AnomalyDetectionTaskManager;

/**
 * ModelsOnNodeSupplier provides a List of ModelStates info for the models the nodes contains
 */
public class TasksOnNodeSupplier implements Supplier<List<Map<String, Object>>> {
    private AnomalyDetectionTaskManager taskManager;

    /**
     * Set that contains the model stats that should be exposed.
     */
    public static Set<String> MODEL_STATE_STAT_KEYS = new HashSet<>(Arrays.asList(MODEL_ID_KEY, DETECTOR_ID_KEY, MODEL_TYPE_KEY));

    /**
     * Constructor
     *
     * @param taskManager object that manages the task hosted on the node
     */
    public TasksOnNodeSupplier(AnomalyDetectionTaskManager taskManager) {
        this.taskManager = taskManager;
    }

    @Override
    public List<Map<String, Object>> get() {
        return taskManager.getTasks();
    }
}
