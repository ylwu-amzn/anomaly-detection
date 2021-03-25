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

package com.amazon.opendistroforelasticsearch.ad.model;

import com.google.common.collect.ImmutableList;

import java.util.List;

public enum ADTaskType {
    REALTIME_SINGLE_ENTITY,
    REALTIME_HC_DETECTOR,
    HISTORICAL_SINGLE_ENTITY,
    HISTORICAL_HC_DETECTOR,
    HISTORICAL_HC_ENTITY;

    public static List<ADTaskType> getHistoricalDetectorTaskTypes() {
        return ImmutableList.of(ADTaskType.HISTORICAL_HC_DETECTOR, ADTaskType.HISTORICAL_SINGLE_ENTITY);
    }

    public static List<ADTaskType> getAllHistoricalTaskTypes() {
        return ImmutableList.of(ADTaskType.HISTORICAL_HC_DETECTOR, ADTaskType.HISTORICAL_SINGLE_ENTITY,
                ADTaskType.HISTORICAL_HC_ENTITY);
    }

    public static List<ADTaskType> getRealtimeTaskTypes() {
        return ImmutableList.of(ADTaskType.REALTIME_SINGLE_ENTITY, ADTaskType.REALTIME_HC_DETECTOR);
    }

}
