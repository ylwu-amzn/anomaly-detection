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

package com.amazon.opendistroforelasticsearch.ad.feature;

import java.io.IOException;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.elasticsearch.test.ESTestCase;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.google.common.collect.ImmutableList;

public class ShingleTests extends ESTestCase {

    public void testEmptyShingle() throws IOException {
        long endTime = 1600000000000l + 8 * 60 * 1000;
        AnomalyDetector detector = randomDetector();
        // AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());

        long maxTimeDifference = detector.getDetectorIntervalInMilliseconds() / 2;
        Deque<Map.Entry<Long, Optional<double[]>>> shingle = new ArrayDeque<>();

        Map<Long, Map.Entry<Long, Optional<double[]>>> featuresMap = getNearbyPointsForShingle(
            detector,
            shingle,
            endTime,
            maxTimeDifference
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        System.out.println(featuresMap.size());
        featuresMap.entrySet().forEach(a -> { System.out.println(a.getKey() + ": " + a.getValue().getKey() + ": " + a.getValue()); });
        List<Map.Entry<Long, Long>> missingRanges = getMissingRangesInShingle(detector, featuresMap, endTime);
    }

    public void testFullShingle() throws IOException {
        long endTime = 1600000000000l + 8 * 60 * 1000;
        AnomalyDetector detector = randomDetector();
        // AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());

        long maxTimeDifference = detector.getDetectorIntervalInMilliseconds() / 2;
        Deque<Map.Entry<Long, Optional<double[]>>> shingle = new ArrayDeque<>();
        // 1600000000000: 1600000060000
        // 1600000060000: 1600000120000
        // 1600000120000: 1600000180000
        // 1600000180000: 1600000240000
        // 1600000240000: 1600000300000
        // 1600000300000: 1600000360000
        // 1600000360000: 1600000420000
        // 1600000420000: 1600000480000
        shingle.add(new AbstractMap.SimpleEntry<>(1600000000000l, Optional.of(new double[] { 1.0, 2.0 })));// 0
        shingle.add(new AbstractMap.SimpleEntry<>(1600000060000l, Optional.of(new double[] { 1.0, 2.0 })));// 1
        shingle.add(new AbstractMap.SimpleEntry<>(1600000120000l, Optional.of(new double[] { 1.0, 2.0 })));// 2
        shingle.add(new AbstractMap.SimpleEntry<>(1600000300000l, Optional.of(new double[] { 1.0, 2.0 })));// 5

        Map<Long, Map.Entry<Long, Optional<double[]>>> featuresMap = getNearbyPointsForShingle(
            detector,
            shingle,
            endTime,
            maxTimeDifference
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        System.out.println(featuresMap.size());
        featuresMap.entrySet().forEach(a -> { System.out.println(a.getKey() + ": " + a.getValue().getKey() + ": " + a.getValue()); });
        List<Map.Entry<Long, Long>> missingRanges = getMissingRangesInShingle(detector, featuresMap, endTime);
        System.out.println("++++++++++++++++++++++++++++++ 111");
        missingRanges.forEach(b -> { System.out.println(b.getKey() + ": " + b.getValue()); });
    }

    private AnomalyDetector randomDetector() throws IOException {
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(10).toLowerCase()),
            ImmutableList.of(TestHelpers.randomFeature()),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            8,
            null,
            randomInt(),
            Instant.now(),
            null,
            TestHelpers.randomUser()
        );
    }

    private List<Map.Entry<Long, Long>> getMissingRangesInShingle(
        AnomalyDetector detector,
        Map<Long, Map.Entry<Long, Optional<double[]>>> featuresMap,
        long endTime
    ) {
        long intervalMilli = detector.getDetectorIntervalInMilliseconds();
        int shingleSize = detector.getShingleSize();
        return getFullShingleEndTimes(endTime, intervalMilli, shingleSize)
            .filter(time -> !featuresMap.containsKey(time))
            .mapToObj(time -> new AbstractMap.SimpleImmutableEntry<>(time - intervalMilli, time))
            .collect(Collectors.toList());
    }

    private Stream<Map.Entry<Long, Map.Entry<Long, Optional<double[]>>>> getNearbyPointsForShingle(
        AnomalyDetector detector,
        Deque<Map.Entry<Long, Optional<double[]>>> shingle,
        long endTime,
        long maxMillisecondsDifference
    ) {
        long intervalMilli = detector.getDetectorIntervalInMilliseconds();
        int shingleSize = detector.getShingleSize();
        TreeMap<Long, Optional<double[]>> search = new TreeMap<>(
            shingle.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        );
        return getFullShingleEndTimes(endTime, intervalMilli, shingleSize).mapToObj(t -> {
            // after is the closest timestamp which is after time stamp t
            Optional<Map.Entry<Long, Optional<double[]>>> after = Optional.ofNullable(search.ceilingEntry(t));
            // after is the closest timestamp which is before time stamp t
            Optional<Map.Entry<Long, Optional<double[]>>> before = Optional.ofNullable(search.floorEntry(t));
            return after
                // Check if current end time stamp closer to after or before , if close to after, set as after; otherwise set as before
                .filter(a -> Math.abs(t - a.getKey()) <= before.map(b -> Math.abs(t - b.getKey())).orElse(Long.MAX_VALUE))
                .map(Optional::of)
                .orElse(before)
                // check if the gap between (t, after) or (t, before) is less than maxMillisecondsDifference(1/2 detection interval)
                .filter(e -> Math.abs(t - e.getKey()) < maxMillisecondsDifference)
                // before -> (t, before)
                .map(e -> new AbstractMap.SimpleImmutableEntry<>(t, e));
        }).filter(Optional::isPresent).map(Optional::get);
    }

    private LongStream getFullShingleEndTimes(long endTime, long intervalMilli, int shingleSize) {
        return LongStream.rangeClosed(1, shingleSize).map(i -> endTime - (shingleSize - i) * intervalMilli);
    }
}
