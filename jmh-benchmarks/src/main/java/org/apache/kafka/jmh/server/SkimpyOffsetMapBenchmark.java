/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.jmh.server;

import kafka.log.MurmurOffsetMap;
import kafka.log.SkimpyOffsetMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(value = Scope.Benchmark)
public class SkimpyOffsetMapBenchmark {

    @Param({"1000000", "10000000", "100000000"})
    public int logLength;

    @Param({"100"})
    public int numKeys;

    @Param({"10"})
    public int keyLength;

    @Param({"1048576", "10485760"})
    public int memory;

    private Random random = new Random();

    private SkimpyOffsetMap skimpyOffsetMap;
    private double maxSkimpyOffsetMapCollisionRate = 0.0;
    private MurmurOffsetMap murmurOffsetMap;
    private double maxMurmurOffsetMapCollisionRate = 0.0;
    private ByteBuffer[] keys;
    private Map<Long, ByteBuffer> offsetToKeyMap;

    @Setup(Level.Trial)
    public void setupTrial() {
        skimpyOffsetMap = new SkimpyOffsetMap(memory, "MD5");
        murmurOffsetMap = new MurmurOffsetMap(memory);
        Set<String> keysSet = new HashSet<>(numKeys);
        for (int i = 0; i < numKeys; ++i) {
            String nextRandom;
            do {
                nextRandom = nextRandomString(keyLength);
            } while (keysSet.contains(nextRandom));
            keysSet.add(nextRandom);
        }
        keys = keysSet
            .stream()
            .map(k -> ByteBuffer.wrap(k.getBytes()))
            .collect(Collectors.toList()).toArray(new ByteBuffer[]{});
        offsetToKeyMap = new HashMap<>(logLength);
        for (long i = 0; i < logLength; ++i) {
            offsetToKeyMap.put(i, ByteBuffer.wrap(keys[random.nextInt(numKeys)].array()));
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        System.out.println(String.format("Max skimpy offset map collision rate: %s", maxSkimpyOffsetMapCollisionRate));
        System.out.println(String.format("Max murmur offset map collision rate: %s", maxMurmurOffsetMapCollisionRate));
    }

    private String nextRandomString(int length) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10;
        return random.ints(leftLimit, rightLimit + 1)
            .limit(targetStringLength)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }

    @Benchmark
    @Threads(1)
    public void measureMd5HashingSpeed() {
        offsetToKeyMap.forEach((offset, key) -> {
            skimpyOffsetMap.put(key, offset);
        });
        double currentCollisionRate = skimpyOffsetMap.collisionRate();
        maxSkimpyOffsetMapCollisionRate = Math.max(currentCollisionRate, maxSkimpyOffsetMapCollisionRate);
    }

    @Benchmark
    @Threads(1)
    public void measureMurmurHashingSpeed() {
        offsetToKeyMap.forEach((offset, key) -> {
            murmurOffsetMap.put(key, offset);
        });
        double currentCollisionRate = murmurOffsetMap.collisionRate();
        maxMurmurOffsetMapCollisionRate = Math.max(currentCollisionRate, maxMurmurOffsetMapCollisionRate);
    }

    public static void main(String[] args) {
        SkimpyOffsetMapBenchmark benchmark = new SkimpyOffsetMapBenchmark();
        benchmark.setupTrial();
        benchmark.measureMd5HashingSpeed();
        System.out.println(benchmark.skimpyOffsetMap.collisionRate());
        benchmark.measureMurmurHashingSpeed();
        System.out.println(benchmark.murmurOffsetMap.collisionRate());
    }
}
