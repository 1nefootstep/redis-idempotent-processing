package com.ocean.redisidempotentprocessing.controller;

import com.ocean.redisidempotentprocessing.event.Expense;
import com.ocean.redisidempotentprocessing.service.OwnerExpenseService;
import com.ocean.redisidempotentprocessing.service.SumRedisHashStrategy;
import com.ocean.redisidempotentprocessing.service.impl.OwnerExpenseServiceImpl;
import com.ocean.redisidempotentprocessing.service.impl.SumRedisHashByVariableAndPipelineAndCache;
import com.ocean.redisidempotentprocessing.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@RestController
@Slf4j
public class BenchmarkController {
    private OwnerExpenseService ownerExpenseServiceBrute;
    private OwnerExpenseService ownerExpenseServicePipeline;
    private OwnerExpenseService ownerExpenseServiceLock;
    private OwnerExpenseService ownerExpenseServicePipelineAndCache;

    @Autowired
    private SumRedisHashByVariableAndPipelineAndCache sumRedisHashByVariableAndPipelineAndCache;

    @Autowired
    private RedissonClient redissonClient;

    public BenchmarkController(@Qualifier("sumRedisHashByBruteForce") SumRedisHashStrategy bruteForceStrategy,
                               @Qualifier("sumRedisHashByVariableAndPipeline") SumRedisHashStrategy variableAndPipelineStrategy,
                               @Qualifier("sumRedisHashByVariableAndPipelineAndLock") SumRedisHashStrategy variableAndLockStrategy,
                               @Qualifier("sumRedisHashByVariableAndPipelineAndCache") SumRedisHashStrategy variableAndPipelineAndCacheStrategy) {
        ownerExpenseServiceBrute = new OwnerExpenseServiceImpl(bruteForceStrategy);
        ownerExpenseServicePipeline = new OwnerExpenseServiceImpl(variableAndPipelineStrategy);
        ownerExpenseServiceLock = new OwnerExpenseServiceImpl(variableAndLockStrategy);
        ownerExpenseServicePipelineAndCache = new OwnerExpenseServiceImpl(variableAndPipelineAndCacheStrategy);
    }

    @GetMapping("/benchmark/flush")
    public ResponseEntity<String> flush() {
        redissonClient.getKeys().flushall();
        return ResponseEntity.ok("flushed");
    }

    @GetMapping("/benchmark/brute-force")
    public ResponseEntity<String> bruteForceBenchmark(@RequestParam(value = "testFilePath") String testFilePath) {
        return this.benchmark(ownerExpenseServiceBrute, testFilePath);
    }

    @GetMapping("/benchmark/pipeline")
    public ResponseEntity<String> pipelineBenchmark(@RequestParam(value = "testFilePath") String testFilePath) {
        return this.benchmark(ownerExpenseServicePipeline, testFilePath);
    }

    @GetMapping("/benchmark/lock")
    public ResponseEntity<String> lockBenchmark(@RequestParam(value = "testFilePath") String testFilePath) {
        return this.benchmark(ownerExpenseServiceLock, testFilePath);
    }

    @GetMapping("/benchmark/pipeline-cache")
    public ResponseEntity<String> pipelineAndCacheBenchmark(@RequestParam(value = "testFilePath") String testFilePath) {
        sumRedisHashByVariableAndPipelineAndCache.clearCache();
        return this.benchmark(ownerExpenseServicePipelineAndCache, testFilePath);
    }

    @GetMapping("/benchmark/all")
    public ResponseEntity<String> all(@RequestParam(value = "testFilePath") String testFilePath) {
        return this.joinResponses(this.benchmark(ownerExpenseServiceBrute, testFilePath),
                                  this.benchmark(ownerExpenseServicePipeline, testFilePath),
                                  this.benchmark(ownerExpenseServiceLock, testFilePath),
                                  this.benchmark(ownerExpenseServicePipelineAndCache, testFilePath));
    }

    @GetMapping("/benchmark/latency-check")
    public ResponseEntity<String> latencyCheck() {
        RBucket<String> bucket = this.redissonClient.getBucket("latency-check");
        long start = System.nanoTime();
        bucket.set("latency-check");
        long second = System.nanoTime();
        bucket.get();
        long third = System.nanoTime();

        String latency = String.format("set latency: %sms, get latency: %sms", (second - start) / 1000000.0d, (third - second) / 1000000.0d);
        return ResponseEntity.ok(latency);
    }

    private ResponseEntity<String> benchmark(OwnerExpenseService ownerExpenseService, String testFilePath) {
        Expense[] expenses;
        String prefix = "src/main/resources/benchmark/";
        String suffix = ".json";
        Map<String, BigDecimal> expectedOutput;
        List<Double> qpsList = new ArrayList<>(4000);
        try {
            String data = new String(Files.readAllBytes(Paths.get(prefix + testFilePath + suffix)), StandardCharsets.UTF_8);
            expenses = JsonUtil.parseArray(data, Expense[].class);
            String result = new String(Files.readAllBytes(Paths.get(prefix + testFilePath + "-result" + suffix)), StandardCharsets.UTF_8);
            expectedOutput = (Map<String, BigDecimal>) JsonUtil.parseObject(result, Map.class).entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> ((Map.Entry) entry).getKey(),
                            entry -> new BigDecimal(((Map.Entry) entry).getValue().toString())
                    ));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        redissonClient.getKeys().flushall();
        Set<String> owners = new HashSet<>();
        int i = 0;
        long start = System.nanoTime();
        long lastTenStart = start;

        boolean backtrackedOne = false;
        boolean backtrackedTwo = false;

        for (int j = 0; j < expenses.length; j++) {
            Expense expense = expenses[j];
            owners.add(expense.getOwnerId());
            BigDecimal bigDecimal = ownerExpenseService.updateExpenseAndAggregate(expense);
            i++;
            if (i % 10 == 0) {
                double qps = this.getQps(10, lastTenStart);
                log.info("query num: {}, current aggregate: {} ownerId: {} qps: {}", i, bigDecimal, expense.getOwnerId(), qps);
                lastTenStart = System.nanoTime();
                qpsList.add(qps);
            }
            if (j == 5000 && !backtrackedOne) {
                backtrackedOne = true;
                j = 4000;
            }
            if (j == 25000 && !backtrackedTwo) {
                backtrackedTwo = true;
                j = 24560;
            }
        }
        String elapsed = String.format("total time elapsed: %s(s)", (System.nanoTime() - start) / 1000000000.0d);
        String description = String.format("strategy: %s", ownerExpenseService.strategyName());
        String overallQps = String.format("overall qps: %s", this.getQps(i, start));
        Collections.sort(qpsList);
        String p99qps = String.format("p99 qps: %s", qpsList.get(qpsList.size() / 100));
        log.info(overallQps);
        log.info(p99qps);
        log.info(elapsed);
        Map<String, BigDecimal> result = owners.stream().collect(Collectors.toMap(
                Function.identity(),
                owner -> ownerExpenseService.aggregate(owner)
        ));

        redissonClient.getKeys().flushall();
        boolean equals = result.equals(expectedOutput);
        log.info("result: {}, expected result: {}, is equal to expectation: {}", result, expectedOutput, equals);
        if (!equals) {
            throw new RuntimeException("result not equal to expectation");
        }
        return ResponseEntity.ok(description + "\n" + overallQps + "\n" + p99qps + "\n" + elapsed);
    }

    private double getQps(int queries, long startInNanos) {
        long elapsedTime = System.nanoTime() - startInNanos;
        double seconds = elapsedTime / 1_000_000_000.0; // Convert to seconds

        return queries / seconds;
    }

    private ResponseEntity<String> joinResponses(ResponseEntity<String>... responses) {
        StringBuilder body = new StringBuilder();
        for (ResponseEntity<String> response : responses) {
            body.append(response.getBody());
            body.append("\n\n");
        }
        return ResponseEntity.ok(body.toString());
    }
}
