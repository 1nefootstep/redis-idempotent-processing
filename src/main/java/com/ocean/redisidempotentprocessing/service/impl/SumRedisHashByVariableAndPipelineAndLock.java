package com.ocean.redisidempotentprocessing.service.impl;

import com.ocean.redisidempotentprocessing.service.SumRedisHashStrategy;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.BatchOptions;
import org.redisson.api.BatchResult;
import org.redisson.api.RBatch;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Objects;

@Component("sumRedisHashByVariableAndPipelineAndLock")
@Slf4j
public class SumRedisHashByVariableAndPipelineAndLock implements SumRedisHashStrategy {
    public static final String AGGREGATE_SUFFIX = ":aggregate";
    public static final String LOCK_SUFFIX = ":lock";

    @Autowired
    private RedissonClient redissonClient;

    @Override
    public String strategyName() {
        return "VariablePipelineAndLock";
    }

    @Override
    public BigDecimal setAndSum(String redisKey, String hashKey, BigDecimal value) {
        RLock lock = this.redissonClient.getLock(this.getLockRedisKey(redisKey));
        lock.lock();
        String aggregateRedisKey = this.getAggregateRedisKey(redisKey);

        RBatch batchRead = this.redissonClient.createBatch();
        batchRead.getMap(redisKey).getAsync(hashKey);
        batchRead.getBucket(aggregateRedisKey).getAsync();
        BatchResult<?> batchReadResult = batchRead.execute();
        BigDecimal prevValue = Objects.requireNonNullElse((BigDecimal) batchReadResult.getResponses().get(0), BigDecimal.ZERO);
        BigDecimal prevAggregate = Objects.requireNonNullElse((BigDecimal) batchReadResult.getResponses().get(1), BigDecimal.ZERO);
        BigDecimal newAggregate = prevAggregate.subtract(prevValue).add(value);

        RBatch batchWrite = this.redissonClient.createBatch(
                BatchOptions.defaults().executionMode(BatchOptions.ExecutionMode.IN_MEMORY_ATOMIC));
        batchWrite.getMap(redisKey).fastPutAsync(hashKey, value);
        batchWrite.getBucket(aggregateRedisKey).setAsync(newAggregate);

        batchWrite.execute();
        lock.unlock();
        return newAggregate;
    }

    @Override
    public void set(String redisKey, String hashKey, BigDecimal value) {
        RLock lock = this.redissonClient.getLock(this.getLockRedisKey(redisKey));
        lock.lock();
        String aggregateRedisKey = this.getAggregateRedisKey(redisKey);

        RBatch batchRead = this.redissonClient.createBatch();
        batchRead.getMap(redisKey).getAsync(hashKey);
        batchRead.getBucket(aggregateRedisKey).getAsync();
        BatchResult<?> batchReadResult = batchRead.execute();
        BigDecimal prevValue = Objects.requireNonNullElse((BigDecimal) batchReadResult.getResponses().get(0), BigDecimal.ZERO);
        BigDecimal prevAggregate = Objects.requireNonNullElse((BigDecimal) batchReadResult.getResponses().get(1), BigDecimal.ZERO);
        BigDecimal newAggregate = prevAggregate.subtract(prevValue).add(value);

        RBatch batchWrite = this.redissonClient.createBatch(
                BatchOptions.defaults().executionMode(BatchOptions.ExecutionMode.IN_MEMORY_ATOMIC));
        batchWrite.getMap(redisKey).fastPutAsync(hashKey, value);
        batchWrite.getBucket(aggregateRedisKey).setAsync(newAggregate);

        batchWrite.execute();
        lock.unlock();
    }

    @Override
    public BigDecimal sum(String redisKey) {
        RBucket<BigDecimal> bucket = this.redissonClient.getBucket(this.getAggregateRedisKey(redisKey));
        return bucket.get();
    }

    private String getAggregateRedisKey(String redisKey) {
        return redisKey + AGGREGATE_SUFFIX;
    }

    private String getLockRedisKey(String redisKey) {
        return redisKey + LOCK_SUFFIX;
    }

}
