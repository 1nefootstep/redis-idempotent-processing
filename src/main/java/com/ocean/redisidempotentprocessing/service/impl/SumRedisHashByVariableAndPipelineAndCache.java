package com.ocean.redisidempotentprocessing.service.impl;

import com.ocean.redisidempotentprocessing.service.SumRedisHashStrategy;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.BatchOptions;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RBucket;
import org.redisson.api.RLocalCachedMap;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Component("sumRedisHashByVariableAndPipelineAndCache")
@Slf4j
public class SumRedisHashByVariableAndPipelineAndCache implements SumRedisHashStrategy {
    public static final String AGGREGATE_SUFFIX = ":aggregate";

    @Autowired
    private RedissonClient redissonClient;
    private Map<String, BigDecimal> aggregateMap = new HashMap<>();

    @Override
    public String strategyName() {
        return "VariablePipelineAndCache";
    }

    @Override
    public BigDecimal setAndSum(String redisKey, String hashKey, BigDecimal value) {
        String aggregateRedisKey = this.getAggregateRedisKey(redisKey);

        RLocalCachedMap<String, BigDecimal> expenseMap = this.redissonClient.getLocalCachedMap(redisKey, LocalCachedMapOptions.<String, BigDecimal>defaults());
        BigDecimal prevValue = Objects.requireNonNullElse(expenseMap.get(hashKey), BigDecimal.ZERO);
        BigDecimal prevAggregate = Objects.requireNonNullElse(this.getAggregate(aggregateRedisKey), BigDecimal.ZERO);
        BigDecimal newAggregate = prevAggregate.subtract(prevValue).add(value);

        RBatch batch = this.redissonClient.createBatch(
                BatchOptions.defaults().executionMode(BatchOptions.ExecutionMode.IN_MEMORY_ATOMIC));
        batch.getMap(redisKey).fastPutAsync(hashKey, value);
        this.putAggregate(batch, aggregateRedisKey, newAggregate);

        batch.execute();
        return newAggregate;
    }

    @Override
    public void set(String redisKey, String hashKey, BigDecimal value) {
        String aggregateRedisKey = this.getAggregateRedisKey(redisKey);

        RLocalCachedMap<String, BigDecimal> expenseMap = this.redissonClient.getLocalCachedMap(redisKey, LocalCachedMapOptions.<String, BigDecimal>defaults());
        BigDecimal prevValue = Objects.requireNonNullElse(expenseMap.get(hashKey), BigDecimal.ZERO);
        BigDecimal prevAggregate = Objects.requireNonNullElse(this.getAggregate(aggregateRedisKey), BigDecimal.ZERO);
        BigDecimal newAggregate = prevAggregate.subtract(prevValue).add(value);

        RBatch batch = this.redissonClient.createBatch(
                BatchOptions.defaults().executionMode(BatchOptions.ExecutionMode.IN_MEMORY_ATOMIC));
        batch.getMap(redisKey).fastPutAsync(hashKey, value);
        this.putAggregate(batch, aggregateRedisKey, newAggregate);

        batch.execute();
    }

    @Override
    public BigDecimal sum(String redisKey) {
        RBucket<BigDecimal> bucket = this.redissonClient.getBucket(this.getAggregateRedisKey(redisKey));
        return bucket.get();
    }

    public void clearCache() {
        this.aggregateMap.clear();
    }

    private BigDecimal getAggregate(String aggregateRedisKey) {
        BigDecimal result = this.aggregateMap.get(aggregateRedisKey);
        if (result != null) {
            return result;
        }

        BigDecimal redisResult = (BigDecimal) this.redissonClient.getBucket(aggregateRedisKey).get();
        if (redisResult != null) {
            this.aggregateMap.put(aggregateRedisKey, redisResult);
        }
        return redisResult;
    }

    private void putAggregate(RBatch rBatch, String aggregateRedisKey, BigDecimal value) {
        this.aggregateMap.put(aggregateRedisKey, value);
        rBatch.getBucket(aggregateRedisKey).setAsync(value);
    }

    private String getAggregateRedisKey(String redisKey) {
        return redisKey + AGGREGATE_SUFFIX;
    }
}
