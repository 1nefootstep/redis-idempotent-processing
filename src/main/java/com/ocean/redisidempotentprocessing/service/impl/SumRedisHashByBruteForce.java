package com.ocean.redisidempotentprocessing.service.impl;

import com.ocean.redisidempotentprocessing.service.SumRedisHashStrategy;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component("sumRedisHashByBruteForce")
@Slf4j
public class SumRedisHashByBruteForce implements SumRedisHashStrategy {
    @Autowired
    private RedissonClient redissonClient;

    @Override
    public String strategyName() {
        return "bruteForce";
    }

    @Override
    public BigDecimal setAndSum(String redisKey, String hashKey, BigDecimal value) {
        this.set(redisKey, hashKey, value);
        return this.sum(redisKey);
    }

    @Override
    public void set(String redisKey, String hashKey, BigDecimal value) {
        RMap<String, BigDecimal> rMap = this.redissonClient.getMap(redisKey);
        rMap.put(hashKey, value);
    }

    @Override
    public BigDecimal sum(String redisKey) {
        RMap<String, BigDecimal> rMap = this.redissonClient.getMap(redisKey);
        BigDecimal result = BigDecimal.ZERO;
        for (BigDecimal value : rMap.readAllValues()) {
            result = result.add(value);
        }
        return result;
    }
}
