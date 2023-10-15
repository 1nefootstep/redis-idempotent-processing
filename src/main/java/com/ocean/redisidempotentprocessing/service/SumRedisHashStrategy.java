package com.ocean.redisidempotentprocessing.service;

import java.math.BigDecimal;

public interface SumRedisHashStrategy {
    String strategyName();

    BigDecimal setAndSum(String redisKey, String hashKey, BigDecimal value);

    void set(String redisKey, String hashKey, BigDecimal value);

    BigDecimal sum(String redisKey);
}
