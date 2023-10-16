package com.ocean.redisidempotentprocessing.service.impl;

import com.ocean.redisidempotentprocessing.event.AssetEvent;
import com.ocean.redisidempotentprocessing.service.OwnerAssetService;
import com.ocean.redisidempotentprocessing.service.SumRedisHashStrategy;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class OwnerAssetServiceImpl implements OwnerAssetService {
    private final SumRedisHashStrategy sumRedisHashStrategy;
    private static final String REDIS_KEY_TEMPLATE = "ocean:expense:{owner:%s}";

    public OwnerAssetServiceImpl(SumRedisHashStrategy sumRedisHashStrategy) {
        this.sumRedisHashStrategy = sumRedisHashStrategy;
    }

    @Override
    public String strategyName() {
        return sumRedisHashStrategy.strategyName();
    }

    @Override
    public BigDecimal updateAssetAndAggregate(AssetEvent assetEvent) {
        String redisKey = this.getRedisKey(assetEvent);
        BigDecimal bdExpense = new BigDecimal(assetEvent.getAsset()).setScale(16, RoundingMode.FLOOR);
        return sumRedisHashStrategy.setAndSum(redisKey, assetEvent.getBusinessId(), bdExpense);
    }

    @Override
    public BigDecimal aggregate(String ownerId) {
        return sumRedisHashStrategy.sum(this.getRedisKey(ownerId));
    }

    private String getRedisKey(AssetEvent assetEvent) {
        return this.getRedisKey(assetEvent.getOwnerId());
    }

    private String getRedisKey(String ownerId) {
        return String.format(REDIS_KEY_TEMPLATE, ownerId);
    }
}
