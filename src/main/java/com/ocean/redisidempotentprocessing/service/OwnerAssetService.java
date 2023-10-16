package com.ocean.redisidempotentprocessing.service;

import com.ocean.redisidempotentprocessing.event.AssetEvent;

import java.math.BigDecimal;

public interface OwnerAssetService {
    String strategyName();

    BigDecimal updateAssetAndAggregate(AssetEvent assetEvent);

    BigDecimal aggregate(String ownerId);
}
