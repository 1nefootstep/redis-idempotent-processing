package com.ocean.redisidempotentprocessing.service;

public interface MemberAggregateStrategy {
    String strategyName();

    int addNewMemberAndSum(String redisKey, String businessId, String memberId);

    int removeMemberAndSum(String redisKey, String businessId, String memberId);

    int count(String redisKey);
}
