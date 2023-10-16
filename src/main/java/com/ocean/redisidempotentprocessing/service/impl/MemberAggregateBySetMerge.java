package com.ocean.redisidempotentprocessing.service.impl;

import com.ocean.redisidempotentprocessing.service.MemberAggregateStrategy;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.BatchOptions;
import org.redisson.api.BatchResult;
import org.redisson.api.RBatch;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component("memberAggregateBySetMerge")
@Slf4j
public class MemberAggregateBySetMerge implements MemberAggregateStrategy {
    @Autowired
    private RedissonClient redissonClient;

    @Override
    public String strategyName() {
        return "MemberAggregateBySetMerge";
    }

    @Override
    public int addNewMemberAndSum(String redisKey, String businessId, String memberId) {
        RBatch batch1 = this.redissonClient.createBatch(BatchOptions.defaults().executionMode(BatchOptions.ExecutionMode.IN_MEMORY_ATOMIC));
        batch1.getSet(redisKey).addAsync(businessId);
        String businessRedisKey = this.getRedisSetKeyForMembers(redisKey, businessId);
        batch1.getSet(businessRedisKey).addAsync(memberId);
        batch1.getSet(redisKey).readAllAsync();
        BatchResult<?> batchResult = batch1.execute();
        List<?> responses = batchResult.getResponses();

        Set<String> businessIds = (Set<String>) responses.get(2);
        businessIds.add(businessId);
        String[] businessRedisKeys = this.getBusinessRedisKeys(redisKey, businessIds);
        return this.redissonClient.getSet(businessRedisKey).readUnion(businessRedisKeys).size();
    }

    @Override
    public int removeMemberAndSum(String redisKey, String businessId, String memberId) {
        RBatch batch1 = this.redissonClient.createBatch(BatchOptions.defaults().executionMode(BatchOptions.ExecutionMode.IN_MEMORY_ATOMIC));
        String businessRedisKey = this.getRedisSetKeyForMembers(redisKey, businessId);
        batch1.getSet(businessRedisKey).removeAsync(memberId);
        batch1.getSet(redisKey).readAllAsync();
        BatchResult<?> batchResult = batch1.execute();
        List<?> responses = batchResult.getResponses();

        Set<String> businessIds = (Set<String>) responses.get(1);
        String[] businessRedisKeys = this.getBusinessRedisKeys(redisKey, businessIds);
        return this.redissonClient.getSet(businessRedisKey).readUnion(businessRedisKeys).size();
    }

    @Override
    public int count(String redisKey) {
        Set<String> businessIds = this.redissonClient.<String>getSet(redisKey).readAll();
        if (businessIds.isEmpty()) {
            return 0;
        }
        String[] businessRedisKeys = this.getBusinessRedisKeys(redisKey, businessIds);
        return this.redissonClient.getSet(businessRedisKeys[0]).readUnion(businessRedisKeys).size();
    }

    private String[] getBusinessRedisKeys(String redisKey, Set<String> businessIds) {
        String[] businessRedisKeys = new String[businessIds.size()];
        int i = 0;
        for (String id : businessIds) {
            businessRedisKeys[i] = this.getRedisSetKeyForMembers(redisKey, id);
            i++;
        }
        return businessRedisKeys;
    }

    private String getRedisSetKeyForMembers(String redisKey, String businessId) {
        return String.format("%s:%s", redisKey, businessId);
    }
}
