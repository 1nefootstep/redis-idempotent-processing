package com.ocean.redisidempotentprocessing.service.impl;

import com.ocean.redisidempotentprocessing.service.MemberAggregateStrategy;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.BatchOptions;
import org.redisson.api.BatchResult;
import org.redisson.api.RBatch;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component("memberAggregateBySetMergeInJvm")
@Slf4j
public class MemberAggregateBySetMergeInJvm implements MemberAggregateStrategy {
    @Autowired
    private RedissonClient redissonClient;

    @Override
    public String strategyName() {
        return "MemberAggregateBySetMergeInJvm";
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
        return this.count(businessIds, businessRedisKeys);
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
        return this.count(businessIds, businessRedisKeys);
    }

    @Override
    public int count(String redisKey) {
        Set<String> businessIds = this.redissonClient.<String>getSet(redisKey).readAll();
        if (businessIds.isEmpty()) {
            return 0;
        }
        RBatch batch = this.redissonClient.createBatch(BatchOptions.defaults());
        String[] businessRedisKeys = this.getBusinessRedisKeys(redisKey, businessIds);
        for (String businessRedisKey : businessRedisKeys) {
            batch.getSet(businessRedisKey).readAllAsync();
        }
        BatchResult<?> batchResult = batch.execute();
        List<?> responses = batchResult.getResponses();
        Set<String> members = new HashSet<>();
        for (Object response : responses) {
            members.addAll((Set<String>) response);
        }
        return members.size();
    }

    public int count(Set<String> businessIds, String[] businessRedisKeys) {
        if (businessIds.isEmpty()) {
            return 0;
        }
        RBatch batch = this.redissonClient.createBatch(BatchOptions.defaults());
        for (String businessRedisKey : businessRedisKeys) {
            batch.getSet(businessRedisKey).readAllAsync();
        }
        BatchResult<?> batchResult = batch.execute();
        List<?> responses = batchResult.getResponses();
        Set<String> members = new HashSet<>();
        for (Object response : responses) {
            members.addAll((Set<String>) response);
        }
        return members.size();
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
