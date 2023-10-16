package com.ocean.redisidempotentprocessing.service.impl;

import com.ocean.redisidempotentprocessing.event.MemberEvent;
import com.ocean.redisidempotentprocessing.service.MemberAggregateStrategy;
import com.ocean.redisidempotentprocessing.service.OwnerMemberService;

public class OwnerMemberServiceImpl implements OwnerMemberService {
    private final MemberAggregateStrategy memberAggregateStrategy;
    private static final String REDIS_KEY_TEMPLATE = "ocean:member:{owner:%s}";

    public OwnerMemberServiceImpl(MemberAggregateStrategy memberAggregateStrategy) {
        this.memberAggregateStrategy = memberAggregateStrategy;
    }

    @Override
    public String strategyName() {
        return memberAggregateStrategy.strategyName();
    }

    @Override
    public int updateMemberAndAggregate(MemberEvent memberEvent) {
        String redisKey = this.getRedisKey(memberEvent);
        if (memberEvent.getStatus() == MemberEvent.Status.MEMBER) {
            return memberAggregateStrategy.addNewMemberAndSum(redisKey, memberEvent.getBusinessId(), memberEvent.getMemberId());
        } else {
            return memberAggregateStrategy.removeMemberAndSum(redisKey, memberEvent.getBusinessId(), memberEvent.getMemberId());
        }
    }

    @Override
    public int aggregate(String ownerId) {
        return memberAggregateStrategy.count(this.getRedisKey(ownerId));
    }

    private String getRedisKey(MemberEvent memberEvent) {
        return this.getRedisKey(memberEvent.getOwnerId());
    }

    private String getRedisKey(String ownerId) {
        return String.format(REDIS_KEY_TEMPLATE, ownerId);
    }
}
