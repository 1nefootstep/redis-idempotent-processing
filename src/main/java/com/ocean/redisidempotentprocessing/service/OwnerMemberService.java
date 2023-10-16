package com.ocean.redisidempotentprocessing.service;

import com.ocean.redisidempotentprocessing.event.MemberEvent;

public interface OwnerMemberService {
    String strategyName();

    int updateMemberAndAggregate(MemberEvent memberEvent);

    int aggregate(String ownerId);
}
