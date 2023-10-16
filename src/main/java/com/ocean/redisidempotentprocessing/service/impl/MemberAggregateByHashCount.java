package com.ocean.redisidempotentprocessing.service.impl;

import com.ocean.redisidempotentprocessing.service.MemberAggregateStrategy;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RMap;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.IntegerCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("memberAggregateByHashCount")
@Slf4j
public class MemberAggregateByHashCount implements MemberAggregateStrategy {
    @Autowired
    private RedissonClient redissonClient;

    // KEYS[1] = redisKey, KEYS[2] = businessRedisKey, ARGV[1] = memberId
    private static final String ADD_SCRIPT = """
            if redis.call('sadd', KEYS[2], ARGV[1]) == 0 then
                return redis.call('hlen', KEYS[1])
            else
                redis.call('hincrby', KEYS[1], ARGV[1], 1)
                return redis.call('hlen', KEYS[1])
            end
            """;
    private static final String REMOVE_SCRIPT = """
            if redis.call('srem', KEYS[2], ARGV[1]) == 0 then
                return redis.call('hlen', KEYS[1])
            else
                if redis.call('hincrby', KEYS[1], ARGV[1], -1) <= 0 then
                    redis.call('hdel', KEYS[1], ARGV[1])
                end
                return redis.call('hlen', KEYS[1])
            end
            """;

    @Override
    public String strategyName() {
        return "MemberAggregateByHashCount";
    }

    @Override
    public int addNewMemberAndSum(String redisKey, String businessId, String memberId) {
        String businessRedisKey = this.getRedisSetKeyForMembers(redisKey, businessId);
        return ((Long) this.redissonClient.getScript().eval(RScript.Mode.READ_WRITE,
                                                            ADD_SCRIPT,
                                                            RScript.ReturnType.INTEGER,
                                                            List.of(redisKey, businessRedisKey),
                                                            memberId)).intValue();
    }

    @Override
    public int removeMemberAndSum(String redisKey, String businessId, String memberId) {
        String businessRedisKey = this.getRedisSetKeyForMembers(redisKey, businessId);
        return ((Long) this.redissonClient.getScript().eval(RScript.Mode.READ_WRITE,
                                                            REMOVE_SCRIPT,
                                                            RScript.ReturnType.INTEGER,
                                                            List.of(redisKey, businessRedisKey),
                                                            memberId)).intValue();
    }

    @Override
    public int count(String redisKey) {
        return this.getMap(redisKey).size();
    }

    private RMap<String, Integer> getMap(String redisKey) {
        return this.redissonClient.getMap(redisKey, IntegerCodec.INSTANCE);
    }

    private String getRedisSetKeyForMembers(String redisKey, String businessId) {
        return String.format("%s:%s", redisKey, businessId);
    }
}
