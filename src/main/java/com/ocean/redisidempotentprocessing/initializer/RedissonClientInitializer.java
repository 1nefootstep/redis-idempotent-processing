package com.ocean.redisidempotentprocessing.initializer;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.Kryo5Codec;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonClientInitializer {
    @Bean
    public RedissonClient redissonClient() {
        Config config = new Config();
        config.setCodec(new Kryo5Codec());
        config.useClusterServers().addNodeAddress("redis://10.0.0.11:7001");
        return Redisson.create(config);
    }
}
