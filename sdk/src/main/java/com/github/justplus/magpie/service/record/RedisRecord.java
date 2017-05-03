package com.github.justplus.magpie.service.record;

import org.springframework.data.redis.core.RedisTemplate;

/**
 * Created by zhaoliang on 2017/5/3.
 */
public class RedisRecord implements IRecord {
    private RedisTemplate redisTemplate;

    public RedisRecord(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 获取某生产者生产数据的最新位置
     *
     * @param producerName 生产者名称
     * @return 返回最新位置
     */
    @Override
    public String getLatestPos(String producerName) {
        final String key = producerName + ":lastest:pos:";
        String pos = (String) this.redisTemplate.opsForValue().get(key);
        return pos;
    }

    /**
     * 设置某生产者生产数据的最新位置
     *
     * @param producerName 生产者名称
     * @param pos          最新位置
     */
    @Override
    public void setLatestPos(String producerName, String pos) {
        final String key = producerName + ":lastest:pos:";
        this.redisTemplate.opsForValue().set(key, pos);
    }

    /**
     * 原子操作
     *
     * @param producerName 生产者名称
     * @param pos          位置
     * @return 返回最新位置
     */
    @Override
    public String getAndSetLatestPos(String producerName, String pos) {
        final String key = producerName + ":lastest:pos:";
        String oldPos = (String) this.redisTemplate.opsForValue().getAndSet(key, pos);
        return oldPos;
    }
}
