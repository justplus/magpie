package com.github.justplus.magpie.service.record;

/**
 * Created by zhaoliang on 2017/5/3.
 */
public interface IRecord {
    /**
     * 获取某生产者生产数据的最新位置
     *
     * @param producerName 生产者名称
     * @return 返回最新位置
     */
    String getLatestPos(String producerName);

    /**
     * 设置某生产者生产数据的最新位置
     *
     * @param producerName 生产者名称
     * @param pos          最新位置
     */
    void setLatestPos(String producerName, String pos);

    /**
     * 原子操作
     *
     * @param producerName 生产者名称
     * @param pos          位置
     * @return 返回最新位置
     */
    String getAndSetLatestPos(String producerName, String pos);
}
