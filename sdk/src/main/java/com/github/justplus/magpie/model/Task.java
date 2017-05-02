package com.github.justplus.magpie.model;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaoliang on 2017/3/28.
 */
public class Task implements Serializable, Cloneable {
    //业务id, 一般为数据表的主键或者集合中的唯一性字段。该字段尤其重要,是后续更新或者删除操作的唯一标识
    private String bizId;
    //操作类型,新增/更新/删除
    private OperationEnum operation;
    //元数据
    private Map<String, Object> metaData;
    //版本号
    private AtomicInteger version = new AtomicInteger(0);
    //生产者名称, 一般指定为业务名称, 全局唯一
    private String producerName;

    public Task(String bizId, OperationEnum operation, Map<String, Object> metaData, String producerName) {
        this.operation = operation;
        this.metaData = metaData;
        this.producerName = producerName;
        this.bizId = String.format("%s_%s", producerName, bizId);
    }

    public final int incrementAndGet() {
        return this.version.incrementAndGet();
    }

    @Override
    public Object clone() {
        Task t = null;
        try {
            t = (Task) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return t;
    }

    /**********************
     * getter and setter
     **********************/
    public String getBizId() {
        return bizId;
    }

    public void setBizId(String bizId) {
        this.bizId = bizId;
    }

    public OperationEnum getOperation() {
        return operation;
    }

    public void setOperation(OperationEnum operation) {
        this.operation = operation;
    }

    public Map<String, Object> getMetaData() {
        return metaData;
    }

    public void setMetaData(Map<String, Object> metaData) {
        this.metaData = metaData;
    }

    public AtomicInteger getVersion() {
        return version;
    }

    public void setVersion(AtomicInteger version) {
        this.version = version;
    }

    public String getProducerName() {
        return producerName;
    }

    public void setProducerName(String producerName) {
        this.producerName = producerName;
    }
}
