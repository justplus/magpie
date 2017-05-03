package com.github.justplus.magpie.service.producer;

import com.github.justplus.magpie.model.Task;
import com.github.justplus.magpie.service.queue.IQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by zhaoliang on 2017/5/2.
 */
public abstract class AbstractProducer {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    public abstract List<Task> produce();

    // 生产者名称,唯一
    protected String producerName;
    // 队列
    protected IQueue queue;

    /**
     * 开启存量数据生产任务
     * 存量数据需要将数据返回后批量插入队列
     */
    public boolean start() {
        List<Task> tasks = this.produce();
        if (tasks != null && tasks.size() > 0) {
            this.queue.bathPush(tasks);
        }
        return tasks == null || tasks.size() == 0;
    }

    /**
     * 增量数据生产任务
     * 增量数据直接产生数据的同时将对应的任务压入队列, 不需要返回来处理
     */
    public void incStart() {
        this.produce();
    }


    /**********************
     * getter and setter
     **********************/
    public void setQueue(IQueue queue) {
        this.queue = queue;
    }

    public IQueue getQueue() {
        return queue;
    }

    public void setProducerName(String producerName) {
        this.producerName = producerName;
    }

    public String getProducerName() {
        return producerName;
    }
}
