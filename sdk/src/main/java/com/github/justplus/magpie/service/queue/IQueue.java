package com.github.justplus.magpie.service.queue;

import com.github.justplus.magpie.model.Task;

import java.util.List;

/**
 * Created by zhaoliang on 2017/5/2.
 */
public interface IQueue {
    /**
     * 将任务入队列
     *
     * @param task 任务
     */
    void push(final Task task);

    /**
     * 将任务批量入队列
     *
     * @param tasks 任务列表
     */
    void bathPush(final List<Task> tasks);

    /**
     * 取出一条任务
     *
     * @return 返回取出的任务
     */
    Task pop();

    /**
     * 批量取出任务列表
     *
     * @param limit 取出的任务数量
     * @return 取出的任务列表
     */
    List<Task> bathPop(int limit);
}
