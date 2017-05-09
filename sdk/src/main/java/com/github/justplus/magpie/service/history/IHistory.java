package com.github.justplus.magpie.service.history;

import com.github.justplus.magpie.model.Task;

import java.util.List;

/**
 * Created by zhaoliang on 2017/5/9.
 */
public interface IHistory {
    /**
     * 生产消息后及时落地,确保消息不会丢失
     *
     * @param task 消息体
     * @return 返回消息是否落地成功
     */
    boolean insertProducerHistory(Task task);

    /**
     * 上一条消息是否被消费
     *
     * @param taskId 消息id
     * @return 返回上一条消息是否被消费
     */
    boolean preTaskConsumed(String taskId);

    /**
     * 判断消息是否被消费过,防止重复消费
     *
     * @param taskId 消息id
     * @return 返回消息是否被消费
     */
    boolean isTaskConsumed(String taskId);

    /**
     * 更新消息的消费状态
     *
     * @param taskId 消息id
     * @return 是否更新成功
     */
    boolean updateConsumerState(String taskId);

    /**
     * 获取没有被正常消费的任务列表
     *
     * @param timeout 多长时间内没有正确响应的算作超时任务
     * @return 返回没有被正常消费的消息列表
     */
    List<Task> listUnConsumedTasks(int timeout);
}
