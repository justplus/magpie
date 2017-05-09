package com.github.justplus.magpie.service.consumer;

import com.github.justplus.magpie.service.filter.AbstractFilter;
import com.github.justplus.magpie.service.history.IHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by zhaoliang on 2017/5/3.
 */
public abstract class AbstractConsumer {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 取出的meta数据,可能需要进行一系列处理才能进入到ES等
     * iFilters是针对新增数据进行的数据处理器
     * uFilter是针对更新操作进行的数据处理器
     * dFilter是针对删除操作进行的数据处理器,暂时可能也用不上,属于保留字段
     */
    List<AbstractFilter> iFilters;
    List<AbstractFilter> uFilters;
    List<AbstractFilter> dFilters;

    private IHistory history;

    boolean canStartConsume(String taskId) {
        if (history != null) {
            return this.history.isTaskConsumed(taskId) || !this.history.preTaskConsumed(taskId);
        }
        return true;
    }

    /**
     * 消息处理完毕后,通知落地的消息变更消费状态
     *
     * @param taskId 任务id
     */
    void afterProcess(String taskId) {
        if (history != null) {
            history.updateConsumerState(taskId);
        }
    }


    public void setuFilters(List<AbstractFilter> uFilters) {
        this.uFilters = uFilters;
    }

    public void setiFilters(List<AbstractFilter> iFilters) {
        this.iFilters = iFilters;
    }

    public void setdFilters(List<AbstractFilter> dFilters) {
        this.dFilters = dFilters;
    }

    public void setHistory(IHistory history) {
        this.history = history;
    }
}
