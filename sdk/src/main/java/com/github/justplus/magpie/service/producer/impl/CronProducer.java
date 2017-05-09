package com.github.justplus.magpie.service.producer.impl;

import com.github.justplus.magpie.model.Task;
import com.github.justplus.magpie.service.history.IHistory;
import com.github.justplus.magpie.service.producer.AbstractProducer;

import java.util.List;

/**
 * Created by zhaoliang on 2017/5/9.
 */
public class CronProducer extends AbstractProducer {
    private IHistory history;
    private int timeout;

    public CronProducer(IHistory history, int timeout) {
        this.history = history;
        this.timeout = timeout;
    }

    public CronProducer(IHistory history) {
        this(history, 5);
    }

    @Override
    public List<Task> produce() {
        List<Task> tasks = this.history.listUnConsumedTasks(this.timeout);
        for (Task task : tasks) {
            this.queue.push(task);
        }
        return null;
    }
}
