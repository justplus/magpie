package com.github.justplus.magpie.service.filter;

import com.github.justplus.magpie.exception.DiscardException;
import com.github.justplus.magpie.exception.RetryException;
import com.github.justplus.magpie.model.Task;

/**
 * Created by zhaoliang on 2017/5/3.
 */
public abstract class AbstractFilter {
    protected abstract Task filter(Task task) throws DiscardException, RetryException;

    public Task doFilter(Task task) throws DiscardException, RetryException {
        if (task != null) {
            return this.filter(task);
        }
        throw new DiscardException("task is null");
    }
}
