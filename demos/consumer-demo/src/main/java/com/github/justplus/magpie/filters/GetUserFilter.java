package com.github.justplus.magpie.filters;

import com.github.justplus.magpie.exception.DiscardException;
import com.github.justplus.magpie.exception.RetryException;
import com.github.justplus.magpie.model.Task;
import com.github.justplus.magpie.service.filter.AbstractFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by zhaoliang on 2017/5/8.
 */
public class GetUserFilter extends AbstractFilter {
    protected final static Logger logger = LoggerFactory.getLogger(GetUserFilter.class);

    @Override
    protected Task filter(Task task) throws DiscardException, RetryException {
        Map<String, Object> metaData = task.getMetaData();
        metaData.put("author", "justplus");
        metaData.put("licence", "MIT");
        task.setMetaData(metaData);
        return task;
    }
}
