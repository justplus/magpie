package com.github.justplus.magpie.filters;

import com.github.justplus.magpie.exception.DiscardException;
import com.github.justplus.magpie.exception.RetryException;
import com.github.justplus.magpie.model.OperationEnum;
import com.github.justplus.magpie.model.Task;
import com.github.justplus.magpie.service.filter.AbstractFilter;
import com.github.justplus.magpie.utils.ESClient;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by zhaoliang on 2017/5/8.
 */
public class UpdateIndexFilter extends AbstractFilter {
    private final static String INDEX = "estream";

    @Autowired
    ESClient elesticClient;

    @Override
    protected Task filter(Task task) throws DiscardException, RetryException {
        if (task.getOperation() == OperationEnum.INSERT) {
            this.elesticClient.insertIndex(INDEX, task.getProducerName(), task.getBizId(),
                    task.getMetaData());
        } else if (task.getOperation() == OperationEnum.UPDATE) {
            this.elesticClient.updateIndex(INDEX, task.getProducerName(), task.getBizId(), task.getMetaData());
        } else if (task.getOperation() == OperationEnum.DELETE) {
            this.elesticClient.deleteIndex(INDEX, task.getProducerName(), task.getBizId());
        }
        return null;
    }
}
