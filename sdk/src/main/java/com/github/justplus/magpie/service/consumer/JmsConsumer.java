package com.github.justplus.magpie.service.consumer;

import com.github.justplus.magpie.exception.DiscardException;
import com.github.justplus.magpie.exception.RetryException;
import com.github.justplus.magpie.model.OperationEnum;
import com.github.justplus.magpie.model.Task;
import com.github.justplus.magpie.service.filter.AbstractFilter;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.JmsUtils;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhaoliang on 2017/5/3.
 */
public class JmsConsumer extends AbstractConsumer implements MessageListener {
    JmsTemplate jmsTemplate;

    public JmsConsumer(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    @Override
    public void onMessage(Message message) {
        Task task = null;
        try {
            task = (Task) ((ObjectMessage) message).getObject();
            //如果消息不合法或者消息已被消费或者消息的前置消息未能被正确消费均不可以被处理,否则可能造成数据不一致
            if (task == null || !canStartConsume(task.getTaskId())) return;
            logger.info("接收到消息, bizId:{}", task.getBizId());
            Task convertedTask = (Task) task.clone();
            if (convertedTask == null) {
                return;
            }
            List<AbstractFilter> filters = new ArrayList<>();
            if (iFilters != null && task.getOperation() == OperationEnum.INSERT) {
                filters = iFilters;
            } else if (uFilters != null && task.getOperation() == OperationEnum.UPDATE) {
                filters = uFilters;
            } else if (dFilters != null && task.getOperation() == OperationEnum.DELETE) {
                filters = dFilters;
            }

            boolean retry = false;
            for (AbstractFilter filter : filters) {
                try {
                    convertedTask = filter.doFilter(convertedTask);
                } catch (DiscardException ex) {
                    //收到该消息, 抛弃当前数据, 所有流程处理完毕
                    break;
                } catch (RetryException ex) {
                    //收到该消息, 需要将任务放入重试列表
                    ex.printStackTrace();
                    retry = true;
                    break;
                    //throw new JMSException("消息处理失败,需要重试。失败原因: " + ex.getMessage());
                }
            }
            //接收到消息处理完成后必须手动确认
            message.acknowledge();
            if (!retry) {
                this.afterProcess(task.getTaskId());
            }

        } catch (JMSException ex) {
            throw JmsUtils.convertJmsAccessException(ex);
        }
    }
}
