package com.github.justplus.magpie.service.queue;

import com.github.justplus.magpie.model.Task;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.util.List;

/**
 * Created by zhaoliang on 2017/5/2.
 */
public class JMSQueue implements IQueue {
    private JmsTemplate jmsTemplate;

    public JMSQueue(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    @Override
    public void push(final Task task) {
        if (task == null) return;
        jmsTemplate.send(task.getProducerName(), new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                return session.createObjectMessage(task);
            }
        });
    }

    @Override
    public void bathPush(List<Task> tasks) {
        for (final Task task : tasks) {
            jmsTemplate.send(task.getProducerName(), new MessageCreator() {
                @Override
                public Message createMessage(Session session) throws JMSException {
                    return session.createObjectMessage(task);
                }
            });
        }
    }

    /**
     * jms使用的是监听方式, 不需要pop任务
     *
     * @return
     */
    @Override
    public Task pop() {
        return null;
    }

    /**
     * jms使用的是监听方式, 不需要pop任务
     *
     * @return
     */
    @Override
    public List<Task> bathPop(int limit) {
        return null;
    }
}
