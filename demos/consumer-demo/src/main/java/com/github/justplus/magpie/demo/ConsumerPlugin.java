package com.github.justplus.magpie.demo;

import com.github.justplus.magpie.api.IConsumer;
import com.github.justplus.magpie.service.consumer.JmsConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import ro.fortsoft.pf4j.Extension;
import ro.fortsoft.pf4j.PluginWrapper;
import ro.fortsoft.pf4j.spring.SpringPlugin;

/**
 * Created by zhaoliang on 2017/5/8.
 */
public class ConsumerPlugin extends SpringPlugin {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerPlugin.class);

    public ConsumerPlugin(PluginWrapper wrapper) {
        super(wrapper);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    protected ApplicationContext createApplicationContext() {
        final AbstractApplicationContext applicationContext = new FileSystemXmlApplicationContext(
                new String[]{"classpath*:spring/consumer.xml"}, false);
        applicationContext.setClassLoader(getWrapper().getPluginClassLoader());
        applicationContext.refresh();
        return applicationContext;
    }

    @Extension
    public static class DemoConsumer implements IConsumer {
        @Autowired
        JmsConsumer demoConsumer;

        @Override
        public void consume() {

        }
    }
}
