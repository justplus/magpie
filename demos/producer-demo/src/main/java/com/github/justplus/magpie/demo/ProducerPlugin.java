package com.github.justplus.magpie.demo;

import com.github.justplus.magpie.api.IProducer;
import com.github.justplus.magpie.model.MagPlugin;
import com.github.justplus.magpie.service.producer.impl.MySQLProducer;
import com.github.justplus.magpie.utils.PluginUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import ro.fortsoft.pf4j.Extension;
import ro.fortsoft.pf4j.PluginDescriptor;
import ro.fortsoft.pf4j.PluginWrapper;
import ro.fortsoft.pf4j.spring.SpringPlugin;

import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

/**
 * Created by zhaoliang on 2017/4/13.
 */
public class ProducerPlugin extends SpringPlugin {
    private final static Logger logger = LoggerFactory.getLogger(ProducerPlugin.class);

    public ProducerPlugin(PluginWrapper wrapper) {
        super(wrapper);
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        super.stop();
        PluginDescriptor description = this.getWrapper().getDescriptor();
    }

    @Override
    protected ApplicationContext createApplicationContext() {
        final AbstractApplicationContext applicationContext = new FileSystemXmlApplicationContext(
                new String[]{"classpath*:spring/producer.xml"}, false);
        applicationContext.setClassLoader(getWrapper().getPluginClassLoader());
        applicationContext.refresh();
        return applicationContext;
    }


    @Extension
    public static class DemoProducer implements IProducer {
        @Autowired
        MySQLProducer demoProducer;

        @Override
        public void produce() {
            final long nowTimestamp = new Date().getTime();
            final int THREAD_NUM = 20;
            CountDownLatch latch = new CountDownLatch(THREAD_NUM);

            demoProducer.setNowTimestamp(nowTimestamp / 1000);
            for (int i = 0; i < THREAD_NUM; i++) {
                ProducerThread producer = new ProducerThread(latch, demoProducer);
                producer.start();
            }
            try {
                latch.await();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
            logger.info("********存量数据导入完毕*******");
        }
    }
}
