package com.github.justplus.magpie;

import com.github.justplus.magpie.api.IConsumer;
import com.github.justplus.magpie.utils.ParameterCallback;
import com.github.justplus.magpie.utils.PluginUtil;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.util.List;

/**
 * Created by zhaoliang on 2017/4/13.
 */
public class AppStart {
    public static void main(String[] args) throws Exception {
        ApplicationContext applicationContext = new AnnotationConfigApplicationContext(SpringConfiguration.class);
        Consumers consumers = applicationContext.getBean(Consumers.class);
        consumers.start();

        ApplicationContext xmlApplicationContext = new FileSystemXmlApplicationContext(new String[]{"classpath:bean.xml"}, true);
        final PluginUtil pluginUtil = xmlApplicationContext.getBean(PluginUtil.class);
        pluginUtil.setLoadCallback(new ParameterCallback() {
            @Override
            public Integer call() throws Exception {
                List<IConsumer> consumers = (List<IConsumer>) this.getParameter();
                if (consumers != null) {
                    System.out.println("插件开始工作");
                    for (IConsumer consumer : consumers) {
                        consumer.consume();
                    }
                }
                return 1;
            }
        });

        pluginUtil.setUnloadCallback(new ParameterCallback() {
            @Override
            public Integer call() throws Exception {
                return 1;
            }
        });
        pluginUtil.startListening(IConsumer.class);

        //保持线程状态
        Thread.sleep(Long.MAX_VALUE);
    }
}
