package com.github.justplus.magpie;

import com.github.justplus.magpie.api.IProducer;
import com.github.justplus.magpie.util.ParameterCallback;
import com.github.justplus.magpie.util.PluginUtil;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.util.List;


/**
 * Created by zhaoliang on 2017/4/12.
 */
public class AppStart {
    public static void main(String[] args) throws Exception {
        ApplicationContext applicationContext = new AnnotationConfigApplicationContext(SpringConfiguration.class);
        Producers producers = applicationContext.getBean(Producers.class);
        producers.start();

        ApplicationContext xmlApplicationContext = new FileSystemXmlApplicationContext(new String[]{"classpath:bean.xml"}, true);
        final PluginUtil pluginUtil = xmlApplicationContext.getBean(PluginUtil.class);
        pluginUtil.setLoadCallback(new ParameterCallback() {
            @Override
            public Integer call() throws Exception {
                List<IProducer> producers = (List<IProducer>) this.getParameter();
                if (producers != null) {
                    System.out.println("插件开始工作");
                    for (IProducer producer : producers) {
                        producer.produce();
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
        pluginUtil.startListening(IProducer.class);

        //保持线程状态
        Thread.sleep(Long.MAX_VALUE);
    }
}
