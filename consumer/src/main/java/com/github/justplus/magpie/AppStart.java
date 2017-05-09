package com.github.justplus.magpie;

import com.github.justplus.magpie.api.IConsumer;
import com.github.justplus.magpie.model.MagPlugin;
import com.github.justplus.magpie.utils.ParameterCallback;
import com.github.justplus.magpie.utils.PluginUtil;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import ro.fortsoft.pf4j.PluginDescriptor;
import ro.fortsoft.pf4j.PluginManager;
import ro.fortsoft.pf4j.PluginWrapper;

import java.util.Arrays;
import java.util.List;

/**
 * Created by zhaoliang on 2017/4/13.
 */
public class AppStart {
    public static void main(String[] args) throws Exception {
        ApplicationContext applicationContext = new AnnotationConfigApplicationContext(SpringConfiguration.class);

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


        PluginManager pluginManager = (PluginManager) applicationContext.getBean("pluginManager");
        pluginManager.loadPlugins();
        List<PluginWrapper> plugins = pluginManager.getPlugins();
        for (PluginWrapper wrapper : plugins) {
            //将插件注册到zk
            PluginDescriptor description = wrapper.getDescriptor();
            MagPlugin magPlugin = new MagPlugin(description.getPluginId(), description.getVersion().toString(),
                    Arrays.asList(description.getPluginDescription().split(",")));

            pluginUtil.registerPlugin(magPlugin);
        }

        //保持线程状态
        Thread.sleep(Long.MAX_VALUE);
    }
}
