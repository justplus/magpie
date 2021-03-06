package com.github.justplus.magpie;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ro.fortsoft.pf4j.*;
import ro.fortsoft.pf4j.spring.ExtensionsInjector;
import ro.fortsoft.pf4j.spring.SpringExtensionFactory;


/**
 * Created by zhaoliang on 2017/4/12.
 */
@Configuration
public class SpringConfiguration {

    @Bean
    public PluginManager pluginManager() {
        PluginManager pluginManager = new DefaultPluginManager() {

            @Override
            protected ExtensionFactory createExtensionFactory() {
                return new SpringExtensionFactory(this);
            }

        };
        return pluginManager;
    }

    @Bean
    public static ExtensionsInjector extensionsInjector() {
        return new ExtensionsInjector();
    }

}