package com.github.justplus.magpie.utils;

import com.github.justplus.magpie.model.MagPlugin;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.DefaultPluginManager;
import ro.fortsoft.pf4j.ExtensionFactory;
import ro.fortsoft.pf4j.PluginState;
import ro.fortsoft.pf4j.spring.SpringExtensionFactory;

import java.io.File;
import java.util.List;

/**
 * Created by zhaoliang on 2017/4/16.
 */
public class PluginUtil {
    private static final Logger logger = LoggerFactory.getLogger(PluginUtil.class);

    private String ZKServerUrl;
    private int ZKSessionTimeout;
    private int ZKConnectionTimeout;
    private ParameterCallback loadCallback;
    private ParameterCallback unloadCallback;
    //区分是消费者插件还是生产者插件
    private String type;

    private final String ROOT_NODE = "/magpie/plugins";
    private final String PRODUCER_NODE = "producers";
    private final String CONSUMER_NODE = "consumers";
    private ZkClient zkClient;

    private DefaultPluginManager pluginManager = new DefaultPluginManager() {
        @Override
        protected ExtensionFactory createExtensionFactory() {
            return new SpringExtensionFactory(this);
        }
    };

    public PluginUtil(String ZKServerUrl, int ZKSessionTimeout, int ZKConnectionTimeout, String type,
                      ParameterCallback loadCallback, ParameterCallback unloadCallback) {
        this.ZKServerUrl = ZKServerUrl;
        this.ZKConnectionTimeout = ZKConnectionTimeout;
        this.ZKSessionTimeout = ZKSessionTimeout;
        this.type = type;
        this.loadCallback = loadCallback;
        this.unloadCallback = unloadCallback;
        this.init();
    }

    public PluginUtil(String ZKServerUrl, int ZKSessionTimeout, int ZKConnectionTimeout, String type) {
        this.ZKServerUrl = ZKServerUrl;
        this.ZKConnectionTimeout = ZKConnectionTimeout;
        this.ZKSessionTimeout = ZKSessionTimeout;
        this.type = type;
        this.init();
    }

    public PluginUtil(String ZKServerUrl, String type, ParameterCallback loadCallback,
                      ParameterCallback unloadCallback) {
        this(ZKServerUrl, 30000, 5000, type, loadCallback, unloadCallback);
    }

    private void init() {
        if (zkClient == null) {
            zkClient = new ZkClient(this.ZKServerUrl, this.ZKSessionTimeout, this.ZKConnectionTimeout);
        }
        zkClient.createPersistent(ROOT_NODE + "/" + PRODUCER_NODE, true);
        zkClient.createPersistent(ROOT_NODE + "/" + CONSUMER_NODE, true);
    }

    /**
     * 将插件信息注册到zk
     *
     * @param magPlugin 插件详情
     */
    public void registerPlugin(MagPlugin magPlugin) {
        if (this.zkClient == null) return;
        String nodePath = String.format("%s/%ss/%s", ROOT_NODE, this.type, magPlugin.getPluginId());
        if (!this.zkClient.exists(nodePath)) {
            this.zkClient.createPersistent(nodePath);
            this.zkClient.writeData(nodePath, magPlugin);
        }
    }

    public void updatePluginState(String pluginId, int data) {
        if (this.zkClient == null) return;
        String nodePath = String.format("%s/%ss/%s", ROOT_NODE, this.type, pluginId);
        if (this.zkClient.exists(nodePath)) {
            MagPlugin magPlugin = zkClient.readData(nodePath, true);
            if (magPlugin != null) {
                magPlugin.setState(data);
                zkClient.writeData(nodePath, magPlugin);
            }
        }
    }

    public void updatePluginProgress(String pluginId, String progress) {
        if (this.zkClient == null) return;
        String nodePath = String.format("%s/%ss/%s", ROOT_NODE, this.type, pluginId);
        if (this.zkClient.exists(nodePath)) {
            MagPlugin magPlugin = zkClient.readData(nodePath, true);
            if (magPlugin != null) {
                magPlugin.setProgress(progress);
                zkClient.writeData(nodePath, magPlugin);
            }
        }
    }

    public <T> void startListening(final Class<T> clazz) throws Exception {
        if (this.zkClient == null) return;
        zkClient.subscribeChildChanges(String.format("%s/%ss", ROOT_NODE, this.type), new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
                for (String subNode : currentChildren) {
                    String fullPath = String.format("%s/%ss/%s", ROOT_NODE, type, subNode);
                    MagPlugin magPlugin = zkClient.readData(fullPath, true);
                    if (magPlugin == null) return;
                    if (magPlugin.getState() == 0) {
                        //loadCallback.setParameter(subNode);
                        String pluginId = pluginManager.loadPlugin(new File(String.format("plugins/%s", subNode)));
                        PluginState pluginState = pluginManager.startPlugin(pluginId);
                        if (pluginState == PluginState.STARTED) {
                            logger.info("插件{}被成功加载...", subNode);
                            List<T> extensions = pluginManager.getExtensions(clazz, pluginId);
                            loadCallback.setParameter(extensions);
                            loadCallback.call();
                        }
                        //更新当前节点的运行状态
                        updatePluginState(pluginId, 1);
                    } else if (magPlugin.getState() == 2) {
                        unloadCallback.setParameter(subNode);
                        unloadCallback.call();
                        String pluginId = pluginManager.loadPlugin(new File(String.format("plugins/%s", subNode)));
                        PluginState pluginState = pluginManager.stopPlugin(pluginId);
                        if (pluginState == PluginState.STOPPED) {
                            pluginManager.unloadPlugin(pluginId);
                            logger.info("插件{}被成功卸载", fullPath);
                        }
                    }
                }
            }
        });
    }

    public void setZKServerUrl(String ZKServerUrl) {
        this.ZKServerUrl = ZKServerUrl;
    }

    public void setZKSessionTimeout(int ZKSessionTimeout) {
        this.ZKSessionTimeout = ZKSessionTimeout;
    }

    public void setZKConnectionTimeout(int ZKConnectionTimeout) {
        this.ZKConnectionTimeout = ZKConnectionTimeout;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setLoadCallback(ParameterCallback loadCallback) {
        this.loadCallback = loadCallback;
    }

    public void setUnloadCallback(ParameterCallback unloadCallback) {
        this.unloadCallback = unloadCallback;
    }
}
