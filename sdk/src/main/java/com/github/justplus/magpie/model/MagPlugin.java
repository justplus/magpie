package com.github.justplus.magpie.model;

import java.io.Serializable;
import java.util.List;

/**
 * Created by zhaoliang on 2017/5/9.
 */
public class MagPlugin implements Serializable {
    /**
     * 插件id
     */
    private String pluginId;
    /**
     * 版本
     */
    private String version;
    /**
     * 插件运行状态, 0:未运行 1:正在运行 2:已完成
     */
    private int state;
    /**
     * 依赖插件列表,存储id
     */
    private List<String> dependency;
    /**
     * 插件运行进度
     */
    private String progress;

    public MagPlugin(String pluginId, String version, List<String> dependency) {
        this.pluginId = pluginId;
        this.version = version;
        this.dependency = dependency;
        this.state = 1;
        this.progress = "";
    }

    /**********************
     * getter and setter
     **********************/
    public String getPluginId() {
        return pluginId;
    }

    public void setPluginId(String pluginId) {
        this.pluginId = pluginId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public List<String> getDependency() {
        return dependency;
    }

    public void setDependency(List<String> dependency) {
        this.dependency = dependency;
    }

    public String getProgress() {
        return progress;
    }

    public void setProgress(String progress) {
        this.progress = progress;
    }
}
