package com.github.justplus.magpie.model;

import java.util.List;

/**
 * Created by zhaoliang on 2017/3/31.
 */
public class Rule {
    // MySQL、MongoDB数据库的数据库名称
    private String databaseName;
    // MySQL数据库的table监听规则 或者 MongoDB数据库的collection监听规则
    private List<TableRule> tableRules;

    public Rule(String databaseName, List<TableRule> tableRules) {
        this.databaseName = databaseName;
        this.tableRules = tableRules;
    }

    /**********************
     * getter and setter
     **********************/
    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public List<TableRule> getTableRules() {
        return tableRules;
    }

    public void setTableRules(List<TableRule> tableRules) {
        this.tableRules = tableRules;
    }
}

