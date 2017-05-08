package com.github.justplus.magpie.model;

import com.github.justplus.magpie.utils.RuleUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhaoliang on 2017/3/31.
 */
public class TableRule {
    private String tableName;
    private List<String> includeFileds;
    private List<String> excludeFields;
    private List<OperationEnum> operations;

    public TableRule(String tableName, String fields, String operations) {
        this.tableName = tableName;
        //处理fileds字段, 支持-/+/*, 不支持混合使用
        this.includeFileds = RuleUtil.listIncludeFields(fields);
        this.excludeFields = RuleUtil.listExcludeFields(fields);
        this.operations = RuleUtil.listOperations(operations);
    }

    public List<String> listSimpleOperations() {
        List<String> result = new ArrayList<>();
        for (OperationEnum operation : operations) {
            if (operation == OperationEnum.INSERT) {
                result.add("i");
            } else if (operation == OperationEnum.UPDATE) {
                result.add("u");
            } else if (operation == OperationEnum.DELETE) {
                result.add("d");
            }
        }
        return result;
    }

    /**********************
     * getter and setter
     **********************/
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getIncludeFileds() {
        return includeFileds;
    }

    public void setIncludeFileds(List<String> includeFileds) {
        this.includeFileds = includeFileds;
    }

    public List<String> getExcludeFields() {
        return excludeFields;
    }

    public void setExcludeFields(List<String> excludeFields) {
        this.excludeFields = excludeFields;
    }

    public List<OperationEnum> getOperations() {
        return operations;
    }

    public void setOperations(List<OperationEnum> operations) {
        this.operations = operations;
    }
}
