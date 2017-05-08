package com.github.justplus.magpie.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.justplus.magpie.model.OperationEnum;
import com.github.justplus.magpie.model.Rule;
import com.github.justplus.magpie.model.TableRule;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhaoliang on 2017/3/31.
 * 监听规则解析类
 */
public class RuleUtil {
    /**
     * 解析监听规则
     *
     * @param jsonRule 监听规则的json字符串
     * @return 返回监听规则列表
     */
    public static List<Rule> analyseMySQLRule(String jsonRule) {
        return analyseRule(jsonRule, "MySQL");
    }

    /**
     * 解析监听规则
     *
     * @param jsonRule 监听规则的json字符串
     * @return 返回监听规则列表
     */
    public static List<Rule> analyseMongoRule(String jsonRule) {
        return analyseRule(jsonRule, "MongoDB");
    }

    /**
     * 判断用户定义的监听规则中是否有对某个库的监听
     *
     * @param rules        监听规则
     * @param databaseName 表名
     * @return 返回是否监听某个库
     */
    public static boolean containDatabase(List<Rule> rules, String databaseName) {
        for (Rule r : rules) {
            if (r != null && StringUtils.isNotBlank(databaseName) && databaseName.equals(r.getDatabaseName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断用户定义的监听规则中是否有对某个表的监听
     *
     * @param rules     监听规则
     * @param database  库名
     * @param tableName 表名
     * @return 返回是否监听某张表
     */
    public static boolean containTable(List<Rule> rules, String database, String tableName) {
        for (Rule r : rules) {
            for (TableRule tr : r.getTableRules()) {
                if (database.equals(r.getDatabaseName()) && tableName.equals(tr.getTableName())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 判断用户定义的监听规则中是否有对库表的操作进行了监听
     *
     * @param rules     监听规则
     * @param database  库名
     * @param tableName 表名
     * @param operation 操作类型
     * @return 返回是否监听某种操作
     */
    public static boolean containOperation(List<Rule> rules, String database,
                                           String tableName, OperationEnum operation) {
        for (Rule r : rules) {
            for (TableRule tr : r.getTableRules()) {
                if (database.equals(r.getDatabaseName()) && tableName.equals(tr.getTableName())
                        && tr.getOperations().contains(operation)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 判断用户定义的监听规则中是否有对相关字段的监听
     *
     * @param rules     监听规则
     * @param database  库名
     * @param tableName 表名
     * @param field     字段
     * @return 返回是否监听某字段
     */
    public static boolean containField(List<Rule> rules, String database,
                                       String tableName, String field) {
        for (Rule r : rules) {
            for (TableRule tr : r.getTableRules()) {
                if (database.equals(r.getDatabaseName()) && tableName.equals(tr.getTableName()) && tr.getIncludeFileds().contains(field)) {
                    return true;
                }
                if (database.equals(r.getDatabaseName()) && tableName.equals(tr.getTableName()) && tr.getExcludeFields().contains(field)) {
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * 获取监听字段, +字符后面跟随的字段,如+abc-de中abc为监听的字段
     *
     * @param fields 字段
     * @return 返回监听字段
     */
    public static List<String> listIncludeFields(String fields) {
        String regex = "\\-(.*?)[\\+|\\s]";
        return getRegFields(fields, regex);
    }

    /**
     * 获取不监听字段 +字符后面跟随的字段,如+abc-de中de为不监听的字段
     *
     * @param fields 字段
     * @return 返回不监听字段
     */
    public static List<String> listExcludeFields(String fields) {
        String regex = "\\+(.*?)[\\-|\\s]";
        return getRegFields(fields, regex);
    }

    /**
     * 获取业务id, 一般是db中的主键, 可以为字符串,如uuid, 也可能是自增long/int类型
     *
     * @param id
     * @return 返回字符串格式的bizId
     */
    public static String getBizId(Object id) {
        if (id instanceof String) {
            return (String) id;
        } else if (id instanceof Long) {
            return String.valueOf(id);
        } else if (id instanceof Integer) {
            return String.valueOf(id);
        }
        return String.valueOf(id);
    }

    /**
     * 获取监听类型
     *
     * @param opes 用户定义的监听字符串,如 uid
     * @return 返回监听类型
     */
    public static List<OperationEnum> listOperations(String opes) {
        List<OperationEnum> operations = new ArrayList<>();
        for (int i = 0; i < opes.length(); i++) {
            char op = opes.charAt(i);
            if (op == 'u') {
                operations.add(OperationEnum.UPDATE);
            } else if (op == 'i') {
                operations.add(OperationEnum.INSERT);
            } else if (op == 'd') {
                operations.add(OperationEnum.DELETE);
            }
        }
        return operations;
    }

    private static List<String> getRegFields(String fields, String regex) {
        fields = fields.replaceAll("\\s", "") + " ";
        List<String> result = new ArrayList<>();

        Matcher matcher = Pattern.compile(regex).matcher(fields);
        while (matcher.find()) {
            String[] fieldArray = matcher.group(1).split(",");
            for (String field : fieldArray) {
                if (!result.contains(field)) {
                    result.add(field);
                }
            }
        }
        return result;
    }


    private static List<Rule> analyseRule(String jsonRule, String type) {
        List<Rule> rules = new ArrayList<>();
        JSONArray array = JSON.parseArray(jsonRule);
        for (int i = 0; i < array.size(); i++) {
            List<TableRule> tableRules = new ArrayList<>();
            JSONObject obj = JSON.parseObject(array.get(i) + "");
            String database = obj.getString("database");
            JSONArray tables = obj.getJSONArray("MySQL".equals(type) ? "tables" : "collections");
            for (int j = 0; j < tables.size(); j++) {
                JSONObject tableObj = JSON.parseObject(tables.get(j) + "");
                String table = tableObj.getString("MySQL".equals(type) ? "table" : "collection");
                String fields = tableObj.getString("fields");
                String operation = tableObj.getString("operation");
                tableRules.add(new TableRule(table, fields, operation));
            }
            rules.add(new Rule(database, tableRules));
        }
        return rules;
    }
}


