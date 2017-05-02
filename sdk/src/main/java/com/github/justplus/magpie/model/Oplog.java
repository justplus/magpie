package com.github.justplus.magpie.model;

import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhaoliang on 2017/3/30.
 */
public class Oplog implements Serializable {
    // 操作编号, oplog中每个操作都有个唯一的id
    private long opId;
    // 操作类型
    private OperationEnum operation;
    // 操作对象所属的命名空间
    private String namespace;
    // 操作对象的主键值
    private String bizId;
    // 对应于o字段, 新增时存储新增的整个字段, 更新和删除时存储操作
    private Map<String, Object> operateLog;

    /**
     * 构造函数: 解析oplog
     *
     * @param h
     * @param op
     * @param ns
     * @param o2
     * @param o
     */
    public Oplog(long h, String op, String ns, Map<String, Object> o2, Map<String, Object> o) {
        this.opId = h;
        switch (op) {
            case "i":
                this.operation = OperationEnum.INSERT;
                break;
            case "u":
                this.operation = OperationEnum.UPDATE;
                break;
            case "d":
                this.operation = OperationEnum.DELETE;
                break;
            default:
                this.operation = OperationEnum.UNKNOWN;
                break;
        }
        this.namespace = ns;
        if (o != null && o.containsKey("_id")) {
            Object id = o.get("_id");
            this.bizId = (id instanceof ObjectId) ? id.toString() : (String) id;
        } else if (o2 != null && o2.containsKey("_id")) {
            Object id = o2.get("_id");
            this.bizId = (id instanceof ObjectId) ? id.toString() : (String) id;
        }
        // 设置变更内容
        if (this.operation == OperationEnum.INSERT) {
            this.operateLog = o;
        } else if (this.operation == OperationEnum.UPDATE && o != null && o instanceof Map) {
            if (o.containsKey("$set")) {
                this.operateLog = (Map) o.get("$set");
            } else if (o.containsKey("$update")) {
                this.operateLog = (Map) o.get("$update");
            }
        }
    }

    /**
     * 将对象转换成map
     *
     * @return
     */
    public Map<String, Object> oplog2Map() {
        Map<String, Object> result = new HashMap<>();
        result.put("opId", this.opId);
        result.put("operation", this.operation);
        result.put("namespace", this.namespace);
        result.put("bizId", this.bizId);
        result.put("operateLog", this.operateLog);
        return result;
    }


    /**********************
     * getter and setter
     **********************/
    public long getOpId() {
        return opId;
    }

    public void setOpId(long opId) {
        this.opId = opId;
    }

    public OperationEnum getOperation() {
        return operation;
    }

    public void setOperation(OperationEnum operation) {
        this.operation = operation;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getBizId() {
        return bizId;
    }

    public void setBizId(String bizId) {
        this.bizId = bizId;
    }

    public Map<String, Object> getOperateLog() {
        return operateLog;
    }

    public void setOperateLog(Map<String, Object> operateLog) {
        this.operateLog = operateLog;
    }
}
