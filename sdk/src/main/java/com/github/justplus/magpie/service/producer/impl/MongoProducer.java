package com.github.justplus.magpie.service.producer.impl;

import com.alibaba.fastjson.JSONObject;
import com.github.justplus.magpie.model.OperationEnum;
import com.github.justplus.magpie.model.Task;
import com.github.justplus.magpie.service.producer.AbstractProducer;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaoliang on 2017/3/29.
 * MongoDB存量数据生产者
 */
public class MongoProducer extends AbstractProducer {
    private MongoTemplate mongoTemplate;
    //查询条件
    private String queryCmd;
    //集合名称
    private String collectionName;
    //字段列表
    private String fields;
    //业务id名称
    private String bizFieldName;
    //每个线程每次最多拉取的数量
    private int limit = 100;
    //时间戳
    private long nowTimestamp;

    private AtomicInteger pos;

    public MongoProducer(MongoTemplate mongoTemplate, String queryCmd, String collectionName, long nowTimestamp, int limit) {
        this.mongoTemplate = mongoTemplate;
        this.queryCmd = queryCmd;
        this.collectionName = collectionName;
        this.limit = limit;
        this.nowTimestamp = nowTimestamp;
        this.pos = new AtomicInteger(-1 * this.limit);
    }

    public MongoProducer(MongoTemplate mongoTemplate, String queryCmd, String collectionName, long nowTimestamp) {
        this(mongoTemplate, queryCmd, collectionName, nowTimestamp, 100);
    }

    public MongoProducer(MongoTemplate mongoTemplate, String queryCmd, String collectionName) {
        this(mongoTemplate, queryCmd, collectionName, new Date().getTime(), 100);
    }

    @Override
    public List<Task> produce() {
        String tmpQuerySQL = this.queryCmd;
        tmpQuerySQL = tmpQuerySQL.replace("[now_dt]", String.valueOf(this.nowTimestamp));
        int skip = pos.get();
        if (limit > 0) {
            skip = pos.addAndGet(limit);
        }
        List<Task> tasks = new ArrayList<>();
        Task task = null;
        DB db = mongoTemplate.getDb();
        DBCollection collection = db.getCollection(this.collectionName);
        if (collection == null) return tasks;
        DBCursor cursor = null;
        BasicDBObject sortQuery = (BasicDBObject) JSON.parse("{\"_id\":1}");
        if (StringUtils.isNotBlank(tmpQuerySQL)) {
            BasicDBObject searchQuery = (BasicDBObject) JSON.parse(tmpQuerySQL);
            if (StringUtils.isBlank(fields)) {
                cursor = collection.find(searchQuery).skip(skip).limit(limit).sort(sortQuery);
            } else {
                BasicDBObject queryFields = new BasicDBObject();
                for (String field : fields.split(",")) {
                    queryFields.put(field, 1);
                }
                cursor = collection.find(searchQuery, queryFields).skip(skip).limit(limit).sort(sortQuery);
            }
        } else {
            if (StringUtils.isBlank(fields)) {
                cursor = collection.find().skip(skip).limit(limit).sort(sortQuery);
            } else {
                BasicDBObject queryFields = new BasicDBObject();
                for (String field : fields.split(",")) {
                    queryFields.put(field, 1);
                }
                cursor = collection.find(new BasicDBObject(), queryFields).skip(skip).limit(limit).sort(sortQuery);
            }
        }

        while (cursor != null && cursor.hasNext()) {
            DBObject obj = cursor.next();
            JSONObject result = JSONObject.parseObject(obj.toString());
            //将JSONObject转换为Map
            Map<String, Object> mapper = new HashMap<>();
            for (String key : result.keySet()) {
                Object value = result.get(key);
                mapper.put(key, value);
            }
            if (mapper.containsKey("_id")) {
                mapper.remove("_id");
            }

            String bizId = result.containsKey(this.bizFieldName) ? (String) result.get(this.bizFieldName) :
                    UUID.randomUUID().toString();
            bizId = String.format("%s_%s", producerName, bizId);

            if (result != null && !result.isEmpty()) {
                task = new Task(bizId, OperationEnum.INSERT, mapper, this.producerName);
                tasks.add(task);
            }
        }
        return tasks;
    }

    /**********************
     * getter and setter
     **********************/
    public void setQueryCmd(String queryCmd) {
        this.queryCmd = queryCmd;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public void setFields(String fields) {
        this.fields = fields;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setMongoTemplate(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public void setNowTimestamp(long nowTimestamp) {
        this.nowTimestamp = nowTimestamp;
    }

    public void setBizFieldName(String bizFieldName) {
        this.bizFieldName = bizFieldName;
    }
}
