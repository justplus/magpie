package com.github.justplus.magpie.service.producer.impl;

import com.github.justplus.magpie.model.Oplog;
import com.github.justplus.magpie.model.Rule;
import com.github.justplus.magpie.model.TableRule;
import com.github.justplus.magpie.model.Task;
import com.github.justplus.magpie.service.producer.AbstractProducer;
import com.github.justplus.magpie.service.record.IRecord;
import com.github.justplus.magpie.util.RuleUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.bson.types.BSONTimestamp;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaoliang on 2017/3/29.
 * MongoDB增量数据生产者
 * 思路: MongoDB的oplog是存储在一个database=local, collection=oplog.rs的集合中, 根据时间戳定时读取更新日志,
 * 然后扔到生产者队列中
 */
public class MongoIncProducer extends AbstractProducer {
    private MongoTemplate localMongoTemplate;
    private IRecord record;
    //存储oplog的collection名称
    private String localCollectionName;
    //监听规则
    private String monitorRule;

    //时间戳, 单位ms 该时间戳以后的数据进行监听
    private long position;

    private List<Rule> rules;


    public MongoIncProducer(MongoTemplate localMongoTemplate, IRecord record, String localCollectionName,
                            String monitorRule) {
        this.localMongoTemplate = localMongoTemplate;
        this.record = record;
        this.localCollectionName = localCollectionName;
        this.monitorRule = monitorRule;
        this.rules = RuleUtil.analyseMongoRule(this.monitorRule);
    }

    @Override
    public List<Task> produce() {
        BSONTimestamp currentTimeStamp = new BSONTimestamp((int) (this.position / 1000),
                new AtomicInteger((new java.util.Random()).nextInt()).getAndIncrement());
        //加载记录的时间戳以后的变化
        DBCursor cursor = this.getOplogCursor(currentTimeStamp);
        while (cursor.hasNext()) {
            DBObject obj = cursor.next();
            long h = (Long) obj.get("h");
            String op = (String) obj.get("op");
            String ns = (String) obj.get("ns");
            Map<String, Object> o2 = (Map<String, Object>) obj.get("o2");
            Map<String, Object> o = (Map<String, Object>) obj.get("o");
            Oplog oplog = new Oplog(h, op, ns, o2, o);
            Task task = new Task(oplog.getBizId(), oplog.getOperation(), oplog.oplog2Map(), this.producerName);
            this.queue.push(task);
            //更新时间戳
            record.setLatestPos(this.producerName, String.valueOf(new Date().getTime()));
            logger.info("====={}", oplog.getBizId());
        }
        return null;
    }

    /********************** private methods **********************/
    /**
     * 根据时间戳获取游标信息
     *
     * @param startTimestamp 时间戳
     * @return 游标
     */
    private DBCursor getOplogCursor(BSONTimestamp startTimestamp) {
        final Query query = new Query();
        Criteria criteria = new Criteria();
        Criteria[] dbCriterias = new Criteria[rules.size()];
        for (int i = 0; i < rules.size(); i++) {
            Rule rule = rules.get(i);
            Criteria cr = new Criteria();
            List<TableRule> collectionRules = rule.getTableRules();

            Criteria[] collectionCriterias = new Criteria[collectionRules.size()];
            for (int j = 0; j < collectionRules.size(); j++) {
                TableRule collectionRule = collectionRules.get(j);
                Criteria tcr = new Criteria();
                tcr.andOperator(Criteria.where("ns").is(rule.getDatabaseName() + "." + collectionRule.getTableName()),
                        Criteria.where("op").in(collectionRule.listSimpleOperations()));
                collectionCriterias[j] = tcr;
            }
            cr.orOperator(collectionCriterias);
            dbCriterias[i] = cr;
        }
        criteria.orOperator(dbCriterias);
        query.addCriteria(criteria);
        query.addCriteria(Criteria.where("ts").gte(startTimestamp));
        return this.localMongoTemplate.getCollection(this.localCollectionName).
                find(query.getQueryObject()).addOption(Bytes.QUERYOPTION_TAILABLE).
                addOption(Bytes.QUERYOPTION_AWAITDATA).addOption(Bytes.QUERYOPTION_NOTIMEOUT).
                sort(new BasicDBObject("$natural", 1));
    }

    /**********************
     * getter and setter
     **********************/
    public void setLocalCollectionName(String localCollectionName) {
        this.localCollectionName = localCollectionName;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public void setMonitorRule(String monitorRule) {
        this.monitorRule = monitorRule;
    }

    public void setRecord(IRecord record) {
        this.record = record;
    }

    public void setLocalMongoTemplate(MongoTemplate localMongoTemplate) {
        this.localMongoTemplate = localMongoTemplate;
    }
}
