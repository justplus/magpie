package com.github.justplus.magpie.service.producer.impl;

import com.github.justplus.magpie.model.OperationEnum;
import com.github.justplus.magpie.model.Task;
import com.github.justplus.magpie.service.producer.AbstractProducer;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhaoliang on 2017/3/28.
 * MySQL存量数据生产者
 */
public class MySQLProducer extends AbstractProducer {
    private JdbcTemplate jdbcTemplate;

    //查询语句
    private String querySQL;
    //每个线程每次拉取的数量
    private int limit = 100;
    //时间戳
    protected long nowTimestamp;

    private AtomicLong pos;

    public MySQLProducer(JdbcTemplate jdbcTemplate, String querySQL, long nowTimestamp, int limit) {
        this.jdbcTemplate = jdbcTemplate;
        this.querySQL = querySQL;
        this.limit = limit;
        this.nowTimestamp = nowTimestamp;
        this.pos = new AtomicLong(-1 * this.limit);
    }

    public MySQLProducer(JdbcTemplate jdbcTemplate, String querySQL, long nowTimestamp) {
        this(jdbcTemplate, querySQL, nowTimestamp, 100);
    }

    public MySQLProducer(JdbcTemplate jdbcTemplate, String querySQL) {
        this(jdbcTemplate, querySQL, new Date().getTime(), 100);
    }

    public MySQLProducer(JdbcTemplate jdbcTemplate, String querySQL, int limit) {
        this(jdbcTemplate, querySQL, new Date().getTime(), 100);
        this.limit = limit;
    }

    @Override
    public List<Task> produce() {
        String tmpQuerySQL = this.querySQL;
        if (limit > 0) {
            long p = pos.addAndGet(limit);
            tmpQuerySQL = String.format("%s limit %d,%d", tmpQuerySQL, p, limit);
        }
        tmpQuerySQL = tmpQuerySQL.replace("[now_dt]", String.format("from_unixtime(%s)", String.valueOf(this.nowTimestamp)));
        logger.info(tmpQuerySQL);

        List<Task> tasks = new ArrayList<Task>();
        List<Map<String, Object>> queryResult = jdbcTemplate.queryForList(tmpQuerySQL);
        for (Map<String, Object> data : queryResult) {
            String bizId;
            if (data.containsKey("id")) {
                bizId = String.valueOf(data.get("id"));
            } else {
                bizId = UUID.randomUUID().toString();
            }
            bizId = String.format("%s_%s", producerName, bizId);
            Task task = new Task(bizId, OperationEnum.INSERT, data, this.producerName);
            tasks.add(task);
        }
        return tasks;
    }

    /**********************
     * getter and setter
     **********************/
    public void setQuerySQL(String querySQL) {
        this.querySQL = querySQL;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void setNowTimestamp(long nowTimestamp) {
        this.nowTimestamp = nowTimestamp;
    }
}
