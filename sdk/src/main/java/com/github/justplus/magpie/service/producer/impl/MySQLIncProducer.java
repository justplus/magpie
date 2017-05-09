package com.github.justplus.magpie.service.producer.impl;

import com.github.justplus.magpie.model.OperationEnum;
import com.github.justplus.magpie.model.Rule;
import com.github.justplus.magpie.model.Task;
import com.github.justplus.magpie.service.producer.AbstractProducer;
import com.github.justplus.magpie.service.record.IRecord;
import com.github.justplus.magpie.utils.AutoOpenReplicator;
import com.github.justplus.magpie.utils.JdbcUtil;
import com.github.justplus.magpie.utils.RuleUtil;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhaoliang on 2017/3/28.
 * MySQL增量数据生产者
 */
public class MySQLIncProducer extends AbstractProducer {
    private JdbcTemplate jdbcTemplate;
    private AutoOpenReplicator openReplicator;
    private IRecord record;
    //binlog的position, 从这个position开始进行监听
    private long position;
    //配置监听规则, 包含监听的数据库、表、字段以及增删改事件
    private String monitorRule;

    private List<Rule> rules;
    private String database;
    private long tableId;
    private String tableName;

    /**
     * 构造函数
     *
     * @param jdbcTemplate   JdbcTemplate实例
     * @param openReplicator AutoOpenReplicator实例
     * @param record         IRecord实例
     * @param position       导入位置
     * @param monitorRule    监听规则
     */
    public MySQLIncProducer(JdbcTemplate jdbcTemplate, AutoOpenReplicator openReplicator, IRecord record,
                            long position, String monitorRule) {
        this.jdbcTemplate = jdbcTemplate;
        this.openReplicator = openReplicator;
        this.record = record;
        this.position = position;
        this.monitorRule = monitorRule;
        this.init();
    }

    public MySQLIncProducer(JdbcTemplate jdbcTemplate, AutoOpenReplicator openReplicator, IRecord record,
                            String monitorRule) {
        this(jdbcTemplate, openReplicator, record, 0, monitorRule);
    }

    private void init() {
        rules = RuleUtil.analyseMySQLRule(monitorRule);

        if (this.position <= 0) {
            this.position = this.getBinlogPos();
        }
        openReplicator.setBinlogPosition(this.position);
        openReplicator.setBinlogEventListener(new BinlogEventListener() {
            @Override
            public void onEvents(BinlogEventV4 event) {
                Class<?> eventClass = event.getClass();
                if (eventClass == QueryEvent.class) {
                    QueryEvent actualEvent = (QueryEvent) event;
                    database = actualEvent.getDatabaseName().toString();
                    return;
                }

                //数据库规则
                if (RuleUtil.containDatabase(rules, database)) {
                    if (eventClass == TableMapEvent.class) {
                        TableMapEvent actualEvent = (TableMapEvent) event;
                        tableName = actualEvent.getTableName().toString();
                        //数据表规则
                        if (RuleUtil.containTable(rules, database, tableName)) {
                            tableId = actualEvent.getTableId();
                        } else {
                            tableId = 0;
                        }
                        logger.info("事件数据表ID：{}， 事件数据库表名称：{}", tableId, tableName);
                        return;
                    }

                    if (eventClass == WriteRowsEventV2.class || eventClass == UpdateRowsEventV2.class
                            || eventClass == WriteRowsEvent.class || eventClass == UpdateRowsEvent.class
                            || eventClass == DeleteRowsEvent.class || eventClass == DeleteRowsEventV2.class) {
                        AbstractRowEvent rowEvent = (AbstractRowEvent) event;
                        //只监控指定的表
                        if (tableId == 0 || (tableId > 0 && rowEvent.getTableId() != tableId)) {
                            return;
                        }
                        //只监听指定的操作类型
                        OperationEnum operation = OperationEnum.UNKNOWN;
                        if (eventClass == WriteRowsEvent.class || eventClass == WriteRowsEventV2.class) {
                            operation = OperationEnum.INSERT;
                        } else if (eventClass == UpdateRowsEvent.class || eventClass == UpdateRowsEventV2.class) {
                            operation = OperationEnum.UPDATE;
                        }
                        if (eventClass == DeleteRowsEvent.class || eventClass == DeleteRowsEventV2.class) {
                            operation = OperationEnum.DELETE;
                        }
                        if (!RuleUtil.containOperation(rules, database, tableName, operation)) {
                            return;
                        }
                        //新增数据
                        if (eventClass == WriteRowsEvent.class || eventClass == WriteRowsEventV2.class) {
                            List<Row> rows;
                            if (eventClass == WriteRowsEvent.class) {
                                WriteRowsEvent actualEvent = (WriteRowsEvent) event;
                                rows = actualEvent.getRows();
                            } else {
                                WriteRowsEventV2 actualEvent = (WriteRowsEventV2) event;
                                rows = actualEvent.getRows();
                            }
                            List<Column> columns = null;
                            for (Row row : rows) {
                                columns = row.getColumns();
                                break;
                            }
                            if (columns != null && columns.size() > 0) {
                                String bizId = RuleUtil.getBizId(columns.get(0).getValue());
                                bizId = String.format("%s_%s", producerName, bizId);
                                Map<String, Object> obj = new HashMap<String, Object>();
                                obj.put("id", bizId);
//                                Task task = new Task(bizId, OperationEnum.INSERT, obj, producerName);
//                                queue.push(task);
                                pushTask(bizId, obj, OperationEnum.INSERT);
                                logger.info("==========监听到插入操作, db:{}, table:{}, bizid:{}", database, tableName, bizId);
                            }
                            //更新binlog position位置
                            record.setLatestPos(getProducerName(), String.valueOf(event.getHeader().getPosition()));
                        }
                        //更新数据
                        else if (eventClass == UpdateRowsEvent.class || eventClass == UpdateRowsEventV2.class) {
                            List<Pair<Row>> rows;
                            if (eventClass == UpdateRowsEvent.class) {
                                UpdateRowsEvent actualEvent = (UpdateRowsEvent) event;
                                rows = actualEvent.getRows();
                            } else {
                                UpdateRowsEventV2 actualEvent = (UpdateRowsEventV2) event;
                                rows = actualEvent.getRows();
                            }
                            List<Column> beforeColumns = null;
                            List<Column> afterColumns = null;
                            for (Pair<Row> row : rows) {
                                Row beforeRow = row.getBefore();
                                Row afterRow = row.getAfter();
                                beforeColumns = beforeRow.getColumns();
                                afterColumns = afterRow.getColumns();
                                break;
                            }
                            List<String> fields = JdbcUtil.listFields(jdbcTemplate, tableName);

                            if (beforeColumns != null && beforeColumns.size() > 0) {
                                String bizId = RuleUtil.getBizId(beforeColumns.get(0).getValue());
                                bizId = String.format("%s_%s", producerName, bizId);
                                Map<String, Object> obj = new HashMap<String, Object>();
                                obj.put("id", bizId);
                                for (int j = 0; j < fields.size(); j++) {
                                    String field = fields.get(j);
                                    if (RuleUtil.containField(rules, database, tableName, field)) {
                                        obj.put(field, afterColumns.get(j).getValue());
                                    }
                                }
//                                Task task = new Task(bizId, OperationEnum.UPDATE, obj, producerName);
//                                queue.push(task);
                                pushTask(bizId, obj, OperationEnum.UPDATE);
                                logger.info("==========监听到更新操作, db:{}, table:{}, bizid:{}", database, tableName, bizId);
                            }
                            //更新binlog position位置
                            record.setLatestPos(getProducerName(), String.valueOf(event.getHeader().getPosition()));
                        }
                        //删除数据
                        else if (eventClass == DeleteRowsEvent.class || eventClass == DeleteRowsEventV2.class) {
                            List<Row> rows;
                            if (eventClass == DeleteRowsEvent.class) {
                                DeleteRowsEvent actualEvent = (DeleteRowsEvent) event;
                                rows = actualEvent.getRows();
                            } else {
                                DeleteRowsEventV2 actualEvent = (DeleteRowsEventV2) event;
                                rows = actualEvent.getRows();
                            }
                            List<Column> columns = null;
                            for (Row row : rows) {
                                columns = row.getColumns();
                                break;
                            }
                            if (columns != null && columns.size() > 0) {
                                String bizId = RuleUtil.getBizId(columns.get(0).getValue());
                                bizId = String.format("%s_%s", producerName, bizId);
                                Map<String, Object> obj = new HashMap<String, Object>();
                                obj.put("id", bizId);
//                                Task task = new Task(bizId, OperationEnum.DELETE, obj, producerName);
//                                queue.push(task);
                                pushTask(bizId, obj, OperationEnum.DELETE);
                                logger.info("==========监听到删除操作, db:{}, table:{}, bizid:{}", database, tableName, bizId);
                            }
                            //更新binlog position位置
                            record.setLatestPos(getProducerName(), String.valueOf(event.getHeader().getPosition()));
                        }
                    }
                }
            }
        });
    }

    @Override
    public List<Task> produce() {
        openReplicator.start();
        return null;
    }

    /**
     * 获取当前binlog的position
     *
     * @return 返回当前的position
     */
    public long getBinlogPos() {
        //先从record中获取
        Long position = Long.parseLong(this.record.getLatestPos(this.getProducerName()));
        if (position != null && position > 0) {
            return position;
        }
        //record中获取不到 直接读取当前的binlog
        Map<String, Object> info = this.openReplicator.getBinlogInfo();
        if (info.containsKey("position")) {
            return (long) info.get("position");
        }
        return 0;
    }

    /**********************
     * getter and setter
     **********************/
    public void setPosition(long position) {
        this.position = position;
    }

    public void setMonitorRule(String monitorRule) {
        this.monitorRule = monitorRule;
    }

    public void setOpenReplicator(AutoOpenReplicator openReplicator) {
        this.openReplicator = openReplicator;
    }

    public void setRecord(IRecord record) {
        this.record = record;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
}

