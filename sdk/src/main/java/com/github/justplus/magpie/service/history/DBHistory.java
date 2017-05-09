package com.github.justplus.magpie.service.history;

import com.alibaba.fastjson.JSON;
import com.github.justplus.magpie.model.Task;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhaoliang on 2017/5/9.
 */
public class DBHistory implements IHistory {
    private JdbcTemplate jdbcTemplate;

    public DBHistory(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 生产消息后及时落地,确保消息不会丢失
     *
     * @param task 消息体
     * @return 返回消息是否落地成功
     */
    @Override
    public boolean insertProducerHistory(Task task) {
        String sql = "insert into history (task_id, biz_id, task, consumed, create_time) values (?, ?, ?, 0, unix_timestamp())";
        int rows = this.jdbcTemplate.update(sql, new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
                ps.setString(1, task.getTaskId());
                ps.setString(2, task.getBizId());
                ps.setString(3, JSON.toJSONString(task));
            }
        });
        return rows == 1;
    }

    /**
     * 上一条消息是否被消费
     *
     * @param taskId 消息id
     * @return 返回上一条消息是否被消费
     */
    @Override
    public boolean preTaskConsumed(String taskId) {
        String sql = "select consumed from history where biz_id= (select biz_id from history where task_id = ?) order by id desc limit 1";
        Boolean result = this.jdbcTemplate.queryForObject(sql, new Object[]{taskId}, Boolean.class);
        return result == null || result == true;
    }

    /**
     * 判断消息是否被消费过,防止重复消费
     *
     * @param taskId 消息id
     * @return 返回消息是否被消费
     */
    @Override
    public boolean isTaskConsumed(String taskId) {
        String sql = "select consumed from history where task_id = ?";
        Boolean result = this.jdbcTemplate.queryForObject(sql, new Object[]{taskId}, Boolean.class);
        return result == null || result == true;
    }

    /**
     * 更新消息的消费状态
     *
     * @param taskId 消息id
     * @return 是否更新成功
     */
    @Override
    public boolean updateConsumerState(String taskId) {
        String sql = "update history set consumed = 1 where task_id = ?";
        int rows = this.jdbcTemplate.update(sql, taskId);
        return rows == 1;
    }

    /**
     * 获取没有被正常消费的任务列表
     *
     * @param timeout 多长时间内没有正确响应的算作超时任务
     * @return 返回没有被正常消费的消息列表
     */
    @Override
    public List<Task> listUnConsumedTasks(int timeout) {
        String sql = "select * from history where consumed = 0 and create_time < unix_timestamp() - ?";
        List<Map<String, Object>> result = this.jdbcTemplate.queryForList(sql, timeout);

        List<Task> tasks = new ArrayList<>();
        for (Map<String, Object> r : result) {
            Task task = JSON.parseObject((String) r.get("task"), Task.class);
            if (task != null) {
                tasks.add(task);
            }
        }
        return tasks;
    }
}
