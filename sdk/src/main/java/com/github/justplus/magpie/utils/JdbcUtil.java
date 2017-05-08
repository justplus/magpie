package com.github.justplus.magpie.utils;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;

import java.util.Arrays;
import java.util.List;

/**
 * Created by zhaoliang on 2017/5/3.
 */
public class JdbcUtil {
    public static List<String> listFields(JdbcTemplate jdbcTemplate, String tableName) {
        SqlRowSet rowSet = jdbcTemplate.queryForRowSet(String.format("select * from %s limit 0", tableName));
        SqlRowSetMetaData metaData = rowSet.getMetaData();
        return Arrays.asList(metaData.getColumnNames());
    }
}
