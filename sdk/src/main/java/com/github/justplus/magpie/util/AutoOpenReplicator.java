package com.github.justplus.magpie.util;

import com.google.code.or.OpenReplicator;
import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.net.Packet;
import com.google.code.or.net.Transport;
import com.google.code.or.net.impl.packet.EOFPacket;
import com.google.code.or.net.impl.packet.ResultSetRowPacket;
import com.google.code.or.net.impl.packet.command.ComQuery;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * MySQL binlog分析程序 ,用到open-replicator包
 */
public class AutoOpenReplicator extends OpenReplicator {
    private static final Logger logger = LoggerFactory.getLogger(AutoOpenReplicator.class);

    // 是否自动重连
    private boolean autoReconnect = true;
    // 重新连接的时间，默认30秒
    private int delayReconnect = 30;

    private Transport comQueryTransport;

    @Override
    public void start() {
        do {
            Map<String, Object> info = this.getBinlogInfo();
            if (info.containsKey("fileName") && StringUtils.isNotBlank((String) info.get("fileName"))) {
                this.setBinlogFileName((String) info.get("fileName"));
            }
            try {
                if (!this.isRunning()) {
                    logger.info("=========开始监听binlog, 当前binlog名称:{}, 位置:{}=========",
                            this.getBinlogFileName(), this.getBinlogPosition());
                    this.reset();
                    super.start();
                }
                TimeUnit.SECONDS.sleep(this.getDelayReconnect());
            } catch (Exception e) {
                logger.error("监听binlog出现异常:{}", e);
                try {
                    TimeUnit.SECONDS.sleep(this.getDelayReconnect());
                } catch (InterruptedException ignore) {
                }
            }
        } while (this.autoReconnect);
    }

    @Override
    public void stopQuietly(long timeout, TimeUnit unit) {
        super.stopQuietly(timeout, unit);
        if (this.getBinlogParser() != null) {
            //重置, 当MySQL服务器进行restart/stop操作时进入该流程
            this.binlogParser.setParserListeners(null);
        }
    }

    public Map<String, Object> getBinlogInfo() {
        Map<String, Object> info = new HashMap<>();
        try {
            ResultSetRowPacket binlogPacket = query("show master status");
            if (binlogPacket != null) {
                List<StringColumn> values = binlogPacket.getColumns();
                info.put("fileName", values.get(0).toString());
                info.put("position", Long.valueOf(values.get(1).toString()));
            } else {
                logger.error("[show master status]查询失败");
            }
        } catch (Exception e) {
            logger.error("[show master status]出现异常:{}", e);
        }
        return info;
    }

    @Override
    public int getServerId() {
        if (super.getServerId() > 0) {
            return super.getServerId();
        }
        //如果没有手动设置server_id, 则自动获取
        try {
            ResultSetRowPacket binlogPacket = query("show variables like 'server_id'");
            if (binlogPacket != null) {
                List<StringColumn> values = binlogPacket.getColumns();
                return Integer.parseInt(values.get(0).toString());
            } else {
                logger.error("[show variables like 'server_id']查询失败, 必须手动设置server_id");
            }
        } catch (Exception e) {
            logger.error("[show variables like 'server_id']出现异常:{}, 必须手动设置server_id", e);
        }
        return -1;
    }

    /**
     * ComQuery 查询
     *
     * @param sql 查询语句
     * @return
     */
    private ResultSetRowPacket query(String sql) throws Exception {
        ResultSetRowPacket row = null;
        final ComQuery command = new ComQuery();
        command.setSql(StringColumn.valueOf(sql.getBytes()));
        if (this.comQueryTransport == null) this.comQueryTransport = getDefaultTransport();
        this.comQueryTransport.connect(this.host, this.port);
        this.comQueryTransport.getOutputStream().writePacket(command);
        this.comQueryTransport.getOutputStream().flush();
        // step 1
        this.comQueryTransport.getInputStream().readPacket();
        //
        Packet packet;
        // step 2
        while (true) {
            packet = comQueryTransport.getInputStream().readPacket();
            if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
                break;
            }
        }
        // step 3
        while (true) {
            packet = comQueryTransport.getInputStream().readPacket();
            if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
                break;
            } else {
                row = ResultSetRowPacket.valueOf(packet);
            }
        }
        this.comQueryTransport.disconnect();
        return row;
    }

    private void reset() {
        this.transport = null;
        this.binlogParser = null;
    }


    /**********************
     * getter and setter
     **********************/
    public boolean isAutoReconnect() {
        return autoReconnect;
    }

    public void setAutoReconnect(boolean autoReconnect) {
        this.autoReconnect = autoReconnect;
    }

    public void setDelayReconnect(int delayReconnect) {
        this.delayReconnect = delayReconnect;
    }

    public int getDelayReconnect() {
        return delayReconnect;
    }

}