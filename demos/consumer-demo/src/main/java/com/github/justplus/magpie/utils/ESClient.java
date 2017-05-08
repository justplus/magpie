package com.github.justplus.magpie.utils;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;

/**
 * Created by zhaoliang on 2017/3/27.
 */
public class ESClient implements DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(ESClient.class);

    protected Client client;
    protected String hosts;
    protected String clusterName;

    /**********************
     * 初始化
     **********************/
    public ESClient(String hosts, String clusterName) throws UnknownHostException {
        super();
        this.init(hosts, clusterName);
    }

    // 初始化client
    private void init(String hosts, String clusterName) throws UnknownHostException {
        if (client != null) return;
        this.hosts = hosts;
        this.clusterName = clusterName;
        Settings settings = Settings.builder()
                .put("client.transport.sniff", true)
                .put("cluster.name", clusterName)
                .build();
        TransportClient transportClient = new PreBuiltTransportClient(settings);
        String[] addresses = hosts.split(",");

        String hostName = "";
        int port = 9300;
        for (String addr : addresses) {
            String[] oneAddress = addr.split(":");
            if (oneAddress.length == 1) {
                hostName = oneAddress[0];
            } else if (oneAddress.length == 2) {
                hostName = oneAddress[0];
                port = Integer.valueOf(oneAddress[1]);
            } else {
                // 抛出异常,配置文件错误
                logger.info("ES节点配置错误:{0}, 无法解析成ip:port形式", addr);
            }
            if (StringUtils.isNotBlank(hostName) && port > 0) {
                transportClient.addTransportAddress(
                        new InetSocketTransportAddress(InetAddress.getByName(hostName), port));
            }
        }
        client = transportClient;
    }

    /************************* ES操作 *************************/
    /**
     * 新增或者更新索引, 自动生成索引id
     *
     * @param index 索引
     * @param type  类型
     * @param doc   新增或者更新的对象
     * @return 返回是否新增或者更新成功
     */
    public boolean insertIndex(String index, String type, Map<String, Object> doc) {
        IndexResponse response = getClient().prepareIndex(index, type).setSource(doc).execute().actionGet();
        return StringUtils.isNotBlank(response.getId());
    }

    /**
     * 更新文档索引
     *
     * @param index 索引
     * @param type  类型
     * @param id    索引id
     * @param doc   索引更新字段
     * @return 判断是否更新成功
     */
    public boolean updateIndex(String index, String type, String id, Map<String, Object> doc) {
        UpdateResponse response = getClient().prepareUpdate(index, type, id).setDoc(doc).execute().actionGet();
        return StringUtils.isNotBlank(response.getId());
    }

    /**
     * 新增或者更新索引
     *
     * @param index 索引
     * @param type  类型
     * @param id    索引id
     * @param doc   新增或者更新的对象
     * @return 返回是否新增或者更新成功
     */
    public boolean insertIndex(String index, String type, String id, Map<String, Object> doc) {
        IndexResponse response = getClient().prepareIndex(index, type, id).setSource(doc).execute().actionGet();
        return StringUtils.isNotBlank(response.getId());
    }

    /**
     * 批量插入或者更新索引
     *
     * @param index 索引
     * @param type  类型
     * @param docs  新增或者更新的对象列表
     * @return 返回是否全部新增或者更新成功
     */
    public boolean batchInsertIndex(String index, String type, Collection<Map<String, Object>> docs) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (Map<String, Object> doc : docs) {
            IndexRequest request = client.prepareIndex(index, type).setSource(doc).request();
            bulkRequest.add(request);
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * 删除单条索引
     *
     * @param index 索引
     * @param type  类型
     * @param id    索引id
     * @return 返回是否删除成功
     */
    public boolean deleteIndex(String index, String type, String id) {
        DeleteResponse response = getClient().prepareDelete(index, type, id).execute().actionGet();
        return StringUtils.isNotBlank(response.getId());
    }

    /**
     * 批量删除索引
     *
     * @param index 索引
     * @param type  类型
     * @param ids   索引id列表
     * @return 返回是否全部删除成功
     */
    public boolean batchDeleteIndex(String index, String type, Collection<String> ids) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (String id : ids) {
            DeleteRequest request = client.prepareDelete(index, type, id).request();
            bulkRequest.add(request);
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void destroy() throws Exception {
        client.close();
    }

    /**********************
     * getter and setter
     **********************/
    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public Client getClient() {
        return client;
    }
}
