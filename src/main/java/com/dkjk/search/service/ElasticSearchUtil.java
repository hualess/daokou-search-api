package com.dkjk.search.service;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author liubaohua
 * @date 2019-10-29 14:19
 */
@Slf4j
public class ElasticSearchUtil {


    @Autowired
    RestHighLevelClient highLevelClient;

    public void createIndex(String indexName, String settings, String mappings) {
        try{
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            buildSetting(request, settings);
            buildIndexMapping(request, mappings);
            highLevelClient.indices().create(request, RequestOptions.DEFAULT);
            log.info("索引创建成功");
        }catch (Exception e){
            log.error("索引创建失败:{}", e);
        }
    }
    /**
     * 设置分片
     * @param request
     */
    private void buildSetting(CreateIndexRequest request, String settings) {
        request.settings(settings, XContentType.JSON);
    }

    /**
     * 设置索引的mapping
     * @param request
     */
    private void buildIndexMapping(CreateIndexRequest request, String mappings) {
        request.mapping(mappings, XContentType.JSON);
    }
}
