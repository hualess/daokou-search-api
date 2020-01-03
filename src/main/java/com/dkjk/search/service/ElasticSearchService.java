package com.dkjk.search.service;


import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.dkjk.search.entity.BookInfo;
import com.dkjk.search.entity.FieldEntity;
import com.dkjk.search.entity.MappingEntity;
import com.dkjk.search.enums.CodeEnum;
import com.dkjk.search.index.ElasticsearchIndex;
import com.dkjk.search.vo.ResponseBean;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 * scroll测试 超过10000条报错
 * @author liubaohua
 * @date 2019-10-25 16:58
 */
@Service
@Slf4j
public class ElasticSearchService {

    @Autowired
    RestHighLevelClient highLevelClient;
    @Autowired
    ElasticsearchIndex elasticsearchIndex;


    public ResponseBean esQueryDemo() {
        SearchRequest searchRequest = new SearchRequest("enterprise_change_records_3");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //精准匹配
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("company_name", "北京");
        //捕捉全部
        MatchAllQueryBuilder matchAllQueryBuilder = matchAllQuery();


        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        boolBuilder.must(matchAllQueryBuilder);
        sourceBuilder.query(boolBuilder);

        //分页
        sourceBuilder.from(0);
        sourceBuilder.size(10);

        //排序
        FieldSortBuilder fsb = SortBuilders.fieldSort("create_month");
        fsb.order(SortOrder.DESC);
        sourceBuilder.sort(fsb);

        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            //总数
            long count=hits.getTotalHits().value;
            SearchHit[] searchHits = hits.getHits();
            System.out.println("------size---"+searchHits.length);
            if(searchHits.length<=0)
                return  new ResponseBean(CodeEnum.NO_DATA.getCode(),CodeEnum.NO_DATA.getMsg(),null);
            for (SearchHit searchHit : searchHits) {
                System.out.println(searchHit.getSourceAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  null;
    }

    /**
     * scroll API 可以被用来检索大量的结果（甚至所有的结果）
     * 深度分页不管是关系型数据库还是Elasticsearch还是其他搜索引擎，都会带来巨大性能开销，特别是在分布式情况下。
     * 有些问题可以考业务解决而不是靠技术解决，比如很多业务都对页码有限制，google 搜索，往后翻到一定页码就不行了。
     * scroll 并不适合用来做实时搜索，而更适用于后台批处理任务，比如群发。
     * search_after不能自由跳到一个随机页面，只能按照 sort values 跳转到下一页。
     * 使用form size 限制页码
     */
    public void scroll(){
        //初始化scroll
        // 这个时间并不需要长到可以处理所有的数据，仅仅需要足够长来处理前一批次的结果。每个 scroll 请求（包含 scroll 参数）设置了一个新的失效时间。
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L)); //设定滚动时间间隔
        SearchRequest searchRequest = new SearchRequest("enterprise_change_records_3");
        searchRequest.scroll(scroll);

        //筛选条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchAllQuery());
        searchSourceBuilder.size(10000); //设定每次返回多少条数据
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        int page = 0 ;
        try {
            searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        //System.out.println("-----首页-----");
        page++;
        System.out.println("-----第"+ page +"页-----");
        for (SearchHit searchHit : searchHits) {
            //System.out.println(searchHit.getSourceAsString());
        }
        //遍历搜索命中的数据，直到没有数据
        while (searchHits != null && searchHits.length > 0) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            try {
                searchResponse = highLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
            if (searchHits != null && searchHits.length > 0) {
                page++;
                System.out.println("-----第"+ page +"页-----");
                //System.out.println("-----下一页-----");
                for (SearchHit searchHit : searchHits) {
                    //System.out.println(searchHit.getSourceAsString());
                }
            }
        }
        //清除滚屏
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);//也可以选择setScrollIds()将多个scrollId一起使用
        ClearScrollResponse clearScrollResponse = null;
        try {
            clearScrollResponse = highLevelClient.clearScroll(clearScrollRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean succeeded = clearScrollResponse.isSucceeded();
        System.out.println("succeeded:" + succeeded);
    }

/*  es7 支持数据类型
    text：默认会进行分词，支持模糊查询（5.x之后版本string类型已废弃，请大家使用text）。
    keyword：不进行分词；keyword类型默认开启doc_values来加速聚合排序操作，占用了大量磁盘io 如非必须可以禁用doc_values。
    number：如果只有过滤场景 用不到range查询的话，使用keyword性能更佳，另外数字类型的doc_values比字符串更容易压缩。
    array：es不需要显示定义数组类型，只需要在插入数据时用'[]'表示即可，'[]'中的元素类型需保持一致。
    range：对数据的范围进行索引；目前支持 number range、date range 、ip range。
    boolean: 只接受true、false 也可以是字符串类型的“true”、“false”
    date：支持毫秒、根据指定的format解析对应的日期格式，内部以long类型存储。
    geo_point：存储经纬度数据对。
    ip：将ip数据存储在这种数据类型中，方便后期对ip字段的模糊与范围查询。
    nested：嵌套类型，一种特殊的object类型，存储object数组，可检索内部子项。
    object：嵌套类型，不支持数组。*/
    public void createIndex(String indexName) {
        try {
            if(existsIndex(indexName)){
                log.info("索引<{}>已存在:请先进行删除",indexName);
                return;
            }
            CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);

            //设置分片的数量，以及副本的数量
            //index.number_of_shards  主分片数，默认为5.只能在创建索引时设置，不能修改
            //index.number_of_replicas  每个主分片的副本数。默认为 1。
            indexRequest.settings(Settings.builder()
                    .put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 2)
            );
            //设置mapper 映射 创建索引时创建文档类型映射

            //properties下面的为索引里面的字段
            //type为数据类型
            //index: true or false
            //es6 之后规定 一个索引对应一个类型
            //es7 创建索引的时候不能指定type type默认为doc  直接插入值得时候可以指定type
            //es8 彻底去除type字段
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    //通过dynamic参数来控制字段的新增：
                    //true（默认）允许自动新增字段
                    //false 不允许自动新增字段，但是文档可以正常写入，但无法对新增字段进行查询等操作
                    //strict 文档不能写入，报错
                    .startObject().field("dynamic","strict")
                    .startObject("properties")
                    //字段映射 设置属性 es7中没有integer
                    .startObject("id").field("type","text").field("index","true").endObject()
                    //分词
                    //ik_max_word 会将文本做最细粒度的拆分，比如会将“中华人民共和国人民大会堂”拆分为“中华人民共和国、中华人民、中华、华人、人民共和国、人民、共和国、大会堂、大会、会堂等词语。
                    //ik_smart 会做最粗粒度的拆分，比如会将“中华人民共和国人民大会堂”拆分为中华人民共和国、人民大会堂。
                    .startObject("name").field("type","text").field("index","true").field("analyzer","ik_max_word").endObject()
                    .startObject("author").field("type","text").field("index","true").field("analyzer","ik_max_word").endObject()
                    //不分词
                    .startObject("desc").field("type","text").field("index","false").endObject()
                    .startObject("content").field("type","text").field("index","false").endObject()
                    //date类型格式化
                    .startObject("createTime").field("type","date").field("format","yyyy-MM-dd").endObject()
                    //动态映射
                    .endObject()
                    .endObject();
            indexRequest.mapping(builder);
            // 索引别名
            //别名不仅仅可以关联一个索引，它能聚合多个索引。
            //例如我们为索引my_index_1 和 my_index_2 创建一个别名my_index_alias，这样对my_index_alias的操作(仅限读操作)，会操作my_index_1和my_index_2，类似于聚合了my_index_1和my_index_2.
            // 但是我们是不能对my_index_alias进行写操作，当有多个索引时alias，不能区分到底操作哪一个。
            indexRequest.alias(new Alias("book_alias"));
            log.info("mapping:{}",indexRequest.mappings().toString());
            CreateIndexResponse indexResponse = highLevelClient.indices().create(indexRequest, RequestOptions.DEFAULT);

            //返回的CreateIndexResponse允许检索有关执行的操作的信息，如下所示：
            boolean acknowledged = indexResponse.isAcknowledged();//指示是否所有节点都已确认请求
            boolean shardsAcknowledged = indexResponse.isShardsAcknowledged();//指示是否在超时之前为索引中的每个分片启动了必需的分片副本数
            log.info("索引<{}>创建成功:节点{},分片{}",indexName,acknowledged,shardsAcknowledged);
        } catch (Exception e) {
            log.error("索引<{}>创建失败:{}",indexName, e);
        }
    }

    //构造Mapping，keySet包含所有的字段
    private XContentBuilder getMapping(Set<String> keySet) {
        XContentBuilder mapping = null;
        try {
            mapping = jsonBuilder()
                    .startObject().startObject("properties");
            Iterator<String> it = keySet.iterator();
            while (it.hasNext()) {
                String str = it.next();
                if(str.equals("@timestamp"))	//跳过@timestamp字段
                    continue;
                mapping.startObject(str).field("type", "double").field("index","not_analyzed").endObject();
            }
            mapping.endObject().endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mapping;
    }

    public boolean existsIndex(String indexName) {
        try{
            GetIndexRequest request = new GetIndexRequest(indexName);
            request.local(false);
            request.humanReadable(true);
            boolean exists = highLevelClient.indices().exists(request, RequestOptions.DEFAULT);
            return exists;
        }catch (Exception e){
            log.error("未知错误:{}", e);
        }
        return false;
    }

    public void insertDoc(String indexName) {
        try{
            IndexRequest request = new IndexRequest(indexName);
            request.id(UUID.randomUUID().toString());
            BookInfo bookInfo=new BookInfo(2,"vue","前端","哈哈哈","嘟嘟udududuu", DateUtil.formatDate(new Date()),"哈哈");
            String jsonString=JSON.toJSONString(bookInfo);
            request.source(jsonString, XContentType.JSON);
            IndexResponse response = highLevelClient.index(request, RequestOptions.DEFAULT);
            log.info("status:"+response.status());
            log.info("索引<{}>数据新增成功",indexName);
        }catch (Exception e){
            log.error("索引<{}>数据新增失败:{}",indexName, e);
        }
    }

    public void bulkInsertDoc(String indexName) {
        try {
            BulkRequest bulkRequest = new BulkRequest();
            IndexRequest indexRequest;
            for (int i=0;i<3;i++){
                indexRequest = new IndexRequest(indexName);
                indexRequest.id(UUID.randomUUID().toString());
                BookInfo bookInfo=new BookInfo(i+3,"vue".concat(String.valueOf(i)),"前端".concat(String.valueOf(i)),"哈哈哈".concat(String.valueOf(i)),"嘟嘟udududuu".concat(String.valueOf(i)), DateUtil.formatDate(new Date()),"哈哈".concat(String.valueOf(i)));
                String jsonString=JSON.toJSONString(bookInfo);
                indexRequest.source(jsonString, XContentType.JSON);
                bulkRequest.add(indexRequest);
            }
            BulkResponse bulkResponse = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            if(bulkResponse.hasFailures()){
                log.info("索引<{}>数据批量新增中有错误",indexName);
            }else{
                log.info("索引<{}>数据批量新增成功",indexName);
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.info("索引<{}>数据批量新增失败",indexName);
        }
    }

    /**
     * 删除索引
     * @param indexName 索引名称
     */
    public void deleteIndex(String indexName) {
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            AcknowledgedResponse deleteIndexResponse  = highLevelClient.indices().delete(request, RequestOptions.DEFAULT);
            boolean acknowledged = deleteIndexResponse.isAcknowledged();
            log.info("索引<{}>删除:{}",indexName,acknowledged?"成功":"失败");
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.NOT_FOUND) {
                log.info("索引<{}>不存在",indexName);
            }
        } catch (IOException e) {
            log.info("索引<{}>删除失败",indexName);
            e.printStackTrace();
        }
    }

    public void deleteDoc(String indexName,String docId) {
        try {
            DeleteRequest request = new DeleteRequest(indexName, docId);
            DeleteResponse deleteResponse = highLevelClient.delete(request, RequestOptions.DEFAULT);
            if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                log.info("索引<{}>中文档id为<{}>不存在",indexName,docId);
            }else{
                log.info("索引<{}>中文档id为<{}>删除成功",indexName,docId);
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.info("索引<{}>中文档id为<{}>删除失败",indexName,docId);
        }
    }

    private boolean existsDoc(String indexName,String docId){
        try {
            GetRequest getRequest = new GetRequest(indexName, docId);
            //建议关闭提取功能_source和所有存储的字段，以使请求的内容稍微减轻一些
            getRequest.fetchSourceContext(new FetchSourceContext(false));
            getRequest.storedFields("_none_");
            boolean exists = highLevelClient.exists(getRequest, RequestOptions.DEFAULT);
            return  exists;
        } catch (IOException e) {
            e.printStackTrace();
            return  false;
        }
    }

    public void updateDoc(String indexName, String docId) {
        try {
            UpdateRequest request = new UpdateRequest(indexName, docId);
            String jsonString="{" +
                    "\"author\":\"前端的技术\"," +
                    "\"name\":\"vue-java\"" +
                    "}";
            request.doc(jsonString, XContentType.JSON);
            //如果文档不存在则新增
            //request.upsert(jsonString, XContentType.JSON);
            UpdateResponse updateResponse = highLevelClient.update(request, RequestOptions.DEFAULT);
            if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                log.info("索引<{}>中文档id为<{}>首次创建",indexName,docId);
            } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                log.info("索引<{}>中文档id为<{}>修改成功",indexName,docId);
            } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
                log.info("索引<{}>中文档id为<{}>删除成功",indexName,docId);
            } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
                log.info("索引<{}>中文档id为<{}>未对文档执行任何操作",indexName,docId);
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                log.info("索引<{}>中文档id为<{}>不存在",indexName,docId);
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.info("索引<{}>中文档id为<{}>删除失败",indexName,docId);
        }
    }

    public void updateDocScript(String indexName, String docId) {

    }

    public void createIndex1(String indexName) {
        try {
            if(existsIndex(indexName)){
                log.info("索引<{}>已存在:请先进行删除",indexName);
                return;
            }
            CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);
            indexRequest.settings(Settings.builder()
                    .put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 2)
            );
            BookInfo bookInfo=new BookInfo();
            indexRequest.mapping(JSON.toJSONString(bookInfo),XContentType.JSON);
            log.info("mapping:{}",indexRequest.mappings().toString());
            CreateIndexResponse indexResponse = highLevelClient.indices().create(indexRequest, RequestOptions.DEFAULT);
            //返回的CreateIndexResponse允许检索有关执行的操作的信息，如下所示：
            boolean acknowledged = indexResponse.isAcknowledged();//指示是否所有节点都已确认请求
            boolean shardsAcknowledged = indexResponse.isShardsAcknowledged();//指示是否在超时之前为索引中的每个分片启动了必需的分片副本数
            log.info("索引<{}>创建成功:节点{},分片{}",indexName,acknowledged,shardsAcknowledged);
        } catch (Exception e) {
            log.error("索引<{}>创建失败:{}",indexName, e);
        }
    }

    public void createIndex2(String indexName) {

        try {
            elasticsearchIndex.createIndex(BookInfo.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
