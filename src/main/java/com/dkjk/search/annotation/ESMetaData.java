package com.dkjk.search.annotation;

import java.lang.annotation.*;


/**
 * es索引元数据的注解，在es entity class上添加
 * @author liubaohua
 * @date 2019-10-29 11:37
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
public @interface ESMetaData {
    /**
     * 索引名称，必须配置
     */
    String indexName();
    /**
     * 索引类型，必须配置，墙裂建议每个index下只有一个type
     */
    String indexType() default "_doc";
    /**
     * 主分片数量
     * @return
     */
    int number_of_shards() default 5;
    /**
     * 备份分片数量
     * @return
     */
    int number_of_replicas() default 1;

    /**
     * 是否打印日志
     * @return
     */
    boolean printLog() default false;
}
