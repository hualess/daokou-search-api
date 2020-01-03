package com.dkjk.search.entity;

import com.dkjk.search.annotation.ESMapping;
import com.dkjk.search.annotation.ESMetaData;
import lombok.Data;

import java.util.Date;

/**
 * @author liubaohua
 * @date 2019-10-29 11:37
 */
@Data
@ESMetaData(indexName = "book_index_1")
public class BookInfo {

    @ESMapping
    private Integer id;
    @ESMapping
    private String  name;
    @ESMapping
    private String author;
    @ESMapping
    private String desc;
    @ESMapping
    private String content;
    @ESMapping
    private String createTime;
    @ESMapping
    private String temp;


    public BookInfo() {
    }

    public BookInfo(Integer id, String name, String author, String desc, String content, String createTime) {
        this.id = id;
        this.name = name;
        this.author = author;
        this.desc = desc;
        this.content = content;
        this.createTime = createTime;
    }

    public BookInfo(Integer id, String name, String author, String desc, String content, String createTime, String temp) {
        this.id = id;
        this.name = name;
        this.author = author;
        this.desc = desc;
        this.content = content;
        this.createTime = createTime;
        this.temp = temp;
    }
}
