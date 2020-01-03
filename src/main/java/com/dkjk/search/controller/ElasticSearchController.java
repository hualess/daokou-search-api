package com.dkjk.search.controller;

import com.dkjk.search.enums.CodeEnum;
import com.dkjk.search.service.ElasticSearchService;
import com.dkjk.search.vo.ResponseBean;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * es 操作
 * @author liubaohua
 * @date 2019-10-29 10:23
 */
@Api(value = "es接口",tags = {"es接口"})
@RestController
@CrossOrigin(origins = "*", methods = {RequestMethod.GET, RequestMethod.POST, RequestMethod.DELETE, RequestMethod.PUT})
public class ElasticSearchController {


    @Resource
    private ElasticSearchService elasticSearchService;


    @ApiOperation(value = "查询demo", notes = "查询demo")
    @RequestMapping(value = "/es/query/demo", method ={RequestMethod.GET})
    public ResponseBean esQueryDemo(){
        try {
            return elasticSearchService.esQueryDemo();
        } catch (Exception ex) {
            return new ResponseBean(CodeEnum.SYS_ERROR.getCode(), CodeEnum.SYS_ERROR.getMsg(), ex.getMessage());
        }
    }

    @ApiOperation(value = "scroll", notes = "scroll")
    @RequestMapping(value = "/es/scroll/demo", method ={RequestMethod.GET})
    public ResponseBean esScrollDemo(){
        try {
            elasticSearchService.scroll();
            return new ResponseBean(CodeEnum.SUCCESS.getCode(), CodeEnum.SUCCESS.getMsg(),null);
        } catch (Exception ex) {
            return new ResponseBean(CodeEnum.SYS_ERROR.getCode(), CodeEnum.SYS_ERROR.getMsg(), ex.getMessage());
        }
    }

    @ApiOperation(value = "索引创建->对应mysql的Databases", notes = "索引创建->对应mysql的Databases")
    @RequestMapping(value = "/es/create/index", method ={RequestMethod.GET})
    public ResponseBean createIndex(@RequestParam String indexName){
        try {
            elasticSearchService.createIndex2(indexName);
            return new ResponseBean(CodeEnum.SUCCESS.getCode(), CodeEnum.SUCCESS.getMsg(),null);
        } catch (Exception ex) {
            return new ResponseBean(CodeEnum.SYS_ERROR.getCode(), CodeEnum.SYS_ERROR.getMsg(), ex.getMessage());
        }
    }

    @ApiOperation(value = "索引删除", notes = "索引删除")
    @RequestMapping(value = "/es/delete/index", method ={RequestMethod.GET})
    public ResponseBean deleteIndex(@RequestParam String indexName){
        try {
            elasticSearchService.deleteIndex(indexName);
            return new ResponseBean(CodeEnum.SUCCESS.getCode(), CodeEnum.SUCCESS.getMsg(),null);
        } catch (Exception ex) {
            return new ResponseBean(CodeEnum.SYS_ERROR.getCode(), CodeEnum.SYS_ERROR.getMsg(), ex.getMessage());
        }
    }
    @ApiOperation(value = "单条文档删除", notes = "单条文档删除")
    @RequestMapping(value = "/es/doc/delete", method ={RequestMethod.GET})
    public ResponseBean deleteIndexData(@RequestParam String indexName,@RequestParam String docId){
        try {
            elasticSearchService.deleteDoc(indexName,docId);
            return new ResponseBean(CodeEnum.SUCCESS.getCode(), CodeEnum.SUCCESS.getMsg(),null);
        } catch (Exception ex) {
            return new ResponseBean(CodeEnum.SYS_ERROR.getCode(), CodeEnum.SYS_ERROR.getMsg(), ex.getMessage());
        }
    }

    @ApiOperation(value = "批量数据插入", notes = "批量数据插入")
    @RequestMapping(value = "/es/doc/insert/bulk", method ={RequestMethod.GET})
    public ResponseBean bulkInsertDoc(@RequestParam String indexName){
        try {
            elasticSearchService.bulkInsertDoc(indexName);
            return new ResponseBean(CodeEnum.SUCCESS.getCode(), CodeEnum.SUCCESS.getMsg(),null);
        } catch (Exception ex) {
            return new ResponseBean(CodeEnum.SYS_ERROR.getCode(), CodeEnum.SYS_ERROR.getMsg(), ex.getMessage());
        }
    }

    @ApiOperation(value = "单条数据插入", notes = "单条数据插入")
    @RequestMapping(value = "/es/doc/insert", method ={RequestMethod.GET})
    public ResponseBean insertDoc(@RequestParam String indexName){
        try {
            elasticSearchService.insertDoc(indexName);
            return new ResponseBean(CodeEnum.SUCCESS.getCode(), CodeEnum.SUCCESS.getMsg(),null);
        } catch (Exception ex) {
            return new ResponseBean(CodeEnum.SYS_ERROR.getCode(), CodeEnum.SYS_ERROR.getMsg(), ex.getMessage());
        }
    }

    @ApiOperation(value = "单条文档修改", notes = "单条数据插入")
    @RequestMapping(value = "/es/doc/update", method ={RequestMethod.GET})
    public ResponseBean updateDoc(@RequestParam String indexName,@RequestParam String docId){
        try {
            elasticSearchService.updateDoc(indexName,docId);
            return new ResponseBean(CodeEnum.SUCCESS.getCode(), CodeEnum.SUCCESS.getMsg(),null);
        } catch (Exception ex) {
            return new ResponseBean(CodeEnum.SYS_ERROR.getCode(), CodeEnum.SYS_ERROR.getMsg(), ex.getMessage());
        }
    }

    @ApiOperation(value = "单条文档修改-脚本修改", notes = "单条数据插入-脚本修改")
    @RequestMapping(value = "/es/doc/update/script", method ={RequestMethod.GET})
    public ResponseBean updateDocScript(@RequestParam String indexName,@RequestParam String docId){
        try {
            elasticSearchService.updateDocScript(indexName,docId);
            return new ResponseBean(CodeEnum.SUCCESS.getCode(), CodeEnum.SUCCESS.getMsg(),null);
        } catch (Exception ex) {
            return new ResponseBean(CodeEnum.SYS_ERROR.getCode(), CodeEnum.SYS_ERROR.getMsg(), ex.getMessage());
        }
    }
}
