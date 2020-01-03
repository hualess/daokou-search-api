package com.dkjk.search.enums;

public enum CodeEnum {

    SUCCESS(200, "操作成功！"),
    PAGE_OUT_ERROR(202, "分页数超过实际数组"),
    NO_DATA(204, "暂无数据"),
    NOT_EXIST(205, "该信息不存在"),
    DATA_EXPIRE(206, "数据已过期"),
    COM_NOT_EXIST(207, "该公司不存在"),
    REPORT_IN_PROGRESS(208, "报告正在生成中"),
    DATA_NOT_CORRESPOND(209, "输入姓名和工商信息中法人名称不同"),
    IN_PROCESS(210, "正在处理中"),


    PARAMETER_ERROR(99101, "输入参数错误"),
    QUERY_ERROR(99102, "查询失败"),
    SUBMIT_ERROR(99103, "提交失败"),
    SEQUENCENO_ERROR(99104, "sequenceNo不合法"),
    CRAWL_DATA_ERROR(99105, "本次爬虫流程失败,请重新走流程"),
    ACCOUNT_PASSWORD_ERROR(99106, "账号密码错误"),
    QUERY_CODE_ERROR(99107, "查询码无效"),
    DATA_NOT_INCOMPLETE(99108, "姓名/身份证号/手机号全传或者全不传"),
    VERIFY_FAIL(99109, "验证不通过"),
    PHONE_FORMAT_ERROR(99110, "手机号格式不正确"),
    ID_FORMAT_ERROR(99111,"身份证号格式不正确"),



    APIKEY_MISS(99001, "请传输参数apikey"),
    USER_NOEXIST(99002, "用户不存在"),
    USER_FREEZE(99003, "用户已被冻结"),
    NO_PERMISSION(99004, "没有使用该接口权限"),
    IP_DENY(99005, "IP受限"),
    NUM_DENY(99006, "请求次数达到限制"),
    NOT_ENOUGH(99007, "余额不足"),
    HIGH_FREQUENCY(99008, "调用频率过高"),
    SYS_ERROR(99999, "系统出现异常");

    /**
     * 响应状态码
     */
    private final int code;

    /**
     * 响应提示
     */
    private final String msg;

    CodeEnum(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }

}
