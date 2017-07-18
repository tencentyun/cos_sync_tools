package com.qcloud.cos.cos_sync.task;

import org.json.JSONObject;

import com.qcloud.cos.cos_sync.meta.FileStat;

/**
 * Task 任务结果
 * @author chengwu
 *
 */
public class TaskResult {
    public String bucketName;    // bucket名称
    public String cosPath;       // cos路径
    public TaskKind taskKind;    // 任务种类
    public JSONObject serverRet; // cos server的返回值
    public FileStat fileStat;    // 相关联的filestat
}
