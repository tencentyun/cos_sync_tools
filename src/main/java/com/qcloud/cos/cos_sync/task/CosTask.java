package com.qcloud.cos.cos_sync.task;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import org.json.JSONObject;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.cos_sync.meta.FileStat;

/**
 * cos相关的任务父类
 * @author chengwu
 *
 */
public abstract class CosTask implements Callable<TaskResult> {
    // cos 客户端
    protected COSClient cosClient;
    // bucket名
    protected String bucketName;
    // cos路径
    protected String cosPath;

    // 相关联的fileStat
    protected FileStat fileStat;
    
    // 并发控制
    protected Semaphore sem;
    
    // 任务种类
    protected TaskKind kind;

    // 实际的任务执行函数
    public abstract TaskResult call() throws Exception;

    public CosTask(COSClient cosClient, String bucketName, String cosPath, FileStat stat,
            Semaphore sem, TaskKind kind) {
        super();
        this.cosClient = cosClient;
        this.bucketName = bucketName;
        this.cosPath = cosPath;
        this.fileStat = stat;
        this.sem = sem;
        this.kind = kind;
    }

    public String getCosPath() {
        return cosPath;
    }

    public String getBucketName() {
        return bucketName;
    }

    protected TaskResult buildTaskResult(String serverRetStr) {
        JSONObject serverRet = new JSONObject(serverRetStr);
        TaskResult taskResult = new TaskResult();
        taskResult.serverRet = serverRet;
        taskResult.bucketName = this.bucketName;
        taskResult.cosPath = this.cosPath;
        taskResult.fileStat = this.fileStat;
        taskResult.taskKind = this.kind;
        return taskResult;
    }

}
