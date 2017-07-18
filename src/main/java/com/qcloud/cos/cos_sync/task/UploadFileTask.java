package com.qcloud.cos.cos_sync.task;

import java.util.concurrent.Semaphore;

import org.json.JSONObject;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.cos_sync.meta.FileStat;
import com.qcloud.cos.meta.InsertOnly;
import com.qcloud.cos.request.DelFileRequest;
import com.qcloud.cos.request.UploadFileRequest;

/*
 * 8 上传文件的任务
 */
public class UploadFileTask extends CosTask {

    private String localPath;

    public UploadFileTask(COSClient cosClient, String bucketName, String cosPath, String localPath,
            FileStat stat, Semaphore sem) {
        super(cosClient, bucketName, cosPath, stat, sem, TaskKind.UploadFileTask);
        this.localPath = localPath;
    }

    @Override
    public TaskResult call() throws Exception {
        sem.acquire();
        try {
            int maxRetyCnt = 5;
            UploadFileRequest uploadRequest =
                    new UploadFileRequest(this.bucketName, this.cosPath, this.localPath);
            uploadRequest.setInsertOnly(InsertOnly.OVER_WRITE);
            String uploadRet = this.cosClient.uploadFile(uploadRequest);
            TaskResult taskResult = buildTaskResult(uploadRet);
            JSONObject jsonObject = new JSONObject(uploadRet);
            int retryIndex = 0;
            // 如果是因为残损文件导致本次上传失败的，那么久删除残损文件，并进行重试
            while (jsonObject.getInt("code") != 0) {
                // retry
                DelFileRequest delRequest = new DelFileRequest(this.bucketName, this.cosPath);
                this.cosClient.delFile(delRequest);
                uploadRet = this.cosClient.uploadFile(uploadRequest);
                taskResult = buildTaskResult(uploadRet);
                jsonObject = new JSONObject(uploadRet);
                ++retryIndex;
                if (retryIndex >= maxRetyCnt) {
                    break;
                }
            }
            return taskResult;
        } finally {
            sem.release();
        }
    }

}
