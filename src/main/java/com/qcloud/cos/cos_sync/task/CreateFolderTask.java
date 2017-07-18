package com.qcloud.cos.cos_sync.task;

import java.util.concurrent.Semaphore;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.cos_sync.meta.FileStat;
import com.qcloud.cos.request.CreateFolderRequest;

/*
 * 8 创建目录的任务
 */
public class CreateFolderTask extends CosTask {


    public CreateFolderTask(COSClient cosClient, String bucketName, String cosPath,
            FileStat fileStat, Semaphore sem) {
        super(cosClient, bucketName, cosPath, fileStat, sem, TaskKind.CreateFolderTask);
    }

    @Override
    public TaskResult call() throws Exception {
        sem.acquire();
        try {
            CreateFolderRequest request = new CreateFolderRequest(this.bucketName, this.cosPath);
            String ret = this.cosClient.createFolder(request);
            return buildTaskResult(ret);
        } finally {
            sem.release();
        }
    }

}
