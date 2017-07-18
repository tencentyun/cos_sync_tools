package com.qcloud.cos.cos_sync.task;

import java.util.concurrent.Semaphore;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.cos_sync.meta.FileStat;
import com.qcloud.cos.request.DelFileRequest;

/**
 * 删除文件的任务
 * 
 * @author chengwu
 *
 */
public class DelFileTask extends CosTask {

    public DelFileTask(COSClient cosClient, String bucketName, String cosPath, FileStat stat,
            Semaphore sem) {
        super(cosClient, bucketName, cosPath, stat, sem, TaskKind.DelFileTask);
    }

    @Override
    public TaskResult call() throws Exception {
        sem.acquire();
        try {
            DelFileRequest request = new DelFileRequest(this.bucketName, this.cosPath);
            String ret = this.cosClient.delFile(request);
            return buildTaskResult(ret);
        } finally {
            sem.release();
        }
    }

}
