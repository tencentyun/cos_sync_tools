package com.qcloud.cos.cos_sync.task;

import java.util.concurrent.Semaphore;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.cos_sync.meta.FileStat;
import com.qcloud.cos.request.DelFolderRequest;

/**
 * 删除目录任务
 * 
 * @author chengwu
 *
 */
public class DelFolderTask extends CosTask {

    public DelFolderTask(COSClient cosClient, String bucketName, String cosPath, FileStat stat,
            Semaphore sem) {
        super(cosClient, bucketName, cosPath, stat, sem, TaskKind.DelFolderTask);
    }

    @Override
    public TaskResult call() throws Exception {
        sem.acquire();
        try {
            DelFolderRequest request = new DelFolderRequest(this.bucketName, this.cosPath);
            String ret = this.cosClient.delFolder(request);
            return buildTaskResult(ret);
        } finally {
            sem.release();
        }
    }

}
