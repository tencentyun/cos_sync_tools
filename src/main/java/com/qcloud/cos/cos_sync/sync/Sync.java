package com.qcloud.cos.cos_sync.sync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.cos_sync.meta.Config;
import com.qcloud.cos.cos_sync.meta.DbRecord;
import com.qcloud.cos.cos_sync.meta.FileStat;
import com.qcloud.cos.cos_sync.meta.LocalFileDirInfo;
import com.qcloud.cos.cos_sync.meta.TaskStatics;
import com.qcloud.cos.cos_sync.task.CreateFolderTask;
import com.qcloud.cos.cos_sync.task.DelFileTask;
import com.qcloud.cos.cos_sync.task.DelFolderTask;
import com.qcloud.cos.cos_sync.task.TaskKind;
import com.qcloud.cos.cos_sync.task.TaskResult;
import com.qcloud.cos.cos_sync.task.UploadFileTask;
import com.qcloud.cos.sign.Credentials;

/**
 * 执行本地同步的主类
 * 
 * @author chengwu
 *
 */
public class Sync {
    private static final Logger LOG = LoggerFactory.getLogger(Sync.class);
    private Config config;
    private DbRecord dbRecord = null;
    private COSClient cosClient;
    private Semaphore sem;
    // 能同时并发执行上传和删除任务数
    private static final int maxThreadCount = 16;
    private ExecutorService executorService = Executors.newFixedThreadPool(maxThreadCount);
    private CompletionService<TaskResult> completionService =
            new ExecutorCompletionService<TaskResult>(this.executorService);
    private long taskCount = 0;
    private static final TaskStatics statics = TaskStatics.getInstance();

    public Sync(Config config) {
        super();
        this.config = config;
        buildCosClient();
        buildParaControl();
    }

    // 用来控制并发度
    private void buildParaControl() {
        int para_num = this.config.getThreadNum();
        if (para_num <= 0) {
            para_num = 1;
        }
        sem = new Semaphore(para_num);
    }

    // 获取db中存储的已经上传成功的文件和目录记录
    private void buildDbRecord() {
        if (this.dbRecord != null) {
            this.dbRecord.shutdown();
        }
        this.dbRecord = new DbRecord(this.config.getDbPath(), Long.valueOf(config.getAppid()),
                this.config.getBucket(), this.config.getLocalPath(), this.config.getCosPath());
    }

    // 生成cos client
    private void buildCosClient() {
        Credentials cred = new Credentials(Long.valueOf(this.config.getAppid()),
                this.config.getSecretId(), this.config.getSecretKey());
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setRegion(this.config.getRegion());
        if (this.config.getEnableHttps() == 1) {
            clientConfig.setUploadCosEndPointPrefix("https://");
        }
        clientConfig.setMaxConnectionsCount(maxThreadCount * 16); // 因为每一个任务中，如果都是大文件，分片上传的并行度是16
        clientConfig.setConnectionTimeout(this.config.getTimeOut() * 1000); // 超时时间单位是ms
        this.cosClient = new COSClient(clientConfig, cred);
    }

    // 清空任务数
    private void clear() {
        this.taskCount = 0;
    }

    // 比较本地的文件或目录，以及上传成功的文件和目录，生成要删除和要创建的文件和目录
    private void diff(ArrayList<FileStat> localFileStatList,
            HashMap<String, FileStat> delFileStatMap, HashMap<String, FileStat> createFileStatMap) {
        for (FileStat stat : localFileStatList) {
            String filePath = stat.getFilePath();
            if (delFileStatMap.containsKey(filePath)) {
                if (stat.isSame(delFileStatMap.get(filePath))) {
                    delFileStatMap.remove(filePath);
                } else {
                    createFileStatMap.put(filePath, stat);
                }
            } else {
                createFileStatMap.put(filePath, stat);
            }
        }
    }

    // 本地文件和目录路径转cos路径
    private String localPathToCosPath(String localPath) {
        String localSourceDir = this.config.getLocalPath();
        String cosTargetDir = this.config.getCosPath();
        String cosPath = cosTargetDir + localPath.substring(localSourceDir.length());
        return cosPath;
    }

    // 执行删除文件的任务
    private void delFile(HashMap<String, FileStat> delFileStatMap) {
        if (this.config.getDeleteSync() != 1) {
            return;
        }
        String bucketName = this.config.getBucket();
        for (Entry<String, FileStat> entry : delFileStatMap.entrySet()) {
            String localPath = entry.getKey();
            FileStat fileStat = entry.getValue();
            String cosPath = localPathToCosPath(localPath);
            DelFileTask task =
                    new DelFileTask(this.cosClient, bucketName, cosPath, fileStat, this.sem);
            completionService.submit(task);
            ++taskCount;
        }
    }

    // 执行删除目录的任务
    private void delFolder(HashMap<String, FileStat> delFolderStatMap) {
        if (this.config.getDeleteSync() != 1) {
            return;
        }
        String bucketName = this.config.getBucket();
        for (Entry<String, FileStat> entry : delFolderStatMap.entrySet()) {
            String localPath = entry.getKey();
            FileStat fileStat = entry.getValue();
            String cosPath = localPathToCosPath(localPath);
            DelFolderTask task =
                    new DelFolderTask(this.cosClient, bucketName, cosPath, fileStat, this.sem);
            completionService.submit(task);
            ++taskCount;
        }
    }

    // 执行创建目录的任务
    private void createFolder(HashMap<String, FileStat> createFolderStatMap) {
        String bucketName = this.config.getBucket();
        for (Entry<String, FileStat> entry : createFolderStatMap.entrySet()) {
            String localPath = entry.getKey();
            FileStat fileStat = entry.getValue();
            String cosPath = localPathToCosPath(localPath);
            if (cosPath.equals("/")) { // 如果是创建bucket路径, 则跳过
                continue;
            }
            CreateFolderTask task =
                    new CreateFolderTask(this.cosClient, bucketName, cosPath, fileStat, this.sem);
            completionService.submit(task);
            ++taskCount;
        }
    }

    // 执行上传文件的任务
    private void uploadFile(HashMap<String, FileStat> createFileStatMap) {
        String bucketName = this.config.getBucket();
        for (Entry<String, FileStat> entry : createFileStatMap.entrySet()) {
            String localPath = entry.getKey();
            FileStat fileStat = entry.getValue();
            String cosPath = localPathToCosPath(localPath);
            UploadFileTask task = new UploadFileTask(this.cosClient, bucketName, cosPath, localPath,
                    fileStat, sem);
            completionService.submit(task);
            ++taskCount;
        }
    }

    // 判断执行的操作是否成功
    private boolean judgeOpSuccess(JSONObject opResult) {
        if (opResult.has("code")) {
            int retCode = opResult.getInt("code");
            if (retCode == 0 || retCode == -178 || retCode == -4018) { // 178是目录路径冲突, 表示该目录已经存在了,
                                                                       // 4018是相同文件上传
                return true;
            }
        }
        return false;
    }

    // 处理task结果, 打印屏幕，记录日志以及数据库
    private void processTaskResult(TaskResult taskResult) {
        TaskKind taskKind = taskResult.taskKind;
        boolean opSuccess = judgeOpSuccess(taskResult.serverRet);
        String bucketName = taskResult.bucketName;
        String cosPath = taskResult.cosPath;
        String serverRetStr = taskResult.serverRet.toString();
        FileStat fileStat = taskResult.fileStat;

        String opResultTip = "ok";
        if (!opSuccess) {
            opResultTip = "fail";
        }
        String printMsg = String.format("[%4s] [%18s] [appid: %s] [bucket: %s] [cos_path: %s]",
                opResultTip, taskKind.toString(), this.config.getAppid(), bucketName, cosPath);
        String logMsg = String.format(
                "[%4s] [%18s] [appid: %s] [bucket: %s] [cos_path: %s] [server_ret: %s]",
                opResultTip, taskKind.toString(), this.config.getAppid(), bucketName, cosPath,
                serverRetStr);
        System.out.println(printMsg);

        if (opSuccess) {
            LOG.info(logMsg);
            if (taskKind == TaskKind.CreateFolderTask) {
                statics.addCreateFolderOk();
                this.dbRecord.updateRecord(fileStat);
            } else if (taskKind == TaskKind.UploadFileTask) {
                statics.addUploadFileOk();
                this.dbRecord.updateRecord(fileStat);
            } else if (taskKind == TaskKind.DelFolderTask) {
                statics.addDelFolderOk();
                this.dbRecord.delRecord(fileStat);
            } else if (taskKind == TaskKind.DelFileTask) {
                statics.addDelFileOk();
                this.dbRecord.delRecord(fileStat);
            }
        } else {
            LOG.error(logMsg);
            if (taskKind == TaskKind.CreateFolderTask) {
                statics.addCreateFolderFail();
            } else if (taskKind == TaskKind.UploadFileTask) {
                statics.addUploadFileFail();
            } else if (taskKind == TaskKind.DelFolderTask) {
                statics.addDelFolderFail();
            } else if (taskKind == TaskKind.DelFileTask) {
                statics.addDelFileFail();
            }
        }
    }

    // 等待所有的任务执行结束
    private void waitForSyncOver() {
        for (long taskIndex = 0; taskIndex < this.taskCount; ++taskIndex) {
            try {
                Future<TaskResult> future = this.completionService.take();
                TaskResult taskResult = future.get();
                processTaskResult(taskResult);
            } catch (InterruptedException e) {
                LOG.error("watiForSyncOver", e);
            } catch (ExecutionException e) {
                LOG.error("watiForSyncOver", e);
            }
        }
    }

    // 同步的主逻辑
    public void run() throws InterruptedException {
        // 获取daemon 模式的配置
        int daemonMode = this.config.getDaemonMode();
        // 获取任务之间的间隔
        int daemonInterval = this.config.getDaemonInterval();
        while (true) {
            // 开始数据统计
            statics.beginCollectStatics();

            // 生成数据库记录
            buildDbRecord();
            // 生成本地记录
            LocalFileDirInfo localInfo = new LocalFileDirInfo(this.config.getLocalPath());

            // 初始化要删除的文件和目录Map
            HashMap<String, FileStat> delFileStatMap = this.dbRecord.getFileStatMap();
            HashMap<String, FileStat> delFolderStatMap = this.dbRecord.getFolderStatMap();

            // 初始化要上传和创建的文件目录结构
            HashMap<String, FileStat> createFileStatMap = new HashMap<String, FileStat>();
            HashMap<String, FileStat> createFolderStatMap = new HashMap<String, FileStat>();

            // 产生文件的差异, 生成要删除和上传的文件结构
            diff(localInfo.getFileStatList(), delFileStatMap, createFileStatMap);
            // 产生目录的差异, 生成要山川和上传的目录结构
            diff(localInfo.getFolderStatList(), delFolderStatMap, createFolderStatMap);

            // 清空任务数
            clear();
            // 删除文件
            delFile(delFileStatMap);
            // 删除目录
            delFolder(delFolderStatMap);
            // 等待所有删除任务执行完成
            waitForSyncOver();

            // 清空任务数
            clear();
            // 创建目录
            createFolder(createFolderStatMap);
            // 创建文件
            uploadFile(createFileStatMap);
            // 等待所有上传任务执行完成
            waitForSyncOver();

            // 生成统计数据
            statics.endCollectStatics();

            // 如果是damon模式, 则sleep一段时间, 否则退出
            if (daemonMode != 1) {
                break;
            } else {
                Thread.sleep(daemonInterval * 1000);
            }
        }
    }

    public void shutdown() {
        this.executorService.shutdownNow();
        this.cosClient.shutdown();
        this.dbRecord.shutdown();
        statics.shutdown();
    }

}
