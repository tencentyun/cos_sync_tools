package com.qcloud.cos.cos_sync.meta;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用于统计各类任务的数量，并将每次执行的任务统计结果打印到本机, 并写入数据库
 * @author chengwu
 *
 */
public class TaskStatics {
    private static final Logger LOG = LoggerFactory.getLogger(TaskStatics.class);
    
    private Connection conn = null;
    private PreparedStatement updatePrepareStat = null;
    private DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
    private DateFormat timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private long startTime = 0;         // 同步开始时间  UNIX时间戳
    private long endTime = 0;           // 同步结束时间 UNIX时间戳
    
    // 创建目录的总量, 成功量, 失败量
    private AtomicLong createFolderSumCnt = new AtomicLong(0L);
    private AtomicLong createFolderOkCnt = new AtomicLong(0L);
    private AtomicLong createFolderFailCnt = new AtomicLong(0L);
    // 上传文件的总量, 成功量, 失败量
    private AtomicLong uploadFileSumCnt = new AtomicLong(0L);
    private AtomicLong uploadFileOkCnt = new AtomicLong(0L);
    private AtomicLong uploadFileFailCnt = new AtomicLong(0L);
    // 删除目录的总量, 成功量，失败量
    private AtomicLong delFolderSumCnt = new AtomicLong(0L);
    private AtomicLong delFolderOkCnt = new AtomicLong(0L);
    private AtomicLong delFolderFailCnt = new AtomicLong(0L);
    // 删除文件的总量, 成功量, 失败量
    private AtomicLong delFileSumCnt = new AtomicLong(0L);
    private AtomicLong delFileOkCnt = new AtomicLong(0L);
    private AtomicLong delFileFailCnt = new AtomicLong(0L);

    // 统计的单例
    private static final TaskStatics instance = new TaskStatics();

    public static TaskStatics getInstance() {
        return instance;
    }

    private TaskStatics() {
        // 创建记录统计结果的数据表
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("create table if not exists op_record")
                .append("(`id` INTEGER PRIMARY key autoincrement,\n").append("`op_date` Integer,\n")
                .append("`start_time` bigint,\n").append("`end_time` bigint,\n")
                .append("`op_sum_cnt` bigint,\n").append("`op_ok_cnt` bigint,\n")
                .append("`op_fail_cnt` bigint,\n")
                .append("`op_status`  varchar(20) NOT NULL DEFAULT 'ALL_OK',\n")
                .append("`create_folder_sum_cnt` bigint, \n")
                .append("`create_folder_ok_cnt` bigint,\n")
                .append("`create_folder_fail_cnt` bigint,\n")
                .append("`upload_file_sum_cnt` bigint,\n").append("`upload_file_ok_cnt` bigint,\n")
                .append("`upload_file_fail_cnt` bigint,\n").append("`del_folder_sum_cnt` bigint,\n")
                .append("`del_folder_ok_cnt` bigint,\n").append("`del_folder_fail_cnt` bigint,\n")
                .append("`del_file_sum_cnt` bigint,\n").append("`del_file_ok_cnt` bigint,\n")
                .append("`del_file_fail_cnt` bigint)");
        String sqlCreateOpTable = stringBuffer.toString();
        try {
            this.conn = DriverManager.getConnection("jdbc:sqlite:./db/op_record.db");
            Statement createTableStat = this.conn.createStatement();
            createTableStat.executeUpdate(sqlCreateOpTable);
            createTableStat.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

        initUpdateRecordPreStat();
    }

    // 初始化更新记录的prestat
    private void initUpdateRecordPreStat() {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("insert into op_record ")
                .append("(op_date, start_time, end_time, op_sum_cnt, op_ok_cnt, op_fail_cnt, ")
                .append("create_folder_sum_cnt, create_folder_ok_cnt, ")
                .append("create_folder_fail_cnt, upload_file_sum_cnt, upload_file_ok_cnt, ")
                .append("upload_file_fail_cnt, del_folder_sum_cnt, del_folder_ok_cnt, ")
                .append("del_folder_fail_cnt, del_file_sum_cnt, del_file_ok_cnt, del_file_fail_cnt, op_status)")
                .append(" values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        try {
            this.updatePrepareStat = this.conn.prepareStatement(stringBuffer.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    // 更新记录
    private void updateRecord() {
        Date date = new Date(this.startTime);
        int opDate = new Integer(this.dateFormatter.format(date)).intValue();
        long opSumCnt = this.createFolderSumCnt.get() + this.delFolderSumCnt.get()
                + this.uploadFileSumCnt.get() + this.delFileSumCnt.get();
        long opOkCnt = this.createFolderOkCnt.get() + this.delFolderOkCnt.get()
                + this.uploadFileOkCnt.get() + this.delFileOkCnt.get();
        long opFailCnt = this.createFolderFailCnt.get() + this.delFolderFailCnt.get()
                + this.uploadFileFailCnt.get() + this.delFileFailCnt.get();
        String opStatus = "";
        if (opOkCnt == opSumCnt) {
            opStatus = "ALL_OK";
        } else if (opFailCnt == opSumCnt) {
            opStatus = "ALL_FAIL";
        } else {
            opStatus = "PART_OK";
        }

        try {
            this.updatePrepareStat.setInt(1, opDate);
            this.updatePrepareStat.setLong(2, this.startTime / 1000);
            this.updatePrepareStat.setLong(3, this.endTime / 1000);
            this.updatePrepareStat.setLong(4, opSumCnt);
            this.updatePrepareStat.setLong(5, opOkCnt);
            this.updatePrepareStat.setLong(6, opFailCnt);
            this.updatePrepareStat.setLong(7, this.createFolderSumCnt.get());
            this.updatePrepareStat.setLong(8, this.createFolderOkCnt.get());
            this.updatePrepareStat.setLong(9, this.createFolderFailCnt.get());
            this.updatePrepareStat.setLong(10, this.uploadFileSumCnt.get());
            this.updatePrepareStat.setLong(11, this.uploadFileOkCnt.get());
            this.updatePrepareStat.setLong(12, this.uploadFileFailCnt.get());
            this.updatePrepareStat.setLong(13, this.delFolderSumCnt.get());
            this.updatePrepareStat.setLong(14, this.delFolderOkCnt.get());
            this.updatePrepareStat.setLong(15, this.delFolderFailCnt.get());
            this.updatePrepareStat.setLong(16, this.delFileSumCnt.get());
            this.updatePrepareStat.setLong(17, this.delFileOkCnt.get());
            this.updatePrepareStat.setLong(18, this.delFileFailCnt.get());
            this.updatePrepareStat.setString(19, opStatus);
            this.updatePrepareStat.executeUpdate();

            String printStr = "\n\nsync over! op statistics:";
            System.out.println(printStr);
            LOG.info(printStr);
            printStr = String.format("%30s : %s", "op_status", opStatus);
            System.out.println(printStr);
            LOG.info(printStr);
            printStr = String.format("%30s : %d", "create_folder_ok", this.createFolderOkCnt.get());
            System.out.println(printStr);
            LOG.info(printStr);
            printStr = String.format("%30s : %d", "create_folder_fail",
                    this.createFolderFailCnt.get());
            System.out.println(printStr);
            LOG.info(printStr);
            printStr = String.format("%30s : %d", "del_folder_ok", this.delFolderOkCnt.get());
            System.out.println(printStr);
            LOG.info(printStr);
            printStr = String.format("%30s : %d", "del_folder_fail", this.delFolderFailCnt.get());
            System.out.println(printStr);
            LOG.info(printStr);
            printStr = String.format("%30s : %d", "upload_file_ok", this.uploadFileOkCnt.get());
            System.out.println(printStr);
            LOG.info(printStr);
            printStr = String.format("%30s : %d", "upload_file_fail", this.uploadFileFailCnt.get());
            System.out.println(printStr);
            LOG.info(printStr);
            printStr = String.format("%30s : %d", "del_file_ok", this.delFileOkCnt.get());
            System.out.println(printStr);
            LOG.info(printStr);
            printStr = String.format("%30s : %d", "del_file_fail", this.delFileFailCnt.get());
            System.out.println(printStr);
            LOG.info(printStr);
            printStr = String.format("%30s : %s", "start_time",
                    this.timeFormatter.format(new Date(this.startTime)));
            System.out.println(printStr);
            LOG.info(printStr);
            printStr = String.format("%30s : %s", "end_time",
                    this.timeFormatter.format(new Date(this.endTime)));
            System.out.println(printStr);
            LOG.info(printStr);
            printStr =
                    String.format("%30s : %d s", "used_time", (this.endTime - this.startTime) / 1000);
            System.out.println(printStr);
            LOG.info(printStr);
        } catch (SQLException e) {
            LOG.error("updatePrepareStat occur a exception", e);
        }

    }

    public void addCreateFolderOk() {
        this.createFolderSumCnt.incrementAndGet();
        this.createFolderOkCnt.incrementAndGet();
    }

    public void addCreateFolderFail() {
        this.createFolderSumCnt.incrementAndGet();
        this.createFolderFailCnt.incrementAndGet();
    }

    public void addUploadFileOk() {
        this.uploadFileSumCnt.incrementAndGet();
        this.uploadFileOkCnt.incrementAndGet();
    }

    public void addUploadFileFail() {
        this.uploadFileSumCnt.incrementAndGet();
        this.uploadFileFailCnt.incrementAndGet();
    }

    public void addDelFolderOk() {
        this.delFolderSumCnt.incrementAndGet();
        this.delFolderOkCnt.incrementAndGet();
    }

    public void addDelFolderFail() {
        this.delFolderSumCnt.incrementAndGet();
        this.delFolderFailCnt.incrementAndGet();
    }

    public void addDelFileOk() {
        this.delFileSumCnt.incrementAndGet();
        this.delFileOkCnt.incrementAndGet();
    }

    public void addDelFileFail() {
        this.delFileSumCnt.incrementAndGet();
        this.delFileFailCnt.incrementAndGet();
    }

    // 用于在每轮任务开始时初始化统计数据
    public void beginCollectStatics() {
        startTime = System.currentTimeMillis();
        this.createFolderSumCnt.set(0L);
        this.createFolderOkCnt.set(0L);
        this.createFolderFailCnt.set(0L);
        this.uploadFileSumCnt.set(0L);
        this.uploadFileOkCnt.set(0L);
        this.uploadFileFailCnt.set(0L);
        this.delFolderSumCnt.set(0L);
        this.delFolderOkCnt.set(0L);
        this.delFolderFailCnt.set(0L);
        this.delFileSumCnt.set(0L);
        this.delFileOkCnt.set(0L);
        this.delFileFailCnt.set(0L);
    }

    // 结束数据统计,将数据刷入表中
    public void endCollectStatics() {
        this.endTime = System.currentTimeMillis();
        updateRecord();
    }

    // 关闭连接
    public void shutdown() {
        try {
            this.conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
