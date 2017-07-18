package com.qcloud.cos.cos_sync.meta;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用于将已经上传成功的文件和目录刷入数据库
 * @author chengwu
 *
 */
public class DbRecord {

    private static final Logger LOG = LoggerFactory.getLogger(DbRecord.class);

    private Connection conn = null;
    private String tableName = "";
    private HashMap<String, FileStat> fileStatMap = new HashMap<String, FileStat>();
    private HashMap<String, FileStat> folderStatMap = new HashMap<String, FileStat>();

    PreparedStatement updatePreStat = null;
    PreparedStatement delPreStat = null;

    public DbRecord(String dbPath, long appid, String bucketName, String localPath,
            String cosPath) {
        try {
            this.conn = DriverManager.getConnection("jdbc:sqlite:./db/db_rec.db");
            String pathMd5 = DigestUtils.md5Hex(localPath + cosPath);
            this.tableName = String.format("cos_sync_table_%d_%s_%s", appid, bucketName, pathMd5);
            String sqlCreateTable = new StringBuilder("create table if not exists ")
                    .append(this.tableName).append(" (`id` INTEGER PRIMARY key autoincrement,")
                    .append(" `file_path` varchar(1024) default '',")
                    .append(" `file_size` bigint not null default 0,")
                    .append(" `file_flag` tinyint not null default 0,")
                    .append(" `mtime` bigint not null,").append(" `md5` char(32) default '',")
                    .append(" unique (`file_path`))").toString();
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sqlCreateTable);

            String sqlSelectAll = "select * from " + this.tableName;
            ResultSet resultSet = stmt.executeQuery(sqlSelectAll);
            while (resultSet.next()) {
                String filePath = resultSet.getString("file_path");
                long fileSize = resultSet.getLong("file_size");
                boolean fileFlag = resultSet.getBoolean("file_flag");
                long mtime = resultSet.getLong("mtime");
                String md5 = resultSet.getString("md5");
                FileStat stat = new FileStat(filePath, fileFlag, fileSize, mtime, md5);
                if (fileFlag) {
                    fileStatMap.put(filePath, stat);
                } else {
                    folderStatMap.put(filePath, stat);
                }
            }

        } catch (SQLException e) {
            LOG.error(e.getMessage());
        }

        initUpdatePreStat();
        initDelPreStat();
    }

    private void initUpdatePreStat() {
        String updateSql = String.format(
                "replace into %s (file_path, file_size, file_flag, mtime, md5) values (?, ?, ?, ?, ?)",
                this.tableName);
        try {
            updatePreStat = conn.prepareStatement(updateSql);
        } catch (SQLException e) {
            LOG.error("init update PreStat error, sql: {}\n, exception:{}", updateSql,
                    e.getMessage());
        }
    }

    private void initDelPreStat() {
        String delSql = String.format("delete from %s where file_path = ?", this.tableName);
        try {
            delPreStat = conn.prepareStatement(delSql);
        } catch (SQLException e) {
            LOG.error("init Del PreStat error, sql: {}\n, exception:{}", delSql, e.getMessage());
        }
    }

    public void updateRecord(FileStat fileStat) {
        if (fileStat.getMd5().isEmpty()) {
            fileStat.buildMd5();
        }

        try {
            updatePreStat.setString(1, fileStat.getFilePath());
            updatePreStat.setLong(2, fileStat.getFileSize());
            updatePreStat.setBoolean(3, fileStat.isFileFlag());
            updatePreStat.setLong(4, fileStat.getMtime());
            updatePreStat.setString(5, fileStat.getMd5());
            updatePreStat.executeUpdate();
        } catch (SQLException e) {
            LOG.error("updateRecord occur a exception", e);
        }
    }

    public void delRecord(FileStat fileStat) {
        try {
            delPreStat.setString(1, fileStat.getFilePath());
            delPreStat.executeUpdate();
        } catch (SQLException e) {
            LOG.error("delRecord occur a exception", e);
        }
    }

    public HashMap<String, FileStat> getFileStatMap() {
        return fileStatMap;
    }

    public HashMap<String, FileStat> getFolderStatMap() {
        return folderStatMap;
    }

    public void shutdown() {
        try {
            this.conn.close();
        } catch (SQLException e) {
            LOG.error("db conn close occur a exception:", e);
        }
    }

}
