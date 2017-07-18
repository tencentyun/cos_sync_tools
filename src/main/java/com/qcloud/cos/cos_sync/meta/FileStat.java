package com.qcloud.cos.cos_sync.meta;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * 封装有关文件的相关信息, 包括文件路径，是否为目录, 文件大小, 修改时间和md5值
 * @author chengwu
 *
 */
public class FileStat {
    private String filePath = "";
    private File fileObj = null;
    private boolean fileFlag = true;
    private long fileSize = 0;
    private long mtime = 0;
    private String md5 = "";

    public FileStat(String filePath) {
        this.filePath = filePath;
        this.fileObj = new File(filePath);
        this.fileFlag = this.fileObj.isFile();
        this.fileSize = this.fileObj.length();
        this.mtime = this.fileObj.lastModified();
    }


    public FileStat(String filePath, boolean fileFlag, long fileSize, long mtime, String md5) {
        super();
        this.filePath = filePath;
        this.fileFlag = fileFlag;
        this.fileSize = fileSize;
        this.mtime = mtime;
        this.md5 = md5;
    }

    public void buildMd5() {
        if (!this.fileFlag) {
            this.md5 = "";
            return;
        }

        FileInputStream fileIn = null;
        try {
            fileIn = new FileInputStream(this.fileObj);
            this.md5 = DigestUtils.md5Hex(fileIn);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileIn != null) {
                try {
                    fileIn.close();
                } catch (Exception e) {
                }
            }
        }
    }

    public boolean isSame(FileStat comparedStat) {
        if (!this.fileFlag) {
            return true;
        }

        if (this.fileSize != comparedStat.fileSize) {
            return false;
        }

        if (this.mtime == comparedStat.mtime) {
            return true;
        }

        buildMd5();
        if (!this.md5.equals(comparedStat.md5)) {
            return false;
        }

        return true;
    }


    public String getFilePath() {
        return filePath;
    }


    public boolean isFileFlag() {
        return fileFlag;
    }


    public long getFileSize() {
        return fileSize;
    }


    public long getMtime() {
        return mtime;
    }


    public String getMd5() {
        return md5;
    }

}
