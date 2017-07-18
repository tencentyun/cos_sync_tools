package com.qcloud.cos.cos_sync.meta;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 扫描目录的子成员, 并将文件和目录分别放在对应的list中
 * 
 * @author chengwu
 *
 */
public class LocalFileDirInfo {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileDirInfo.class);
    private ArrayList<FileStat> fileStatList = new ArrayList<FileStat>();
    private ArrayList<FileStat> folderStatList = new ArrayList<FileStat>();

    public LocalFileDirInfo(String localPath) {
        SimpleFileVisitor<Path> finder = new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException {
                String dirPath = dir.toString();
                // 格式化dirpath, 确保以/结尾, 同时替换其中的\分隔符
                dirPath = dirPath.replace('\\', '/');
                if (!dirPath.endsWith("/")) {
                    dirPath += "/";
                }
                folderStatList.add(new FileStat(dirPath));
                return super.preVisitDirectory(dir, attrs);
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                String filePath = file.toString();
                // 格式化filepath, 替换其中的\分隔符
                filePath = filePath.replace('\\', '/');
                fileStatList.add(new FileStat(filePath));
                return super.visitFile(file, attrs);
            }
        };

        try {
            java.nio.file.Files.walkFileTree(Paths.get(localPath), finder);
        } catch (IOException e) {
            LOG.error("walk file tree error", e);
        }
    }

    // 获取文件列表
    public ArrayList<FileStat> getFileStatList() {
        return fileStatList;
    }

    // 获取目录列表
    public ArrayList<FileStat> getFolderStatList() {
        return folderStatList;
    }

}
