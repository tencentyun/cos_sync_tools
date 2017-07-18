package com.qcloud.cos.cos_sync.meta;

import java.io.File;
import java.io.FileInputStream;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * 同步工具的配置类, 从文件中读取配置, 并进行检查, 配置为JSON格式
 * @author chengwu
 *
 */
public class Config {
    private final String configPath = "./conf/config.json";
    private boolean initConfigFlag = true;
    private String initConfigErr = "";

    private JSONObject configJson = null;

    public Config() {
        super();
        init();
    }

    private void init() {
        File configFile = new File(this.configPath);
        if (!configFile.exists()) {
            this.initConfigFlag = false;
            this.initConfigErr = String.format("config file %s not exist", this.configPath);
            return;
        }

        if (!configFile.isFile()) {
            this.initConfigFlag = false;
            this.initConfigErr =
                    String.format("config file %s is not regular file", this.configPath);
            return;
        }

        FileInputStream configIn = null;
        String configContent = "";
        try {
            configIn = new FileInputStream(configFile);
            int size = configIn.available();
            byte[] buffer = new byte[size];
            configIn.read(buffer);
            configContent = new String(buffer, "UTF-8");
        } catch (Exception e) {
            this.initConfigFlag = false;
            this.initConfigErr = String.format("read config file get exception:%s", e.getMessage());
            return;
        } finally {
            try {
                if (configIn != null) {
                    configIn.close();
                }
            } catch (Exception e) {
            }
        }

        try {
            this.configJson = new JSONObject(configContent);
        } catch (JSONException e) {
            this.initConfigFlag = false;
            this.initConfigErr = "config file is invalid json";
            return;
        }

        String[] validConfigKeyArry = {"appid", "secret_id", "secret_key", "bucket", "region",
                "timeout", "local_path", "cos_path", "thread_num", "delete_sync", "daemon_mode",
                "daemon_interval", "enable_https"};

        for (String validKey : validConfigKeyArry) {
            if (!this.configJson.has(validKey)) {
                this.initConfigFlag = false;
                this.initConfigErr = String.format("config file not contain %s", validKey);
                return;
            }
            try {
                this.configJson.getString(validKey);
            } catch (JSONException e) {
                this.initConfigFlag = false;
                this.initConfigErr = String.format("wrong config, %s value must be string", validKey);
                return;
            }
        }

        if (!checkAppid() || !checkLocalPath() || !checkCosPath() || !checkDaemonMode()
                || !checkDaemonInterval() || !checkDeleteSync() || !checkTimeOut()
                || !checkEnableHttps() || !checkThreadNum()) {
            return;
        }

        formatLocalPath();
        formatCosPath();
        formatDbPath();

        this.initConfigFlag = true;
    }


    /**
     * check APPID, TimeOut等配置是否正确
     * 
     * @return 配置正确返回True, 否则False
     */
    private boolean checkAppid() {
        return checkValueIntStr("appid");
    }

    private boolean checkTimeOut() {
        return checkValueIntStr("timeout");
    }
    
    private boolean checkThreadNum() {
        return checkValueIntStr("thread_num");
    }

    private boolean checkDeleteSync() {
        return checkValueIntStr("delete_sync");
    }

    private boolean checkDaemonMode() {
        return checkValueIntStr("daemon_mode");
    }

    private boolean checkDaemonInterval() {
        return checkValueIntStr("daemon_interval");
    }

    private boolean checkEnableHttps() {
        return checkValueIntStr("enable_https");
    }

    private boolean checkValueIntStr(String key) {
        try {
            Integer.valueOf(this.configJson.getString(key));
            return true;
        } catch (NumberFormatException e) {
            this.initConfigFlag = false;
            this.initConfigErr = String.format("wrong config, %s is illegal", key);
            return false;
        }
    }

    private boolean checkLocalPath() {
        String localPath = this.configJson.getString("local_path");
        File localPathDir = new File(localPath);
        if (!localPathDir.exists()) {
            this.initConfigFlag = false;
            this.initConfigErr = String.format("wrong config, %s not exist!", localPath);
            return false;
        }

        if (!localPathDir.isDirectory()) {
            this.initConfigFlag = false;
            this.initConfigErr = String.format("wrong config, %s is not dir!", localPath);
            return false;
        }
        return true;
    }

    private boolean checkCosPath() {
        String cosPath = this.configJson.getString("cos_path");
        if (!cosPath.startsWith("/")) {
            this.initConfigFlag = false;
            this.initConfigErr = "wrong config, cos_path must start with bucket root /";
            return false;
        }
        return true;
    }


    // 格式化cos path
    private void formatCosPath() {
        String cosPath = this.configJson.getString("cos_path");
        if (!cosPath.endsWith("/")) {
            cosPath += "/";
        }
        this.configJson.put("cos_path", cosPath);
    }

    // 格式化local_path
    private void formatLocalPath() {
        String localPath = this.configJson.getString("local_path");
        try {
            String formatPath = new File(localPath).getCanonicalPath();
            // 如果是windows的\\分隔符, 则换为/
            formatPath = formatPath.replace("\\", "/");
            if (!formatPath.endsWith("/")) {
                formatPath += "/";
            }
            this.configJson.put("local_path", formatPath);
        } catch (Exception e) {
        }
    }

    // db path固定为db_rec.db
    private void formatDbPath() {
        this.configJson.put("db_path", "./db/db_rec.db");
    }

    public boolean isValidConfig() {
        return initConfigFlag;
    }

    public String getInitConfigErr() {
        return initConfigErr;
    }

    public String getAppid() {
        return this.configJson.getString("appid");
    }

    public String getSecretId() {
        return this.configJson.getString("secret_id");
    }

    public String getSecretKey() {
        return this.configJson.getString("secret_key");
    }

    public String getBucket() {
        return this.configJson.getString("bucket");
    }

    public String getLocalPath() {
        return this.configJson.getString("local_path");
    }

    public String getCosPath() {
        return this.configJson.getString("cos_path");
    }

    public int getTimeOut() {
        return getIntValue("timeout");
    }

    public int getThreadNum() {
        return getIntValue("thread_num");
    }

    public int getDeleteSync() {
        return getIntValue("delete_sync");
    }

    public int getDaemonMode() {
        return getIntValue("daemon_mode");
    }
    
    public int getDaemonInterval() {
        return getIntValue("daemon_interval");
    }

    public int getEnableHttps() {
        return getIntValue("enable_https");
    }

    public String getDbPath() {
        return this.configJson.getString("db_path");
    }

    public String getRegion() {
        return this.configJson.getString("region");
    }

    private int getIntValue(String key) {
        int keyValue = new Integer(this.configJson.getString(key)).intValue();
        return keyValue;
    }
}
