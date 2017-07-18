package com.qcloud.cos.cos_sync.main;

import com.qcloud.cos.cos_sync.meta.Config;
import com.qcloud.cos.cos_sync.sync.Sync;


public class CosSyncMain {
    public static void main(String[] args) throws Exception {
        Config config = new Config();
        if (!config.isValidConfig()) {
            System.out.println(config.getInitConfigErr());
            return;
        }
        Sync sync = new Sync(config);
        sync.run();
        sync.shutdown();
    }
}
