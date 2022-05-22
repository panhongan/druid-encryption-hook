package com.github.panhongan.bigdata.extension.druid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.Instrumentation;

/**
 * -javaagent:xxx/xxx/druid-encryption-hook-1.0.jar:xxx/xxx/encryption.properties
 */
public class DruidFileEncryptionAgent {

    private static final Logger LOGGER = LoggerFactory.getLogger(DruidFileEncryptionAgent.class);

    /**
     * @param arg cluaster name, eg: iad, pii, timeline
     * @param instrumentation
     */
    public static void premain(String arg, Instrumentation instrumentation) {
        LOGGER.info("encryption config file: {}", arg);
        System.out.println("encryption config file: " + arg);
        EncryptionConfig.loadConfigFile(arg);

        instrumentation.addTransformer(new DruidFileEncryptionTransformer());
    }
}
