package com.github.panhongan.bigdata.extension.druid;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

public class EncryptionConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptionConfig.class);

    private static final EncryptionConfig EMPTY_CONFIG = new EncryptionConfig(false, EncryptionCoverage.NO_DATASOURCES, Collections.emptySet());

    private static Map<String, EncryptionConfig> encryptionConfigMap = new HashMap<>();

    static {
        System.out.println(EncryptionConfig.class.getClassLoader().getResource("").getPath());
        parseConfigFile("cluster1");
        parseConfigFile("cluster2");
    }

    private boolean enableEncryption;

    private EncryptionCoverage encryptionCoverage;

    private Set<String> encryptedDatasources;

    public EncryptionConfig(boolean enableEncryption, EncryptionCoverage encryptionCoverage, Set<String> encryptedDatasources) {
        this.enableEncryption = enableEncryption;
        this.encryptionCoverage = encryptionCoverage;
        this.encryptedDatasources = encryptedDatasources;
    }

    public boolean encryptionEnabled() {
        return enableEncryption;
    }

    public EncryptionCoverage getEncryptionCoverage() {
        return encryptionCoverage;
    }

    public final Set<String> getEncryptedDatasources() {
        return ImmutableSet.copyOf(this.encryptedDatasources);
    }

    public boolean needEncryption() {
        return enableEncryption &&
                (encryptionCoverage == EncryptionCoverage.ALL_DATASOURCES ||
                  (encryptionCoverage == EncryptionCoverage.PARTIAL_DATASOURCES && CollectionUtils.isNotEmpty(encryptedDatasources)));
    }

    @Override
    public String toString() {
        return "EncryptionConfig("
                + this.enableEncryption + ", "
                + this.encryptionCoverage + ", "
                + this.encryptedDatasources + ")";
    }

    public static EncryptionConfig getEncryptionConfig(String clusterName) {
        return encryptionConfigMap.getOrDefault(clusterName, EMPTY_CONFIG);
    }

    private static String getResourceFile(String clusterName) {
        return clusterName + "/encryption.properties";
    }

    private static void parseConfigFile(String clusterName) {
        String resourceFile = getResourceFile(clusterName);
        LOGGER.info("parse resource file: {}", resourceFile);

        try (InputStream inputStream = EncryptionConfig.class.getClassLoader().getResourceAsStream(resourceFile)) {
            Properties properties = new Properties();
            properties.load(inputStream);

            // encryption flag
            boolean encryptionFlg = Boolean.getBoolean(properties.getProperty("druid.enable.encryption", "false"));

            // datasource
            Set<String> datasourceSet = new HashSet<>();

            String datasources = properties.getProperty("druid.encrypted.datasources", "none");
            EncryptionCoverage coverage = EncryptionCoverage.getEncryptionCoverage(datasources);
            if (coverage == EncryptionCoverage.PARTIAL_DATASOURCES) {
                String[] arr = datasources.split(",");
                if (ArrayUtils.isNotEmpty(arr)) {
                    for (String datasource : arr) {
                        datasourceSet.add(datasource.trim());
                    }
                }
            }

            EncryptionConfig encryptionConfig = new EncryptionConfig(encryptionFlg, coverage, datasourceSet);
            encryptionConfigMap.put(clusterName, encryptionConfig);

            LOGGER.info("cluster = {}, encryption config = {}", clusterName, encryptionConfig);
        } catch (IOException e) {
            LOGGER.error("", e);
            System.exit(1);
        }
    }

    public enum EncryptionCoverage {
        ALL_DATASOURCES("all"),
        NO_DATASOURCES("none"),
        PARTIAL_DATASOURCES("partial");

        private final String value;

        EncryptionCoverage(String value) {
            this.value = value;
        }

        public static EncryptionCoverage getEncryptionCoverage(String value) {
            if (Objects.equals(ALL_DATASOURCES.value, "all")) {
                return ALL_DATASOURCES;
            }

            if (Objects.equals(NO_DATASOURCES.value, "none")) {
                return NO_DATASOURCES;
            }

            return PARTIAL_DATASOURCES;
        }
    }
}
