package com.github.panhongan.bigdata.extension.druid;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

public class EncryptionConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptionConfig.class);

    private static final EncryptionConfig EMPTY_CONFIG = new EncryptionConfig(false,
            EncryptionCoverage.NO_DATASOURCES,
            Collections.emptySet());

    private static EncryptionConfig instance = EMPTY_CONFIG;

    private final boolean enableEncryption;

    private final EncryptionCoverage encryptionCoverage;

    private final Set<String> encryptedDatasources;

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

    public boolean needEncryption(String datasource) {
        if (!enableEncryption) {
            return false;
        }

        if (encryptionCoverage == EncryptionCoverage.ALL_DATASOURCES) {
            return true;
        }

        if (encryptionCoverage == EncryptionCoverage.PARTIAL_DATASOURCES
                && CollectionUtils.isNotEmpty(encryptedDatasources)
                && encryptedDatasources.contains(datasource)) {
            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return "EncryptionConfig("
                + this.enableEncryption + ", "
                + this.encryptionCoverage + ", "
                + this.encryptedDatasources + ")";
    }

    public static EncryptionConfig getInstance() {
        return instance;
    }

    public static void loadConfigFile(String confFile) {
        try (InputStream inputStream = new FileInputStream(confFile)) {
            Properties properties = new Properties();
            properties.load(inputStream);

            // encryption flag
            boolean encryptionFlg = Boolean.valueOf(properties.getProperty("druid.enable.encryption", "false"));

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

            instance = new EncryptionConfig(encryptionFlg, coverage, datasourceSet);
        } catch (IOException e) {
            LOGGER.error("", e);
            System.exit(1);
        }

        LOGGER.info("Encryption config = {}", instance);
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
            if (Objects.equals(ALL_DATASOURCES.value, value)) {
                return ALL_DATASOURCES;
            }

            if (Objects.equals(NO_DATASOURCES.value, value)) {
                return NO_DATASOURCES;
            }

            return PARTIAL_DATASOURCES;
        }
    }

    public static void main(String[] args) {
        EncryptionConfig.loadConfigFile("conf/encryption.properties");
        System.out.println(EncryptionConfig.getInstance());
    }
}
