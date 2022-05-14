package org.apache.druid.java.util.common.io.smoosh;

import org.apache.druid.java.util.common.StringUtils;

import java.io.File;

public class FileSmoosherExt {

    private static final String FILE_EXTENSION = "smoosh";

    private static final String ENCRYPTION_MARK_FILE = "encryption.mark";

    public static File metaFile(File baseDir)
    {
        return new File(baseDir, StringUtils.format("meta.%s", FILE_EXTENSION));
    }

    public static File makeChunkFile(File baseDir, int i) {
        return new File(baseDir, StringUtils.format("%05d.%s", new Object[]{i, FILE_EXTENSION}));
    }

    public static File encryptionMarkFile(File baseDir) {
        return new File(baseDir, ENCRYPTION_MARK_FILE);
    }
}
