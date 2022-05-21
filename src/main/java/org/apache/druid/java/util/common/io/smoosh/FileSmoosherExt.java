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

    public static String getDatasourceByPersistOrMergePath(final File persistOrMergeDir) {
        String[] arr = persistOrMergeDir.getAbsolutePath().split("/");
        String segmentId = arr[arr.length - 2];
        String datasource = getDatasourceBySegmentId(segmentId);
        return datasource;
    }

    public static String getDatasourceBySmooshFilePath(final File smooshFile) {
        String[] arr = smooshFile.getAbsolutePath().split("/");
        String segmentId = arr[arr.length - 3];
        String datasource = getDatasourceBySegmentId(segmentId);
        return datasource;
    }

    public static String getDatasourceBySegmentId(String segmentId) {
        int fromIndex = segmentId.length() - 1;
        int pos = -1;

        for (int i = 0; i < 3; ++i) {
            pos = segmentId.lastIndexOf('_', fromIndex);
            if (pos != -1) {
                fromIndex = pos - 1;
            }
        }

        if (pos != -1) {
            return segmentId.substring(0, pos);
        }

        throw new RuntimeException("Invalid segmentId: " + segmentId);
    }
}
