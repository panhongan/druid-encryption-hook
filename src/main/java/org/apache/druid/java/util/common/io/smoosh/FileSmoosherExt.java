package org.apache.druid.java.util.common.io.smoosh;

import org.apache.druid.java.util.common.StringUtils;

import java.io.File;

public class FileSmoosherExt {

    private static final String FILE_EXTENSION = "smoosh";

    public static File metaFile(File baseDir)
    {
        return new File(baseDir, StringUtils.format("meta.%s", FILE_EXTENSION));
    }
}
