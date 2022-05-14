package com.github.panhongan.bigdata.extension.druid;

import com.google.common.base.Joiner;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosherExt;
import org.apache.druid.java.util.common.io.smoosh.MetadataExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class FileSmoosherHookUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSmoosherHookUtils.class);

    private static final Joiner JOINER = Joiner.on(",");

    public static final String FILE_SMOOSHER_CLASS_BY_DOT = "org.apache.druid.java.util.common.io.smoosh.FileSmoosher";

    public static final String FILE_SMOOSHER_CLASS_BY_SLASH = FILE_SMOOSHER_CLASS_BY_DOT.replace('.', '/');

    public static final String FILE_SMOOSHER_TARGET_METHOD = "close";

    public static byte[] addFileSmoosherHook(byte[] classfileBuffer) {
        try {
            ClassPool classPool = ClassPool.getDefault();
            CtClass ctClass = classPool.get(FILE_SMOOSHER_CLASS_BY_DOT);
            CtMethod ctMethod = ctClass.getDeclaredMethod(FILE_SMOOSHER_TARGET_METHOD);

            ctMethod.setBody("{" +
                    "Map<String, MetadataExt> internalFilesExtMap = new TreeMap<>();" +
                    "for (Map.Entry<String, Metadata> entry : internalFiles.entrySet()) {" +
                        "internalFilesExtMap.put(entry.getKey(), MetadataExt.toMetadataExt(entry.getValue()));" +
                    "};" +
                    FileSmoosherHookUtils.class.getName() + ".close(completedFiles, filesInProcess, currOut, baseDir, maxChunkSize, outFiles, internalFilesExtMap);" +
            "}");

            return ctClass.toBytecode();
        } catch (Throwable t) {
            LOGGER.error("", t);
        }

        return classfileBuffer;
    }

    public static void close(final List<File> completedFiles,
                             final List<File> filesInProcess,
                             final FileSmoosher.Outer currOut,
                             final File baseDir,
                             int maxChunkSize,
                             final List<File> outFiles,
                             final Map<String, MetadataExt> internalFilesExtMap) throws IOException {
        if (!completedFiles.isEmpty() || !filesInProcess.isEmpty()) {
            for (File file : completedFiles) {
                if (!file.delete()) {
                    LOGGER.warn("Unable to delete file [%s]", file);
                }
            }
            for (File file : filesInProcess) {
                if (!file.delete()) {
                    LOGGER.warn("Unable to delete file [%s]", file);
                }
            }
            throw new ISE(
                    "[%d] writers in progress and [%d] completed writers needs to be closed before closing smoosher.",
                    filesInProcess.size(), completedFiles.size()
            );
        }

        if (currOut != null) {
            currOut.close();
        }

        File metaFile = FileSmoosherExt.metaFile(baseDir);

        try (Writer out =
                     new BufferedWriter(new OutputStreamWriter(new FileOutputStream(metaFile), StandardCharsets.UTF_8))) {
            out.write(StringUtils.format("v1,%d,%d", maxChunkSize, outFiles.size()));
            out.write("\n");

            for (Map.Entry<String, MetadataExt> entry : internalFilesExtMap.entrySet()) {
                final MetadataExt metadata = entry.getValue();
                out.write(
                        JOINER.join(
                                entry.getKey(),
                                metadata.getFileNum(),
                                metadata.getStartOffset(),
                                metadata.getEndOffset()
                        )
                );
                out.write("\n");
            }
        }

        LOGGER.info("Write meta file succeed: {}", metaFile.getName());
    }
}
