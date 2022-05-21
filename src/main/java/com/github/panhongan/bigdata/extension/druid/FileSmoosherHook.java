package com.github.panhongan.bigdata.extension.druid;

import com.google.common.base.Joiner;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtMethod;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosherExt;
import org.apache.druid.java.util.common.io.smoosh.MetadataExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FileSmoosherHook {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSmoosherHook.class);

    public static final String FILE_SMOOSHER_CLASS_BY_DOT = "org.apache.druid.java.util.common.io.smoosh.FileSmoosher";

    public static final String FILE_SMOOSHER_CLASS_BY_SLASH = FILE_SMOOSHER_CLASS_BY_DOT.replace('.', '/');

    public static final String FILE_SMOOSHER_TARGET_METHOD = "close";

    public static final String FILE_SMOOSHER_OUTER_CLASS_BY_DOT = "org.apache.druid.java.util.common.io.smoosh.FileSmoosher.Outer";

    public static final String FILE_SMOOSHER_OUTER_CLASS_BY_SLASH = FILE_SMOOSHER_OUTER_CLASS_BY_DOT.replace('.', '/');

    public static int getEncryptionPrefixLen() throws IOException {
        return generateEncryptedPrefix().length;
    }

    public static byte[] addFileSmoosherHook(byte[] classfileBuffer) {
        try {
            ClassPool classPool = ClassPool.getDefault();
            CtClass ctClass = classPool.get(FILE_SMOOSHER_CLASS_BY_DOT);
            CtMethod ctMethod = ctClass.getDeclaredMethod(FILE_SMOOSHER_TARGET_METHOD);

            String body = "{\n" +
                    "java.util.Map/*<String, org.apache.druid.java.util.common.io.smoosh.MetadataExt>*/ internalFilesExtMap = new java.util.TreeMap/*<>*/();\n" +
                    "java.util.Set s = internalFiles.entrySet();\n" +
                    "java.util.Iterator it = s.iterator();\n" +
                    "while (it.hasNext()) {\n" +
                    "    java.util.Map.Entry entry = (java.util.Map.Entry) it.next();\n" +
                    "    internalFilesExtMap.put(entry.getKey(), " + FileSmoosherHook.class.getName() + ".toMetadataExt(entry.getValue()));\n" +
                    "}\n" +
                    FileSmoosherHook.class.getName() + ".close(completedFiles, filesInProcess, currOut, baseDir, maxChunkSize, outFiles, internalFilesExtMap);\n" +
                    "}";

            LOGGER.info("New method body for FileSmoosher::close() :\n{}", body);

            ctMethod.setBody(body);

            return ctClass.toBytecode();
        } catch (Throwable t) {
            LOGGER.error("", t);
        }

        return classfileBuffer;
    }

    public static byte[] addFileSmoosherOuterHook(byte[] classfileBuffer) {
        try {
            ClassPool classPool = ClassPool.getDefault();
            CtClass ctClass = classPool.get(FILE_SMOOSHER_OUTER_CLASS_BY_DOT);

            // constructor
            CtConstructor ctConstructor = ctClass.getDeclaredConstructor(new CtClass[] {CtClass.intType, classPool.get(File.class.getName()), CtClass.intType});

            String insertAfterCode = "{\n" +
                    FileSmoosherHook.class.getName() + ".writeEncryptedPrefix($0.outFile, $0.channel);\n" +
                    "}";

            LOGGER.info("insertAfter code for FileSmoosher.Outer::Outer() :\n{}", insertAfterCode);

            ctConstructor.insertAfter(insertAfterCode);

            return ctClass.toBytecode();
        } catch (Throwable t) {
            LOGGER.error("", t);
        }

        return classfileBuffer;
    }

    public static MetadataExt toMetadataExt(Object metadataObj) {
        return MetadataExt.toMetadataExt(metadataObj);
    }

    /**
     * hook implementation
     *
     * @param completedFiles
     * @param filesInProcess
     * @param currOut
     * @param baseDir
     * @param maxChunkSize
     * @param outFiles
     * @param internalFilesExtMap
     * @throws IOException
     */
    public static void close(final List<File> completedFiles,
                             final List<File> filesInProcess,
                             final FileSmoosher.Outer currOut,
                             final File baseDir,
                             int maxChunkSize,
                             final List<File> outFiles,
                             final Map<Object, Object> internalFilesExtMap) throws IOException {
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

        writeMetaSmooshFile(baseDir, maxChunkSize, outFiles.size(), internalFilesExtMap);
    }

    private static void writeMetaSmooshFile(final File baseDir,
                                              int maxChunkSize,
                                              int fileNum,
                                              final Map<Object, Object> internalFilesExtMap) throws IOException {
        boolean needEncryption = needEncryption(FileSmoosherExt.getDatasourceByPersistOrMergePath(baseDir));
        int encryptionPrefixLen = 0;

        File metaFile = FileSmoosherExt.metaFile(baseDir);
        OutputStream outputStream = new FileOutputStream(metaFile);
        if (needEncryption) {
            outputStream = new CipherOutputStream(outputStream, AESUtils.getEncryptCipher());
            encryptionPrefixLen = getEncryptionPrefixLen();
        }

        try (Writer out = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
            out.write(StringUtils.format("v1,%d,%d", maxChunkSize, fileNum));
            out.write("\n");

            for (Map.Entry<Object, Object> entry : internalFilesExtMap.entrySet()) {
                final MetadataExt metadata = (MetadataExt) entry.getValue();
                out.write(
                        Joiner.on(",").join(
                                entry.getKey(),
                                metadata.getFileNum(),
                                metadata.getStartOffset() - encryptionPrefixLen,
                                metadata.getEndOffset() - encryptionPrefixLen
                        )
                );
                out.write("\n");
            }
        }

        LOGGER.info("Write meta file succeed: {}", metaFile.getAbsolutePath());

        // create encryption mark file
        if (needEncryption) {
            createEncryptionMarkFile(baseDir);
        }
    }

    private static void createEncryptionMarkFile(final File baseDir) throws IOException {
        File encryptionMarkFile = FileSmoosherExt.encryptionMarkFile(baseDir);
        try (FileOutputStream markFileOutputStream = new FileOutputStream(encryptionMarkFile)) {
            // write nothing
        }

        LOGGER.info("Write encryption mark succeed: {}", encryptionMarkFile.getAbsolutePath());
    }

    public static boolean encryptionMarkFileExists(final File baseDir) {
        File encryptionMarkFile = FileSmoosherExt.encryptionMarkFile(baseDir);
        return encryptionMarkFile.exists();
    }

    private static boolean needEncryption(String datasource) throws IOException {
        String clusterName = DruidFileEncryptionAgent.AgentArg.getInstance().getClusterName();
        EncryptionConfig encryptionConfig = EncryptionConfig.getEncryptionConfig(clusterName);
        LOGGER.info("cluster name: {}, encryption config = {}", clusterName, encryptionConfig);
        return encryptionConfig.needEncryption(datasource);
    }

    private static byte[] generateEncryptedPrefix() throws IOException {
        String md5 = DigestUtils.md5Hex(String.valueOf(System.currentTimeMillis())); // 32 bytes
        return AESUtils.encrypt(md5.getBytes(StandardCharsets.UTF_8)); // 32 bytes
    }

    /**
     * hook implementation
     *
     * @param smooshFile like 00000.smoosh
     * @param channel
     * @throws Exception
     */
    public static void writeEncryptedPrefix(final File smooshFile, final GatheringByteChannel channel) throws Exception {
        String datasource = FileSmoosherExt.getDatasourceBySmooshFilePath(smooshFile);
        if (needEncryption(datasource)) {
            byte[] output = generateEncryptedPrefix();
            channel.write(ByteBuffer.wrap(output));
        }
    }

    public static void main(String[] args) throws IOException {
        File baseDir = new File("/Users/hpan/work/code/panhongan/test-java/druid-encryption-hook/persist");

        testWriteMetaFile(baseDir);
        testDecryptMetaSmooshFile(baseDir);
    }

    public static void testWriteMetaFile(final File baseDir) throws IOException {
        Map<Object, Object> map = new TreeMap<>();
        map.put("__time", new MetadataExt(0, 0, 12754));
        map.put("clientId", new MetadataExt(0, 14647, 7017533));
        map.put("customerId", new MetadataExt(0, 12754, 14647));
        map.put("index.drd", new MetadataExt(0, 349393489, 349393813));
        map.put("metadata.drd", new MetadataExt(0, 349393813, 349393983));
        map.put("sessionId", new MetadataExt(0, 7017533, 12275470));
        map.put("sessionStartTimeMs", new MetadataExt(0, 12275470, 17710478));
        map.put("version", new MetadataExt(0, 17710478, 17712366));
        map.put("videoSessionJson", new MetadataExt(0, 17712366, 349393489));

        writeMetaSmooshFile(baseDir, Integer.MAX_VALUE, 1, map);

        System.out.println(encryptionMarkFileExists(baseDir));
    }

    public static void testDecryptMetaSmooshFile(final File baseDir) throws IOException {
        File metaFile = FileSmoosherExt.metaFile(baseDir);
        InputStream inputStream = new FileInputStream(metaFile);

        if (encryptionMarkFileExists(baseDir)) {
            inputStream = new CipherInputStream(inputStream, AESUtils.getDecryptCipher());
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
    }
}
