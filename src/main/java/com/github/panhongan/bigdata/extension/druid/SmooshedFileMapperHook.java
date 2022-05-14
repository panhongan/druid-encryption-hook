package com.github.panhongan.bigdata.extension.druid;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosherExt;
import org.apache.druid.java.util.common.io.smoosh.MetadataExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.CipherInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SmooshedFileMapperHook {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmooshedFileMapperHook.class);

    public static final String SMOOSHED_FILE_MAPPER_CLASS_BY_DOT = "org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper";

    public static final String SMOOSHED_FILE_MAPPER_CLASS_BY_SLASH = SMOOSHED_FILE_MAPPER_CLASS_BY_DOT.replace('.', '/');

    public static final String SMOOSHED_FILE_MAPPER_TARGET_METHOD = "load";

    public static byte[] addSmooshedFileMapperHook(byte[] classfileBuffer) {
        try {
            ClassPool classPool = ClassPool.getDefault();
            CtClass ctClass = classPool.get(SMOOSHED_FILE_MAPPER_CLASS_BY_DOT);
            CtMethod ctMethod = ctClass.getDeclaredMethod(SMOOSHED_FILE_MAPPER_TARGET_METHOD);

            String body = "{\n" +
                    "org.apache.druid.java.util.common.Pair pair = " + SmooshedFileMapperHook.class.getName() + ".load($1);\n" +
                    //"java.util.Map/*<String, org.apache.druid.java.util.common.io.smoosh.Metadata>*/ internalFilesMap = new java.util.TreeMap/*<>*/();\n" +
                    //"java.util.Set s = pair.rhs.entrySet();\n" +
                    //"java.util.Iterator it = s.iterator();\n" +
                    //"while (it.hasNext()) {\n" +
                    //"    java.util.Map.Entry entry = (java.util.Map.Entry) it.next();\n" +
                    //"    internalFilesMap.put(entry.getKey(), " + SmooshedFileMapperHook.class.getName() + ".toMetadataExt(entry.getValue()));\n" +
                    //"}\n" +
                    "return new org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper(pair.lhs, pair.rhs);\n" +
                    "}";

            LOGGER.info("New method body for SmooshedFileMapper::load() :\n{}", body);

            ctMethod.setBody(body);

            return ctClass.toBytecode();
        } catch (Throwable t) {
            LOGGER.error("", t);
        }

        return classfileBuffer;
    }

    public static Pair<List<File>, Map<String, MetadataExt>> load(final File baseDir) throws IOException {
        File metaFile = FileSmoosherExt.metaFile(baseDir);

        InputStream inputStream = new FileInputStream(metaFile);

        if (FileSmoosherHook.encryptionMarkFileExists(baseDir)) {
            inputStream = new CipherInputStream(inputStream, AESUtils.getDecryptCipher());
        }

        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

            String line = in.readLine();
            if (line == null) {
                throw new ISE("First line should be version,maxChunkSize,numChunks, got null.");
            }

            String[] splits = line.split(",");
            if (!"v1".equals(splits[0])) {
                throw new ISE("Unknown version[%s], v1 is all I know.", splits[0]);
            }
            if (splits.length != 3) {
                throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
            }
            final Integer numFiles = Integer.valueOf(splits[2]);
            List<File> outFiles = Lists.newArrayListWithExpectedSize(numFiles);

            for (int i = 0; i < numFiles; ++i) {
                outFiles.add(FileSmoosherExt.makeChunkFile(baseDir, i));
            }

            Map<String, MetadataExt> internalFiles = new TreeMap<>();
            while ((line = in.readLine()) != null) {
                splits = line.split(",");

                if (splits.length != 4) {
                    throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
                }
                internalFiles.put(
                        splits[0],
                        new MetadataExt(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[3]))
                );
            }

            return Pair.of(outFiles, internalFiles);
        }
        finally {
            Closeables.close(in, false);
        }
    }
}
