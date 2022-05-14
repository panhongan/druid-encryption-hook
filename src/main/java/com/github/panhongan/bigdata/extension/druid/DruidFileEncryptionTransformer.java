package com.github.panhongan.bigdata.extension.druid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class DruidFileEncryptionTransformer implements ClassFileTransformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DruidFileEncryptionTransformer.class);

    private static Map<String, Function<byte[], byte[]>> hookMap = new HashMap<String, Function<byte[], byte[]>>() {{
        put(KafkaTaskHookUtils.KAFKA_TASK_RUNNER_TARGET_CLASS_BY_SLASH, KafkaTaskHookUtils::addKafkaTaskRunnerHook);
        put(FileSmoosherHookUtils.FILE_SMOOSHER_CLASS_BY_SLASH, FileSmoosherHookUtils::addFileSmoosherHook);

    }};

    @Override
    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
        Function<byte[], byte[]> function = hookMap.get(className);
        if (Objects.nonNull(function)) {
            return function.apply(classfileBuffer);
        } else {
            return classfileBuffer;
        }
    }
}
