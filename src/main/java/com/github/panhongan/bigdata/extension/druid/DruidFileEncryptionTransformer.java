package com.github.panhongan.bigdata.extension.druid;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class DruidFileEncryptionTransformer implements ClassFileTransformer {

    private static Map<String, Function<byte[], byte[]>> hookMap = new HashMap<String, Function<byte[], byte[]>>() {{
        put(KafkaTaskHook.KAFKA_TASK_RUNNER_TARGET_CLASS_BY_SLASH, KafkaTaskHook::addKafkaTaskRunnerHook);
        put(FileSmoosherHook.FILE_SMOOSHER_CLASS_BY_SLASH, FileSmoosherHook::addFileSmoosherHook);
        put(SmooshedFileMapperHook.SMOOSHED_FILE_MAPPER_CLASS_BY_SLASH, SmooshedFileMapperHook::addSmooshedFileMapperHook);
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
