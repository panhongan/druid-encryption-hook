package com.github.panhongan.bigdata.extension.druid;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import org.apache.druid.indexing.kafka.KafkaIndexTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class KafkaTaskHook {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTaskHook.class);

    public static final String KAFKA_TASK_RUNNER_TARGET_CLASS_BY_DOT = "org.apache.druid.indexing.kafka.IncrementalPublishingKafkaIndexTaskRunner";

    public static final String KAFKA_TASK_RUNNER_TARGET_CLASS_BY_SLASH = KAFKA_TASK_RUNNER_TARGET_CLASS_BY_DOT.replace('.', '/');

    private static final String CONTEXT_ATTR_NEED_ENCRYPTION = "druid.enable.encryption";

    public static boolean enableEncryption = false;

    public static boolean kafkaTaskHookInitialized = false;

    public static byte[] addKafkaTaskRunnerHook(byte[] classfileBuffer) {
        try {
            ClassPool classPool = ClassPool.getDefault();
            CtClass ctClass = classPool.get(KAFKA_TASK_RUNNER_TARGET_CLASS_BY_DOT);
            CtConstructor ctMethod = ctClass.getDeclaredConstructors()[0];

            ctMethod.insertAfter(KafkaTaskHook.class.getName() + ".hookKafkaTaskRunnerFunc($1);");

            return ctClass.toBytecode();
        } catch (Throwable t) {
            LOGGER.error("", t);
        }

        return classfileBuffer;
    }

    public static void hookKafkaTaskRunnerFunc(Object paramValue) {
        if (Objects.isNull(paramValue)) {
            LOGGER.warn("Invalid parameter");
            return;
        }

        try {
            if (paramValue instanceof KafkaIndexTask) {
                KafkaIndexTask kafkaIndexTask = (KafkaIndexTask) paramValue;
                LOGGER.info("hook task: {}", kafkaIndexTask.getId());

                // CONTEXT_DIMENSION_KEY
                Object enableEncryptionObj = kafkaIndexTask.getContextValue(CONTEXT_ATTR_NEED_ENCRYPTION);
                if (Objects.nonNull(enableEncryptionObj)) {
                    enableEncryption = (Boolean) enableEncryptionObj;
                    LOGGER.info("Context attribute: {} = {}", CONTEXT_ATTR_NEED_ENCRYPTION, enableEncryption);
                } else {
                    LOGGER.info("Context attribute not set: {}", CONTEXT_ATTR_NEED_ENCRYPTION);
                }

                kafkaTaskHookInitialized = true;
            } else {
                LOGGER.error("Unexpected parameter");
            }
        } catch (Throwable t) {
            LOGGER.error("", t);
        }
    }

    public static boolean canEncrypt() {
        return kafkaTaskHookInitialized && enableEncryption;
    }
}
