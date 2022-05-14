package com.github.panhongan.bigdata.extension.druid;

import java.lang.instrument.Instrumentation;

public class DruidFileEncryptionAgent {

    public static void premain(String arg, Instrumentation instrumentation) {
        instrumentation.addTransformer(new DruidFileEncryptionTransformer());
    }
}
