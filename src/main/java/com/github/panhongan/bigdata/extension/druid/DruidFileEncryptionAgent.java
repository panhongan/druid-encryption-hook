package com.github.panhongan.bigdata.extension.druid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.Instrumentation;

public class DruidFileEncryptionAgent {

    private static final Logger LOGGER = LoggerFactory.getLogger(DruidFileEncryptionAgent.class);

    /**
     * @param arg cluaster name, eg: iad, pii, timeline
     * @param instrumentation
     */
    public static void premain(String arg, Instrumentation instrumentation) {
        LOGGER.info("cluster name: {}", arg);
        AgentArg.getInstance().setClusterName(arg);

        instrumentation.addTransformer(new DruidFileEncryptionTransformer());
    }

    public static class AgentArg {

        private static AgentArg INSTANCE = new AgentArg();

        private String clusterName;

        private AgentArg() { }

        public String getClusterName() {
            return clusterName;
        }

        public void setClusterName(String clusterName) {
            this.clusterName = clusterName;
        }

        public static AgentArg getInstance() {
            return INSTANCE;
        }
    }
}
