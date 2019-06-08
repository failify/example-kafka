package io.failify.examples.kafka;


import io.failify.FailifyRunner;
import io.failify.dsl.entities.Deployment;
import io.failify.exceptions.RuntimeEngineException;
import io.failify.execution.NetPart;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);

    @Test
    public void sampleTest() throws RuntimeEngineException, InterruptedException {
        int NUM_OF_BROKERS = 3;
        FailifyHelper failifyHelper = new FailifyHelper(NUM_OF_BROKERS);
        Deployment deployment = failifyHelper.getDeployment();
        KafkaHelper kafkaHelper = new KafkaHelper(failifyHelper.getBootStrapServers());
        deployment = Deployment.builder(deployment)
            .withService("producer", "common")
                .appPath("config/connect-standalone.properties", "/connect.properties",
                        new HashMap<String, String>() {{put("BOOTSTRAP", failifyHelper.getBootStrapServers());}})
                .appPath("config/connect-file-source.properties", "/file.properties")
                .startCmd("bin/connect-standalone.sh /connect.properties /file.properties")
                .appPath("input/test1.txt", "/test.txt").and().withNode("p1", "producer").offOnStartup().and()
            .withService("consumer", "common")
                .startCmd("bin/kafka-console-consumer.sh --bootstrap-server " + failifyHelper.getBootStrapServers()
                        + " --group g1 --topic connect-test --from-beginning")
                .and().nodeInstances(NUM_OF_BROKERS, "c", "consumer", true).build();
        FailifyRunner runner = deployment.start();
        failifyHelper.waitForCluster();
        kafkaHelper.createTopic("connect-test", 3, 3);
        runner.runtime().startNode("p1");
        for (int i=1; i<=3; i++) runner.runtime().startNode("c" + i);
        Thread.sleep(10000);
        String coordinator = kafkaHelper.getConsumerGroupCoordinator("g1");
        runner.runtime().networkPartition(NetPart.partitions(coordinator).build());
        Thread.sleep(120000);
        runner.stop();
    }
}
