package io.failify.examples.kafka;

import io.failify.dsl.entities.Deployment;
import io.failify.dsl.entities.PathAttr;
import io.failify.dsl.entities.ServiceType;
import io.failify.exceptions.RuntimeEngineException;
import io.failify.execution.ULimit;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class FailifyHelper {
    public static final Logger logger = LoggerFactory.getLogger(FailifyHelper.class);

    private final int numOfBrokers;

    public FailifyHelper(int numOfBrokers) {
        this.numOfBrokers = numOfBrokers;
    }

    public Deployment getDeployment() {
        String version = "2.11-2.2.0"; // this can be dynamically generated from maven metadata
        String dir = "kafka_" + version;
        return Deployment.builder("example-kafka")
            .withService("zk").dockerFileAddr("docker/zk", true).dockerImgName("zookeeper:3.4.14").disableClockDrift()
                .and().withNode("zk1", "zk").and()
            .withService("common")
                .appPath("../" + dir + "-build/distributions/" + dir + ".tgz", "/kafka", PathAttr.COMPRESSED)
                .dockerImgName("failify/kafka:1.0").dockerFileAddr("docker/Dockerfile", true).disableClockDrift()
                .logDir("/kafka/" + dir + "/logs").workDir("/kafka/" + dir).serviceType(ServiceType.SCALA).and()
            .withService("broker", "common")
                .startCmd("bin/kafka-server-start.sh config/server.properties")
                .appPath("config/server.properties", "/kafka/" + dir + "/config/server.properties",
                        new HashMap<String, String>() {{ put("PARTITIONS", String.valueOf(Math.min(8, numOfBrokers)));
                                                        put("REPLICAS", String.valueOf(Math.min(3, numOfBrokers)));}})
                .ulimit(ULimit.NOFILE, 100000).and().nodeInstances(numOfBrokers, "br", "broker", false).build();
    }

    public void waitForCluster() throws RuntimeEngineException {
        logger.info("Waiting for cluster to start up ...");
        boolean started = false;
        ZooKeeper zkClient;
        int attempt = 0;
        final CountDownLatch connectedSignal = new CountDownLatch(1);

        try {
            zkClient = new ZooKeeper("zk1:2181", 3000, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            });
        } catch (IOException e) {
            throw new RuntimeEngineException("Network Failure while connecting to zookeeper");
        }
        try {
            connectedSignal.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeEngineException("Zookeeper wait signal got interrupted");
        }

        do {
            if (attempt++ >= 60)
                throw new RuntimeEngineException("Timeout in waiting for cluster startup");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeEngineException("waitForCluster sleep thread got interrupted");
            }
            try {
                if (zkClient.getChildren("/brokers/ids", false).size() == numOfBrokers) started = true;
            } catch (KeeperException | InterruptedException e) {}// next round
        } while (!started);
        logger.info("The cluster is UP!");
    }

    public String getBootStrapServers() {
        StringJoiner joiner = new StringJoiner(",");
        for (int i = 1; i<= numOfBrokers; i++) joiner.add("br" + i + ":9092");
        return joiner.toString();
    }
}
