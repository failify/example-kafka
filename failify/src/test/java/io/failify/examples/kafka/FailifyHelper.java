package io.failify.examples.kafka;

import io.failify.FailifyRunner;
import io.failify.dsl.entities.Deployment;
import io.failify.dsl.entities.PathAttr;
import io.failify.dsl.entities.ServiceType;
import io.failify.exceptions.RuntimeEngineException;
import io.failify.execution.ULimit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
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

    public static Deployment getDeployment(int numOfNodes) {
        String version = "2.12-2.2.0"; // this can be dynamically generated from maven metadata
        String dir = "kafka_" + version;
        StringJoiner seeds = new StringJoiner(",");
        for (int i=1; i<=numOfNodes; i++) seeds.add("n" + i);
        return Deployment.builder("example-kafka")
            .withService("zk").dockerImgName("zookeeper:3.4.14").disableClockDrift().and().withNode("zk1", "zk").and()
            .withService("kafka")
                .appPath("../" + dir + "-build/distributions/" + dir + ".tgz", "/kafka", PathAttr.COMPRESSED)
                .workDir("/kafka/" + dir).startCmd("bin/kafka-server-start.sh config/server.properties")
                .dockerImgName("failify/kafka:1.0").dockerFileAddr("docker/Dockerfile", true)
                .logDir("/kafka/" + dir + "/logs").serviceType(ServiceType.SCALA).disableClockDrift()
                .appPath("config/server.properties", "/kafka/" + dir + "/config/server.properties",
                        new HashMap<String, String>() {{ put("PARTITIONS", String.valueOf(Math.min(8, numOfNodes)));
                                                        put("REPLICAS", String.valueOf(Math.min(3, numOfNodes)));}})
                .ulimit(ULimit.NOFILE, 100000).and().nodeInstances(numOfNodes, "n", "kafka", false).build();
    }

    public static void waitForCluster(FailifyRunner runner) throws RuntimeEngineException {
        logger.info("Waiting for cluster to start up ...");
        boolean started = false;
        ZooKeeper zkClient;
        int numOfNodes = getNumberOfNodes(runner);
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
                if (zkClient.getChildren("/brokers/ids", false).size() == numOfNodes) started = true;
            } catch (KeeperException | InterruptedException e) {}// next round
        } while (!started);
        logger.info("The cluster is UP!");
    }

    public static String getBootStrapServers(FailifyRunner runner) {
        StringJoiner joiner = new StringJoiner(",");
        for (int i=1; i<=getNumberOfNodes(runner); i++) joiner.add("n" + i + ":9092");
        return joiner.toString();
    }

    private static int getNumberOfNodes(FailifyRunner runner) {
        return (int) runner.runtime().nodeNames().stream().filter(x -> x.startsWith("n")).count();
    }

    public static AdminClient getAdminClient(FailifyRunner runner) {
        return AdminClient.create(new HashMap<String, Object>() {{
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootStrapServers(runner)); }});
    }
}
