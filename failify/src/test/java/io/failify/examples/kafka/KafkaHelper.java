package io.failify.examples.kafka;

import org.apache.kafka.clients.admin.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class KafkaHelper {

    private final String bootstrapServer;

    public KafkaHelper(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    private AdminClient getAdminClient() {
        Map<String, Object> props = new HashMap<String, Object>() {{put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);}};
        return AdminClient.create(props);
    }

    public void createTopic(String topic, int partitions, int replicas) {
        try (AdminClient adminClient = getAdminClient()) {
            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(new NewTopic(topic, partitions, (short) replicas)));
            result.values().get(topic).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("error while creating topic " + topic + " using bootstrap server " + bootstrapServer);
        }
    }

    public Map<String, TopicListing> listTopics() {
        try (AdminClient adminClient = getAdminClient()) {
            ListTopicsResult result = adminClient.listTopics();
            try {
                return result.namesToListings().get();
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException("error while listings topics using bootstrap server " + bootstrapServer);
            }
        }
    }

    public String getConsumerGroupCoordinator(String group) {
        try (AdminClient adminClient = getAdminClient()) {
            try {
                return adminClient.describeConsumerGroups(Collections.singleton(group)).describedGroups().get(group).get().coordinator().host();
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException("error while getting the coordinator of consumer group " + group +
                        " using bootstrap server " + bootstrapServer);
            }
        }
    }
}
