package headlinepulse.tools;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import headlinepulse.common.KafkaSettings;
import headlinepulse.common.Topics;

/**
 * Creates any topic in {@link Topics#ALL} that doesn't exist, and sets any config that has drifted
 * from the list. Running it again with nothing changed does nothing.
 */
public final class CreateTopics {
    private static final Logger log = LoggerFactory.getLogger(CreateTopics.class);
    private static final short REPLICATION_FACTOR = 1;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.bootstrapServers()))) {
            Set<String> existing = admin.listTopics().names().get();

            List<NewTopic> missing = Topics.ALL.stream()
                    .filter(spec -> !existing.contains(spec.name()))
                    .map(spec -> new NewTopic(spec.name(), spec.partitions(), REPLICATION_FACTOR)
                            .configs(spec.configs()))
                    .toList();
            if (!missing.isEmpty()) {
                admin.createTopics(missing).all().get();
                missing.forEach(t -> log.info("Created {} ({} partitions, {})", t.name(), t.numPartitions(), t.configs()));
            }

            int updated = 0;
            for (Topics.Spec spec : Topics.ALL) {
                if (existing.contains(spec.name()) && updateConfigs(admin, spec)) {
                    updated++;
                }
            }

            if (missing.isEmpty() && updated == 0) {
                log.info("All {} topics already exist with the right configs", Topics.ALL.size());
            }
        }
    }

    private static boolean updateConfigs(Admin admin, Topics.Spec spec) throws ExecutionException, InterruptedException {
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, spec.name());
        Config current = admin.describeConfigs(List.of(resource)).all().get().get(resource);

        Collection<AlterConfigOp> ops = new ArrayList<>();
        spec.configs().forEach((key, want) -> {
            ConfigEntry entry = current.get(key);
            String have = entry == null ? null : entry.value();
            if (!want.equals(have)) {
                log.info("Updating {}: {} {} -> {}", spec.name(), key, have, want);
                ops.add(new AlterConfigOp(new ConfigEntry(key, want), AlterConfigOp.OpType.SET));
            }
        });
        if (ops.isEmpty()) {
            return false;
        }
        admin.incrementalAlterConfigs(Map.of(resource, ops)).all().get();
        return true;
    }

    private CreateTopics() {}
}
