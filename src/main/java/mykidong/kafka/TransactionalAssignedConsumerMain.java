package mykidong.kafka;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * Created by mykidong on 2019-09-16.
 */
public class TransactionalAssignedConsumerMain {

    public static void main(String[] args)
    {
        // ----------------------------------------------------------
        // consumer is assigned to just one partition with unique group id.
        // that is, one partition per consumer approach.
        //
        // transactional(exactly once) consumer example.
        // ----------------------------------------------------------


        OptionParser parser = new OptionParser();
        parser.accepts("brokers").withRequiredArg().ofType(String.class);
        parser.accepts("topic").withRequiredArg().ofType(String.class);
        parser.accepts("partition").withRequiredArg().ofType(String.class);
        parser.accepts("registry").withRequiredArg().ofType(String.class);
        parser.accepts("groupId").withRequiredArg().ofType(String.class);

        OptionSet options = parser.parse(args);

        String brokers = (String) options.valueOf("brokers");
        String topic = (String) options.valueOf("topic");
        String partition = (String) options.valueOf("partition");
        String registry = (String) options.valueOf("registry");
        String groupId = (String) options.valueOf("groupId");


        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("specific.avro.reader", "true"); // avro specific record reader activated
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);

        // transaction properties.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // group id.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // construct consumer.
        TransactionalAssignedConsumer consumer = new TransactionalAssignedConsumer(props, topic, Integer.valueOf(partition));
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        Thread mainThread = Thread.currentThread();

        // register Message as shutdown hook
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(consumer, mainThread));
    }
}
