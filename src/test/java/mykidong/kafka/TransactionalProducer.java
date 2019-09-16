package mykidong.kafka;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import kafka.common.KafkaException;
import mykidong.domain.UserKey;
import mykidong.domain.avro.events.Events;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by mykidong on 2019-09-10.
 */
public class TransactionalProducer {

    @Test
    public void produce() throws Exception {
        String brokers = System.getProperty("brokers", "localhost:9092");
        String topic = System.getProperty("topic", "test-events");
        String registry = System.getProperty("registry", "http://localhost:8081");
        String transactionalId = System.getProperty("transactionalId", "producer-1"); // unique transactional id.

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);

        // transaction properties.
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId); // unique transactional id.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        // partition class.
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "mykidong.kafka.partitioner.UserKeyPartitioner");


        // construct producer.
        KafkaProducer<UserKey, Events> producer = new KafkaProducer<>(props);

        // initiate transaction.
        producer.initTransactions();
        try {
            // begin transaction.
            producer.beginTransaction();

            // send messages.
            UserKey key = null; // TODO:
            Events events = null; // TODO:
            producer.send(new ProducerRecord<UserKey, Events>(topic, key, events));

            // commit transaction.
            producer.commitTransaction();

        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }

        // close producer.
        producer.close();

    }
}
