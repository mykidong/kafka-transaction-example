package mykidong.kafka;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import kafka.common.KafkaException;

import mykidong.domain.UserKey;
import mykidong.domain.avro.events.Events;
import mykidong.util.Log4jConfigurer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by mykidong on 2019-09-10.
 */
public class TransactionalProducer {

    private static Logger log = LoggerFactory.getLogger(TransactionalProducer.class);

    @Before
    public void init() throws Exception {
        Log4jConfigurer log4j = new Log4jConfigurer();
        log4j.setConfPath("/log4j-test.xml");
        log4j.afterPropertiesSet();
    }

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
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "mykidong.serializer.UserKeySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);

        // transaction properties.
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId); // unique transactional id.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 600000);

        // partition class.
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "mykidong.partitioner.UserKeyPartitioner");


        // construct producer.
        KafkaProducer<UserKey, Events> producer = new KafkaProducer<>(props);

        // initiate transaction.
        producer.initTransactions();
        log.info("tx init...");
        try {
            // begin transaction.
            producer.beginTransaction();
            log.info("tx begun...");

            for(int i = 0; i < 20; i++) {
                Events events = new Events();
                events.setCustomerId("customer-id-" + (i % 5));
                events.setOrderInfo("some order info " + new Date().toString() + "-" + i);

                Date date = new Date();
                events.setEventTime(date.getTime());


                UserKey key = new UserKey(events.getCustomerId().toString(), date);


                // send messages.
                Future<RecordMetadata> response = producer.send(new ProducerRecord<UserKey, Events>(topic, key, events));
                log.info("message sent ... " + new Date().toString() + "-" + i);

                RecordMetadata recordMetadata = response.get();
                log.info("response - topic [{}], partition [{}], offset [{}]", Arrays.asList(recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()).toArray());
            }

            // commit transaction.
            producer.commitTransaction();
            log.info("tx committed...");

        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }

        // close producer.
        producer.close();

    }

    @Test
    public void printMultipleParams()
    {
        String topic = "test-topic";
        int partition = 0;
        long offset = 124;

        log.info("topic: [{}], partition: [{}], offset: [{}]", Arrays.asList(topic, partition, offset).toArray());
    }
}
