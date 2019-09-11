package mykidong.kafka;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import mykidong.util.Log4jConfigurer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mykidong on 2019-09-10.
 */
public class TransactionalConsumer {

    private static Logger log = LoggerFactory.getLogger(TransactionalConsumer.class);

    @Before
    public void init() throws Exception {
        Log4jConfigurer log4j = new Log4jConfigurer();
        log4j.setConfPath("/log4j-test.xml");
        log4j.afterPropertiesSet();
    }

    @Test
    public void consume() throws Exception
    {

        String brokers = System.getProperty("brokers", "localhost:9092");
        String topic = System.getProperty("topic", "test-events");
        String registry = System.getProperty("registry", "http://localhost:8081");
        String groupId = System.getProperty("groupId", "group-p1");
        String partition = System.getProperty("partition", "0");

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);

        // transaction properties.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // group id.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);


        // TODO: consumer partition assignment.


        new Consumer(props, topic).start();

        Thread.sleep(Long.MAX_VALUE);
    }

    private static class Consumer extends Thread {
        KafkaConsumer<Integer, GenericRecord> consumer;
        private final String topic;

        private AtomicLong count = new AtomicLong(0);

        public Consumer(Properties props, String topic) {
            consumer = new KafkaConsumer<>(props);
            this.topic = topic;
        }

        public void run() {
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                ConsumerRecords<Integer, GenericRecord> records = consumer.poll(100);
                for (ConsumerRecord<Integer, GenericRecord> record : records) {
                    GenericRecord genericRecord = record.value();

                    String json = genericRecord.toString();

                    log.info("offset = " + record.offset() + ", key = " + record.key() + ", value = " + json + ", count = " + count.incrementAndGet());
                }
            }
        }
    }
}
