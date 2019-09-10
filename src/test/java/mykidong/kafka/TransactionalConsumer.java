package mykidong.kafka;

import mykidong.util.Log4jConfigurer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", "kafka-event-consumer-test");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", registry);

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
