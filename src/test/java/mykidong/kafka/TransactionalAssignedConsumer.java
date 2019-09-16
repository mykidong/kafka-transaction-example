package mykidong.kafka;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import mykidong.domain.avro.events.Events;
import mykidong.util.Log4jConfigurer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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
public class TransactionalAssignedConsumer {

    private static Logger log = LoggerFactory.getLogger(TransactionalAssignedConsumer.class);

    @Before
    public void init() throws Exception {
        Log4jConfigurer log4j = new Log4jConfigurer();
        log4j.setConfPath("/log4j-test.xml");
        log4j.afterPropertiesSet();
    }

    @Test
    public void consume() throws Exception
    {
        // ----------------------------------------------------------
        // 하나의 consumer 가 하나의 특정 topic partition 에만 assign 되어
        // consuming 을 할때: 특정 topic partition number 와 unique 한 group id 를 설정함.
        //
        // transactional(exactly once) 한 process 하는 예제.
        // ----------------------------------------------------------


        String brokers = System.getProperty("brokers", "localhost:9092");
        String topic = System.getProperty("topic", "test-events");
        String partition = System.getProperty("partition", "0"); // topic partition.
        String registry = System.getProperty("registry", "http://localhost:8081");
        String groupId = System.getProperty("groupId", "group-p1"); // unique group id.

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

        // construct consume handler.
        ConsumeHandler consumeHandler = new ConsumeHandler(props, topic, Integer.valueOf(partition));
        consumeHandler.start();

        Thread.sleep(Long.MAX_VALUE);
    }

    public static class ConsumeHandler extends Thread {
        KafkaConsumer<String, Events> consumer;
        private final String topic;
        private final int partition;

        private AtomicLong count = new AtomicLong(0);

        public ConsumeHandler(Properties props, String topic, int partition) {
            consumer = new KafkaConsumer<>(props);
            this.topic = topic;
            this.partition = partition;
        }

        public KafkaConsumer<String, Events> getConsumer()
        {
            return consumer;
        }

        public void processEvents(Events events)
        {
            // TODO: process events streams.
        }

        public void saveEventsToDB(Events events)
        {
            // TODO: save processed events to db.
        }

        public void saveOffsetsToDB(String topic, int partition, long offset)
        {
            // TODO: save offset info to db.
        }

        public void commitDBTransaction()
        {
            // TODO: commit db transaction.
        }

        public long getOffsetFromDB(TopicPartition partition)
        {
            long offset = -1L;

            // TODO: get offset from db with topic and partition.
            // 이때 db 에 저장된 offset + 1 로 return 을 함.

            return offset;
        }

        public void run() {
            TopicPartition topicPartition = new TopicPartition(topic, partition);

            // 하나의 partition 에만 assign 함.
            consumer.assign(Arrays.asList(new TopicPartition(topic, partition)));

            // 그리고 consumer 는 해당 partiion 의 offset 을 seek 함.
            consumer.seek(topicPartition, getOffsetFromDB(topicPartition));


            while (true) {
                ConsumerRecords<String, Events> records = consumer.poll(100);
                for (ConsumerRecord<String, Events> record : records) {
                    Events events = record.value();

                    // process events.
                    processEvents(events);

                    // an action involved in this db transaction.
                    saveEventsToDB(events);

                    // another action involved in this db transaction.
                    saveOffsetsToDB(record.topic(), record.partition(), record.offset());
                }

                commitDBTransaction();
            }
        }
    }
}
