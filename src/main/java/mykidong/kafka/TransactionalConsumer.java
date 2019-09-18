package mykidong.kafka;

import mykidong.domain.avro.events.Events;
import mykidong.util.JsonUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

import static mykidong.kafka.TransactionalAssignedConsumer.convertGenericToSpecificRecord;

/**
 * Created by mykidong on 2019-09-10.
 */
public class TransactionalConsumer extends AbstractConsumerHandler<String, Events>{

    private static Logger log = LoggerFactory.getLogger(TransactionalConsumer.class);

    public TransactionalConsumer(Properties props, String topic) {
        super(props, topic);
    }

    @Override
    public void run() {
        try {
            // consumer subscribe with consumer rebalance listener.
            consumer.subscribe(Arrays.asList(topic), new TransactionalConsumerRebalanceListener(this));
            consumer.poll(0);

            // When the consumer first starts, after we subscribed to topics, we call poll()
            // once to make sure we join a consumer group and get assigned partitions and
            // then we immediately seek() to the correct offset in the partitions we are assigned
            // to. Keep in mind that seek() only updates the position we are consuming from,
            // so the next poll() will fetch the right messages.
            for (TopicPartition topicPartition : this.consumer.assignment()) {
                consumer.seek(topicPartition, getOffsetFromDB(topicPartition));
            }


            while (true) {
                // if wakeupCalled flag set to true, throw WakeupException to exit, before that flushing message by producer
                // and offsets committed by consumer will occur.
                if (this.wakeupCalled) {
                    throw new WakeupException();
                }

                ConsumerRecords<String, Events> records = consumer.poll(100);
                for (ConsumerRecord<String, Events> record : records) {
                    String key = record.key();
                    GenericRecord genericRecord = record.value();
                    Events events = convertGenericToSpecificRecord(genericRecord);

                    log.info("key: [" + key + "], events: [" + events.toString() + "], topic: [" + record.topic() + "], partition: [" + record.partition() + "], offset: [" + record.offset() + "]");

                    // process events.
                    processEvents(events);

                    // an action involved in this db transaction.
                    saveEventsToDB(events);

                    // another action involved in this db transaction.
                    saveOffsetsToDB(record.topic(), record.partition(), record.offset());
                }

                commitDBTransaction();
            }

        } catch (WakeupException e) {

        } finally {
            commitDBTransaction();
            this.consumer.close();
        }
    }
}
