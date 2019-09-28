package mykidong.kafka;

import mykidong.domain.avro.events.Events;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

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
                long offset = getOffsetFromDB(groupId, topicPartition);
                consumer.seek(topicPartition, offset);
                log.info("consumer seek to the offset [{}] with groupId [{}], topic [{}] and parition [{}]", Arrays.asList(offset, groupId, topicPartition.topic(), topicPartition.partition()).toArray());
            }


            while (true) {
                // if wakeupCalled flag set to true, throw WakeupException to exit, before that flushing message by producer
                // and offsets committed by consumer will occur.
                if (this.wakeupCalled) {
                    throw new WakeupException();
                }

                ConsumerRecords<String, Events> records = consumer.poll(100);
                if(!records.isEmpty()) {
                    for (ConsumerRecord<String, Events> record : records) {
                        String key = record.key();
                        Events events = record.value();

                        log.info("key: [" + key + "], events: [" + events.toString() + "], topic: [" + record.topic() + "], partition: [" + record.partition() + "], offset: [" + record.offset() + "]");

                        // process events.
                        processEvents(events);

                        // an action involved in this db transaction.

                        // NOTE: if consumers run with difference group id, avoid saving duplicated events to db.
                        saveEventsToDB(events);

                        // another action involved in this db transaction.
                        saveOffsetsToDB(groupId, record.topic(), record.partition(), record.offset());
                    }

                    commitDBTransaction();
                }
            }

        } catch (WakeupException e) {

        } finally {
            commitDBTransaction();
            this.consumer.close();
        }
    }
}
