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
public class TransactionalAssignedConsumer extends AbstractConsumerHandler<String, Events>{

    private static Logger log = LoggerFactory.getLogger(TransactionalAssignedConsumer.class);


    public TransactionalAssignedConsumer(Properties props, String topic, int partition) {
        super(props, topic, partition);
    }


    @Override
    public void run() {
        try {
            TopicPartition topicPartition = new TopicPartition(topic, partition);

            // 하나의 partition 에만 assign 함.
            consumer.assign(Arrays.asList(new TopicPartition(topic, partition)));

            // 그리고 consumer 는 해당 partiion 의 offset 을 seek 함.
            long offset = getOffsetFromDB(groupId, topicPartition);
            consumer.seek(topicPartition, offset);
            log.info("consumer seek to the offset [{}] with groupId [{}], topic [{}] and parition [{}]", Arrays.asList(offset, groupId, topicPartition.topic(), topicPartition.partition()).toArray());


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
