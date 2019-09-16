package mykidong.kafka;

import mykidong.domain.avro.events.Events;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by mykidong on 2019-09-16.
 */
public abstract class AbstractConsumerHandler<K, V> implements ConsumerHandler<K, V> {

    private static Logger log = LoggerFactory.getLogger(AbstractConsumerHandler.class);

    protected KafkaConsumer<K, V> consumer;
    protected String topic;
    protected int partition;
    protected boolean wakeupCalled = false;

    public AbstractConsumerHandler(Properties props, String topic) {
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }


    public AbstractConsumerHandler(Properties props, String topic, int partition)
    {
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.partition = partition;
    }



    public KafkaConsumer<K, V> getConsumer() {
        return consumer;
    }

    public void setWakeupCalled(boolean wakeupCalled) {
        this.wakeupCalled = wakeupCalled;
    }

    public void processEvents(Events events) {
        // TODO: process events streams.
    }

    public void saveEventsToDB(Events events) {
        // TODO: save processed events to db.
    }

    public void saveOffsetsToDB(String topic, int partition, long offset) {
        // TODO: save offset info to db.
    }

    public void commitDBTransaction() {
        // TODO: commit db transaction.
    }

    public long getOffsetFromDB(TopicPartition topicPartition) {
        long offset = -1L;

        String topic = topicPartition.topic();
        int partition = topicPartition.partition();

        // TODO: get offset from db with topic and partition.
        // 이때 db 에 저장된 offset + 1 로 return 을 함.

        log.info("offset to be returned: [{}]", offset);

        return offset;
    }


    public abstract void run();
}

