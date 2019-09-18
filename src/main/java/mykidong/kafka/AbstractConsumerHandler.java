package mykidong.kafka;

import mykidong.api.dao.EventsDao;
import mykidong.dao.mysql.MySQLEventsDao;
import mykidong.domain.avro.events.Events;
import mykidong.util.JsonUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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

    private EventsDao eventsDao;

    public AbstractConsumerHandler(Properties props, String topic) {
        this(props, topic, -1);
    }


    public AbstractConsumerHandler(Properties props, String topic, int partition)
    {
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.partition = partition;

        eventsDao = new MySQLEventsDao();
    }



    public KafkaConsumer<K, V> getConsumer() {
        return consumer;
    }

    public void setWakeupCalled(boolean wakeupCalled) {
        this.wakeupCalled = wakeupCalled;
    }

    public void processEvents(Events events) {
        // TODO: process events streams.

        log.info("events processed: [{}]", events.toString());
    }

    public void saveEventsToDB(Events events) {

        eventsDao.saveEventsToDB(events);

        log.info("events saved to db: [{}]", events.toString());
    }

    public void saveOffsetsToDB(String topic, int partition, long offset) {
        eventsDao.saveOffsetsToDB(topic, partition, offset);

        log.info("offset saved to db - topic: [{}], partition: [{}], offset: [{}]", Arrays.asList(topic, partition, offset).toArray());
    }

    public void commitDBTransaction() {
        eventsDao.commitDBTransaction();

        log.info("transaction committed...");
    }

    public long getOffsetFromDB(TopicPartition topicPartition) {

        long offset = eventsDao.getOffsetFromDB(topicPartition);

        log.info("offset returned: [{}]", offset);

        return offset;
    }

    public abstract void run();
}

