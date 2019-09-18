package mykidong.api.dao;

import mykidong.domain.avro.events.Events;
import org.apache.kafka.common.TopicPartition;

/**
 * Created by mykidong on 2019-09-18.
 */
public interface EventsDao {

    void saveEventsToDB(Events events);

    void saveOffsetsToDB(String topic, int partition, long offset);

    void commitDBTransaction();

    long getOffsetFromDB(TopicPartition topicPartition);

}
