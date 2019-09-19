package mykidong.dao.mysql;

import mykidong.api.dao.EventsDao;
import mykidong.domain.avro.events.Events;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by mykidong on 2019-09-18.
 */
public class MySQLEventsDaoTest {

    private static Logger log = LoggerFactory.getLogger(MySQLEventsDaoTest.class);

    private EventsDao eventsDao;

    @Before
    public void init() throws Exception {
        eventsDao = new MySQLEventsDao();
    }

    @Test
    public void saveEventsAndOffetToDB() throws Exception
    {
        saveEventsToDB();
        saveOffsetsToDB();

        eventsDao.commitDBTransaction();
    }


    private void saveEventsToDB() throws Exception
    {
        Events events = new Events();
        events.setCustomerId("customer-id-1");
        events.setOrderInfo("some order info ... 1");
        events.setEventTime(new Date().getTime());

        eventsDao.saveEventsToDB(events);
    }

    private void saveOffsetsToDB() throws Exception
    {
        String groupId = "group-test";
        String topic = "test-topic";
        int partition = 0;
        long offset = 1;

        eventsDao.saveOffsetsToDB(groupId, topic, partition, offset);
    }

    @Test
    public void getOffsetFromDB() throws Exception
    {
        String groupId = "group-test";
        String topic = "test-topic";
        int partition = 0;

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        long offset = eventsDao.getOffsetFromDB(groupId, topicPartition);
        log.info("offset returned: [{}]", offset);
        Assert.assertTrue(offset == 2);
    }

}
