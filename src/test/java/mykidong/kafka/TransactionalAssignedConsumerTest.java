package mykidong.kafka;

import mykidong.util.Log4jConfigurer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * Created by mykidong on 2019-09-10.
 */
public class TransactionalAssignedConsumerTest {

    private static Logger log = LoggerFactory.getLogger(TransactionalAssignedConsumerTest.class);

    @Before
    public void init() throws Exception {
        Log4jConfigurer log4j = new Log4jConfigurer();
        log4j.setConfPath("/log4j-test.xml");
        log4j.afterPropertiesSet();
    }

    @Test
    public void consume() throws Exception {
        String brokers = System.getProperty("brokers", "localhost:9092");
        String topic = System.getProperty("topic", "test-events");
        String partition = System.getProperty("partition", "0"); // topic partition.
        String registry = System.getProperty("registry", "http://localhost:8081");
        String groupId = System.getProperty("groupId", "group-p1"); // unique group id.

        ArrayList argsList = new ArrayList();
        argsList.add("--brokers");
        argsList.add(brokers);
        argsList.add("--topic");
        argsList.add(topic);
        argsList.add("--partition");
        argsList.add(partition);
        argsList.add("--registry");
        argsList.add(registry);
        argsList.add("--groupId");
        argsList.add(groupId);

        TransactionalAssignedConsumerMain.main((String[])argsList.toArray(new String[0]));

        Thread.sleep(Long.MAX_VALUE);
    }
}
