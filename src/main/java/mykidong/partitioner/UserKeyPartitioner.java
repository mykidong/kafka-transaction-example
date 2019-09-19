package mykidong.partitioner;

import mykidong.domain.UserKey;
import mykidong.domain.avro.events.Events;
import mykidong.kafka.TransactionalConsumerRebalanceListener;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by mykidong on 2019-09-11.
 */
public class UserKeyPartitioner extends DefaultPartitioner {

    private static Logger log = LoggerFactory.getLogger(TransactionalConsumerRebalanceListener.class);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Object newKey = null;
        if (key != null) {
            UserKey userKey = (UserKey) key;
            newKey = userKey.getCustomerId();
            keyBytes = ((String) newKey).getBytes();
        }

        int partition = super.partition(topic, newKey, keyBytes, value, valueBytes, cluster);
        log.info("partition number [{}] for the customerId [{}] and events [{}]", Arrays.asList(partition, (key != null) ? ((UserKey) key).getCustomerId() : null, ((Events)value).toString()).toArray());

        return partition;
    }
}
