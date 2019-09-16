package mykidong.partitioner;

import mykidong.domain.UserKey;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

/**
 * Created by mykidong on 2019-09-11.
 */
public class UserKeyPartitioner extends DefaultPartitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Object newKey = null;
        if (key != null) {
            UserKey userKey = (UserKey) key;
            newKey = userKey.getCustomerId();
            keyBytes = ((String) newKey).getBytes();
        }
        return super.partition(topic, newKey, keyBytes, value, valueBytes, cluster);
    }
}
