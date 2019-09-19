package mykidong.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

/**
 * Created by mykidong on 2019-09-16.
 */
public class TransactionalConsumerRebalanceListener<K, V> implements ConsumerRebalanceListener {

    private static Logger log = LoggerFactory.getLogger(TransactionalConsumerRebalanceListener.class);

    private AbstractConsumerHandler<K, V> consumeHandler;

    public TransactionalConsumerRebalanceListener(AbstractConsumerHandler<K, V> consumeHandler)
    {
        this.consumeHandler = consumeHandler;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        // commit db transaction for saving records and offsets to db.
        this.consumeHandler.commitDBTransaction();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
        for(TopicPartition topicPartition : topicPartitions)
        {
            // get offset from db and let consumer seek to this offset.
            String groupId = this.consumeHandler.groupId;
            long offset = this.consumeHandler.getOffsetFromDB(groupId, topicPartition);
            this.consumeHandler.getConsumer().seek(topicPartition, offset);

            log.info("in rebalance listener, consumer seek to the offset [{}] with groupId [{}], topic [{}] and parition [{}]", Arrays.asList(offset, groupId, topicPartition.topic(), topicPartition.partition()).toArray());
        }
    }
}
