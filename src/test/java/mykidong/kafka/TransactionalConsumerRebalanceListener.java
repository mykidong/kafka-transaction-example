package mykidong.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * Created by mykidong on 2019-09-16.
 */
public class TransactionalConsumerRebalanceListener implements ConsumerRebalanceListener {

    private TransactionalConsumer.ConsumeHandler consumeHandler;

    public TransactionalConsumerRebalanceListener(TransactionalConsumer.ConsumeHandler consumeHandler)
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
            this.consumeHandler.getConsumer().seek(topicPartition, this.consumeHandler.getOffsetFromDB(topicPartition));
        }
    }
}
