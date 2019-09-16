package mykidong.kafka;

import mykidong.domain.avro.events.Events;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Created by mykidong on 2019-09-16.
 */
public interface ConsumerHandler<K, V> extends Runnable {

    KafkaConsumer<K, V> getConsumer();

    void setWakeupCalled(boolean wakeupCalled);

}
