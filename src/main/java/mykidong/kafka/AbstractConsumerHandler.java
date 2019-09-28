package mykidong.kafka;

import mykidong.api.dao.EventsDao;
import mykidong.dao.mysql.MySQLEventsDao;
import mykidong.domain.avro.events.Events;
import mykidong.util.JsonUtils;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
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
    protected Properties props;
    protected String groupId;

    private EventsDao eventsDao;

    public AbstractConsumerHandler(Properties props, String topic) {
        this(props, topic, -1);
    }


    public AbstractConsumerHandler(Properties props, String topic, int partition)
    {
        this.props = props;
        groupId = props.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
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

    public void saveOffsetsToDB(String groupId, String topic, int partition, long offset) {
        eventsDao.saveOffsetsToDB(groupId, topic, partition, offset);

        log.info("offset saved to db - groupId: [{}], topic: [{}], partition: [{}], offset: [{}]", Arrays.asList(groupId, topic, partition, offset).toArray());
    }

    public void commitDBTransaction() {
        eventsDao.commitDBTransaction();

        log.info("transaction committed...");
    }

    public long getOffsetFromDB(String groupId, TopicPartition topicPartition) {

        long offset = eventsDao.getOffsetFromDB(groupId, topicPartition);

        log.info("offset returned: [{}] with groupId: [{}], topic: [{}], partition: [{}]", Arrays.asList(offset, groupId, topicPartition.topic(), topicPartition.partition()).toArray());

        return offset;
    }

    public static Events convertGenericToSpecificRecord(GenericRecord genericRecord)
    {
        Events events = null;
        try {
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(Events.getClassSchema());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(genericRecord, encoder);
            encoder.flush();

            byte[] avroData = out.toByteArray();
            out.close();

            SpecificDatumReader<Events> reader = new SpecificDatumReader<Events>(Events.class);
            Decoder decoder = DecoderFactory.get().binaryDecoder(avroData, null);
            events = reader.read(null, decoder);
        }catch (Exception e)
        {
            e.printStackTrace();
        }

        return events;
    }

    public abstract void run();
}

