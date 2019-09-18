package mykidong.util;

import com.cedarsoftware.util.io.JsonWriter;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Arrays;

/**
 * Created by mykidong on 2017-11-27.
 */
public class KafkaSchemaRegistryHandler {

    private static Logger log = LoggerFactory.getLogger(KafkaSchemaRegistryHandler.class);

    @Before
    public void init() throws Exception {
        Log4jConfigurer log4j = new Log4jConfigurer();
        log4j.setConfPath("/log4j-test.xml");
        log4j.afterPropertiesSet();
    }

    /**
     * local 에 있는 avsc file 을 읽어 schema registry 에 등록.
     *
     * @throws Exception
     */
    @Test
    public void registerSubject() throws Exception {
        String url = System.getProperty("url", "http://localhost:8081");
        String topic = System.getProperty("topic");
        String schemaPath = System.getProperty("schemaPath");

        String subject = topic + "-value";

        String schema = null;

        FileInputStream inputStream = new FileInputStream(schemaPath);
        try {
            schema = IOUtils.toString(inputStream);
        } finally {
            inputStream.close();
        }

        Schema avroSchema = new Schema.Parser().parse(schema);

        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(url, 20);

        client.register(subject, avroSchema);

        SchemaMetadata schemaMetadata = client.getLatestSchemaMetadata(subject);

        System.out.printf("registered subject: [%s], id: [%d], schema: [%s]", subject, schemaMetadata.getId(), schemaMetadata.getSchema());
    }


    /**
     * classpath 내 schema directory 와 schema name 을 통한 registry 등록.
     *
     *
     * @throws Exception
     */
    @Test
    public void registerSubjectWithAvroSchemaLoader() throws Exception {
        String url = System.getProperty("url", "http://localhost:8081");
        log.info("url: [{}]", url);

        String topic = System.getProperty("topic");
        log.info("topic: [{}]", topic);

        String pathDir = System.getProperty("pathDir", "/META-INF/avro/events");
        log.info("pathDir: [{}]", pathDir);

        String schemaName = System.getProperty("schemaName");
        log.info("schemaName: [{}]", schemaName);

        String subject = topic + "-value";

        AvroSchemaLoader avroSchemaLoader = AvroSchemaLoader.singleton(pathDir);

        Schema avroSchema = avroSchemaLoader.getSchema(schemaName);
        log.info("avroSchema: [{}]", avroSchema.toString(true));

        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(url, 20);

        client.register(subject, avroSchema);

        SchemaMetadata schemaMetadata = client.getLatestSchemaMetadata(subject);

        log.info("registered subject: [{}], id: [{}], schema: [{}]", Arrays.asList(subject, schemaMetadata.getId(), JsonWriter.formatJson(schemaMetadata.getSchema())).toArray());
    }

    /**
     * classpath 내 avro avsc schema file path 와 schema name 을 이용하여 avro schema 를 registry 에 등록.
     *
     * @throws Exception
     */
    @Test
    public void registerSubjectWithAvroSchemaLoaderAndSchemaPath() throws Exception {
        String url = System.getProperty("url", "http://localhost:8081");
        log.info("url: [{}]", url);

        String topic = System.getProperty("topic");
        log.info("topic: [{}]", topic);

        String schemaPath = System.getProperty("schemaPath", "/META-INF/avro/events/events.avsc");
        log.info("schemaPath: [{}]", schemaPath);

        String schemaName = System.getProperty("schemaName");
        log.info("schemaName: [{}]", schemaName);

        String subject = topic + "-value";

        AvroSchemaLoader avroSchemaLoader = AvroSchemaLoader.singletonForSchemaPaths(schemaPath);

        Schema avroSchema = avroSchemaLoader.getSchema(schemaName);
        log.info("avroSchema: [{}]", avroSchema.toString(true));

        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(url, 20);

        client.register(subject, avroSchema);

        SchemaMetadata schemaMetadata = client.getLatestSchemaMetadata(subject);

        log.info("registered subject: [{}], id: [{}], schema: [{}]", Arrays.asList(subject, schemaMetadata.getId(), JsonWriter.formatJson(schemaMetadata.getSchema())).toArray());
    }
}
