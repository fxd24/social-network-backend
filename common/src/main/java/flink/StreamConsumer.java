package flink;

import com.dspa.project.model.Stream;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class StreamConsumer {
    public static FlinkKafkaConsumer011<Stream> createStreamConsumer(String topic, String kafkaAddress, String kafkaGroup, DeserializationSchema deserializationSchema) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaAddress);
        properties.setProperty("group.id", kafkaGroup);
        FlinkKafkaConsumer011<Stream> consumer = new FlinkKafkaConsumer011<Stream>(
                topic, deserializationSchema, properties);

        return consumer;
    }
}
