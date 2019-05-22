package flink;

import com.dspa.project.common.deserialization.PostEventStreamDeserializationSchema;
import com.dspa.project.model.PostEventStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class PostEventStreamConsumer {
    public static FlinkKafkaConsumer011<PostEventStream> createPostEventStreamConsumer(String topic, String kafkaAddress, String kafkaGroup) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaAddress);
        properties.setProperty("group.id", kafkaGroup);
        FlinkKafkaConsumer011<PostEventStream> consumer = new FlinkKafkaConsumer011<PostEventStream>(
                topic, new PostEventStreamDeserializationSchema(), properties);

        return consumer;
    }
}
