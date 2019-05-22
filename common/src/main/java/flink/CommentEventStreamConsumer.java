package flink;

import com.dspa.project.common.deserialization.CommentStreamDeserializationSchema;
import com.dspa.project.model.CommentEventStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class CommentEventStreamConsumer {
    public static FlinkKafkaConsumer011<CommentEventStream> createCommentEventStreamConsumer(String topic, String kafkaAddress, String kafkaGroup) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaAddress);
        properties.setProperty("group.id", kafkaGroup);
        FlinkKafkaConsumer011<CommentEventStream> consumer = new FlinkKafkaConsumer011<CommentEventStream>(
                topic, new CommentStreamDeserializationSchema(), properties);

        return consumer;
    }
}
