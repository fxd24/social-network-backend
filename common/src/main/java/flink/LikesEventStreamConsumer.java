package flink;


import com.dspa.project.common.deserialization.LikesEventStreamDeserializationSchema;
import com.dspa.project.model.LikesEventStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class LikesEventStreamConsumer {
    public static FlinkKafkaConsumer011<LikesEventStream> createLikesEventStreamConsumer(String topic, String kafkaAddress, String kafkaGroup) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaAddress);
        properties.setProperty("group.id", kafkaGroup);
        FlinkKafkaConsumer011<LikesEventStream> consumer = new FlinkKafkaConsumer011<LikesEventStream>(
                topic, new LikesEventStreamDeserializationSchema(), properties);

        return consumer;
    }
}