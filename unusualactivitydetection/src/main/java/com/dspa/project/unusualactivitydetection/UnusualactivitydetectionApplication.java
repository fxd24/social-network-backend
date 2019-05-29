package com.dspa.project.unusualactivitydetection;

import com.dspa.project.common.deserialization.CommentStreamDeserializationSchema;
import com.dspa.project.common.deserialization.LikesEventStreamDeserializationSchema;
import com.dspa.project.common.deserialization.PostEventStreamDeserializationSchema;
import com.dspa.project.model.Stream;
import flink.StreamConsumer;
import flink.StreamTimestampAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;


@SpringBootApplication
public class UnusualactivitydetectionApplication implements CommandLineRunner {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(UnusualactivitydetectionApplication.class, args);


        context.close();
    }
    @Override
    public void run(String... args) {
        /*******************  General Config *********************/
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTimestampAssigner streamTimestampAssigner = new StreamTimestampAssigner(Time.milliseconds(300000)); //TODO: modify to new value
        /*******************  CommentEventStream Config *********************/
        FlinkKafkaConsumer011<Stream> consumeComment = StreamConsumer.createStreamConsumer("comment","localhost:9092", "bar", new CommentStreamDeserializationSchema()); //TODO: change to correct topic
        consumeComment.setStartFromEarliest(); //TODO: change this based on what is required
        DataStream<Stream> commentInputStream = environment.addSource(consumeComment);
        /*******************  LikesEventStream Config *********************/
        FlinkKafkaConsumer011<Stream> consumeLikes = StreamConsumer.createStreamConsumer("likes", "localhost:9092", "bar", new LikesEventStreamDeserializationSchema());
        consumeLikes.setStartFromEarliest();
        DataStream<Stream> likesInputStream = environment.addSource(consumeLikes);
        /*******************  PostEventStream Config *********************/
        FlinkKafkaConsumer011<Stream> consumePost = StreamConsumer.createStreamConsumer("post", "localhost:9092", "bar", new PostEventStreamDeserializationSchema());
        consumePost.setStartFromEarliest();
        DataStream<Stream> postInputStream = environment.addSource(consumePost);


        //TODO: do we persist the data in this module?

        /** Assign Timestamps and Watermarks on all the streams **/
        //DataStream<Stream> connectedStream = commentInputStream.union(likesInputStream,postInputStream).assignTimestampsAndWatermarks(streamTimestampAssigner);
        commentInputStream
                .assignTimestampsAndWatermarks(streamTimestampAssigner)
                .print();
    }




}
