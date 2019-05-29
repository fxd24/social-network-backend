package com.dspa.project.recommendation;

import com.dspa.project.common.deserialization.CommentStreamDeserializationSchema;
import com.dspa.project.common.deserialization.LikesEventStreamDeserializationSchema;
import com.dspa.project.common.deserialization.PostEventStreamDeserializationSchema;
import com.dspa.project.model.LikesEventStream;
import com.dspa.project.model.Stream;
import com.dspa.project.recommendation.repository.ForumRepository;
import flink.StreamConsumer;
import flink.StreamTimestampAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;


@SpringBootApplication
@EntityScan(basePackages = {"com.dspa.project.model"})
public class RecommendationApplication implements CommandLineRunner {


    public static void main(String[] args) {

        ConfigurableApplicationContext context = SpringApplication.run(RecommendationApplication.class, args);

        context.close();
    }

    @Override
    public void run(String... args) {
/*******************  General Config *********************/
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTimestampAssigner streamTimestampAssigner = new StreamTimestampAssigner(Time.milliseconds(300000)); //TODO: modify to new value
        /*******************  CommentEventStream Config *********************/
        FlinkKafkaConsumer011<Stream> consumeComment = StreamConsumer.createStreamConsumer("comment","localhost:9092", "recommender", new CommentStreamDeserializationSchema()); //TODO: change to correct topic
        consumeComment.setStartFromEarliest(); //TODO: change this based on what is required
        DataStream<Stream> commentInputStream = environment.addSource(consumeComment);

        /*******************  LikesEventStream Config *********************/
        FlinkKafkaConsumer011<Stream> consumeLikes = StreamConsumer.createStreamConsumer("likes", "localhost:9092", "recommender", new LikesEventStreamDeserializationSchema());
        consumeLikes.setStartFromEarliest();
        DataStream<Stream> likesInputStream = environment.addSource(consumeLikes);

        /*******************  PostEventStream Config *********************/
        FlinkKafkaConsumer011<Stream> consumePost = StreamConsumer.createStreamConsumer("post", "localhost:9092", "recommender", new PostEventStreamDeserializationSchema());
        consumePost.setStartFromEarliest();
        DataStream<Stream> postInputStream = environment.addSource(consumePost);

        /***********    COOL STUFF FOR RECOMMENDATION  ****************/

        int[] personIds = {47, 192, 265, 395, 434, 581, 650, 724, 838, 913};

        likesInputStream
                .assignTimestampsAndWatermarks(streamTimestampAssigner)
                .keyBy(x->{return ((LikesEventStream) x).getPersonId();})
                .window(SlidingEventTimeWindows.of(Time.hours(4),Time.hours(1)))
//                .process()
//                .print()
                  ;
        /******* EXECUTE THE COOL STUFF ABOVE ********/
        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
