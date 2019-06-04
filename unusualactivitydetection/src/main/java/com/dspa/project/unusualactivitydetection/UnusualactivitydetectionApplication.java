package com.dspa.project.unusualactivitydetection;

import com.dspa.project.common.deserialization.CommentStreamDeserializationSchema;
import com.dspa.project.common.deserialization.PostEventStreamDeserializationSchema;
import com.dspa.project.model.CommentEventStream;
import com.dspa.project.model.PostEventStream;
import com.dspa.project.model.Stream;
import flink.StreamConsumer;
import flink.StreamTimestampAssigner;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;


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
        StreamTimestampAssigner streamTimestampAssigner = new StreamTimestampAssigner(Time.milliseconds(300000));
        /*******************  CommentEventStream Config *********************/
        FlinkKafkaConsumer011<Stream> consumeComment = StreamConsumer.createStreamConsumer("comment","localhost:9092", "unusual", new CommentStreamDeserializationSchema());
        consumeComment.setStartFromEarliest();
        DataStream<Stream> commentInputStream = environment.addSource(consumeComment);
        /*******************  PostEventStream Config *********************/
        FlinkKafkaConsumer011<Stream> consumePost = StreamConsumer.createStreamConsumer("post", "localhost:9092", "unusual", new PostEventStreamDeserializationSchema());
        consumePost.setStartFromEarliest();
        DataStream<Stream> postInputStream = environment.addSource(consumePost);

        /** Assign Timestamps and Watermarks on all the streams **/
        //DataStream<Stream> connectedStream = commentInputStream.union(likesInputStream,postInputStream).assignTimestampsAndWatermarks(streamTimestampAssigner);
        commentInputStream
                .assignTimestampsAndWatermarks(streamTimestampAssigner)
                .map(x->{
                    return (CommentEventStream) x;
                })
                .process(new DetectUnusualActivityForCommentFunction())
                .print();

        postInputStream.assignTimestampsAndWatermarks(streamTimestampAssigner)
                .map(x->{
                    return (PostEventStream) x;
                })
                .process(new DetectUnusualActivityForPostFunction())
                .print();


        /******* EXECUTE THE COOL STUFF ABOVE ********/
        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public class DetectUnusualActivityForCommentFunction extends ProcessFunction<CommentEventStream, Tuple3<Integer, Date, String>> {
        transient AnomalyStatistics comments;
        transient AnomalyStatistics replies;

        @Override
        public void processElement(CommentEventStream commentEventStream, Context context, Collector<Tuple3<Integer, Date, String>> collector) throws Exception {
            boolean isAnomaly = false;

            double ratio = ratioUniqueWordsVsTotalWords(commentEventStream.getContent());
            if(commentEventStream.getReply_to_postId()==-1){
                if(replies==null){
                    replies = new AnomalyStatistics();
                }
                isAnomaly = replies.isAnomaly(ratio);
                if(isAnomaly){
                    collector.collect(new Tuple3<>(commentEventStream.getPersonId(), new Date(context.timestamp()), "Unusual Activity Detected in a Reply"));
                }
                replies.update(ratio);
            }else {
                if(comments==null){
                    comments = new AnomalyStatistics();
                }
                isAnomaly = comments.isAnomaly(ratio);
                if(isAnomaly){
                    collector.collect(new Tuple3<>(commentEventStream.getPersonId(), new Date(context.timestamp()), "Unusual Activity Detected in a Comment"));
                }
                replies.update(ratio);
            }
        }
    }

    public class DetectUnusualActivityForPostFunction extends ProcessFunction<PostEventStream, Tuple3<Integer, Date, String>> {
        transient AnomalyStatistics posts;

        @Override
        public void processElement(PostEventStream postEventStream, Context context, Collector<Tuple3<Integer, Date, String>> collector) throws Exception {
            boolean isAnomaly = false;

            double ratio = ratioUniqueWordsVsTotalWords(postEventStream.getContent());
                if(posts==null){
                    posts = new AnomalyStatistics();
                }
                isAnomaly = posts.isAnomaly(ratio);
                if(isAnomaly){
                    collector.collect(new Tuple3<>(postEventStream.getPersonId(), new Date(context.timestamp()), "Unusual Activity Detected in a Post"));
                }
                posts.update(ratio);

        }
    }


    public static int countUniqueWords(String s){
        Set<String> wordSet = new HashSet<>();
        String[] wordList = s.split(" ");
        for(String w : wordList){
            wordSet.add(w.toLowerCase());
            //System.out.println(w);
        }
        return wordSet.size();
    }
    public static int countWords(String s){
        String[] wordList = s.split(" ");
        return wordList.length;
    }

    public static double ratioUniqueWordsVsTotalWords(String s){
        double unique = countUniqueWords(s);
        double total = countWords(s);

        return (unique+2)/(unique+total);
    }

    public static class AnomalyStatistics {
        int count = 0;
        double mean = 0;
        double variance = 0;
        //double threshold = 0;


        public AnomalyStatistics() {
            this.count = 0;
            this.mean = 0;
            this.variance = 0;
        }

        public void update(double val){
            this.count++;
            double delta = val - this.mean;
            this.mean += delta/this.count;
            this.variance += delta*(val -this.mean);
        }

        public double getMean() {
            return this.mean;
        }

        public double stddev(){
            return Math.sqrt(this.variance);
        }

        public boolean isAnomaly(double value){
            double cut_off = 0.5*stddev(); //here you can specify how many stddevs you want to use as threshold.
            double lower = this.mean-cut_off;
            double upper = this.mean+cut_off;
            if((value < lower || value > upper) && count > 4){ //count>4 in order to have enough samples to say something
                return true;
            } else {
                return false;
            }
        }
    }




}
