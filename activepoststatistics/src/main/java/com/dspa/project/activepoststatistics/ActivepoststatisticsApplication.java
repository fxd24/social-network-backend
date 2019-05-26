package com.dspa.project.activepoststatistics;

import com.dspa.project.activepoststatistics.flink.PersistForComment;
import com.dspa.project.activepoststatistics.flink.PersistForLikes;
import com.dspa.project.activepoststatistics.flink.PersistForPost;
import com.dspa.project.activepoststatistics.repo.PostAndCommentRepository;
import com.dspa.project.activepoststatistics.repo.PostAndDateRepository;
import com.dspa.project.common.deserialization.CommentStreamDeserializationSchema;
import com.dspa.project.common.deserialization.LikesEventStreamDeserializationSchema;
import com.dspa.project.common.deserialization.PostEventStreamDeserializationSchema;
import com.dspa.project.model.*;
import flink.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;


@SpringBootApplication
@EntityScan(basePackages = {"com.dspa.project.model"})
@EnableJpaRepositories
public class ActivepoststatisticsApplication implements CommandLineRunner {


    @Autowired
    PostAndDateRepository repo;

    public static void main(String[] args) {
        //Spring
        ConfigurableApplicationContext context = SpringApplication.run(ActivepoststatisticsApplication.class, args);

        context.close();
    }
    @Override
    public void run(String... args) {
        /*******************  General Config *********************/
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTimestampAssigner streamTimestampAssigner = new StreamTimestampAssigner(Time.milliseconds(300000)); //TODO: add to other streams and modify classes BoundedOutOfOrder...
        /*******************  CommentEventStream Config *********************/
        FlinkKafkaConsumer011<Stream> consumeComment = StreamConsumer.createStreamConsumer("comment","localhost:9092", "bar", new CommentStreamDeserializationSchema()); //TODO: change to correct topic
        consumeComment.setStartFromLatest(); //TODO: change this based on what is required
        DataStream<Stream> commentInputStream = environment.addSource(consumeComment);
        //PersistForComment persistForComment = new PersistForComment();
        /*******************  LikesEventStream Config *********************/
        FlinkKafkaConsumer011<Stream> consumeLikes = StreamConsumer.createStreamConsumer("likes", "localhost:9092", "bar", new LikesEventStreamDeserializationSchema());
        consumeLikes.setStartFromLatest();
        DataStream<Stream> likesInputStream = environment.addSource(consumeLikes);
        //PersistForLikes persistForLikes = new PersistForLikes();
        /*******************  PostEventStream Config *********************/
        FlinkKafkaConsumer011<Stream> consumePost = StreamConsumer.createStreamConsumer("post", "localhost:9092", "bar", new PostEventStreamDeserializationSchema());
        consumePost.setStartFromLatest();
        DataStream<Stream> postInputStream = environment.addSource(consumePost);
        //PersistForPost persistForPost = new PersistForPost();
        /***********    COOL STUFF FOR ANALYSIS  ****************/
        //TODO: does NOT work in parallel. first a stream is read and then the next, but the first stram is unbounded...
        /** Assign Timestamps and Watermarks on all the streams **/
        DataStream<Stream> connectedStream = commentInputStream.union(likesInputStream,postInputStream);

        connectedStream
                .assignTimestampsAndWatermarks(streamTimestampAssigner);
        connectedStream.map(x->{
            System.out.println("Serial");
            return x;
        });
        connectedStream.map(x->{
            System.out.println("PARALLEL FROCIO");
            return x;
        });
//                .split(new OutputSelector<Stream>() {
//                    @Override
//                    public Iterable<Stream> select(Stream x) {
//                        List<Stream> output = new ArrayList<Stream>();
//                        if (value % 2 == 0) {
//                            output.add("even");
//                        }
//                        else {
//                            output.add("odd");
//                        }
//                        return output;
//                        if(x instanceof CommentEventStream){
//                            output.add(x);
//                        } else if(x instanceof LikesEventStream){
//                            LikesEventStream tmp = (LikesEventStream) x;
//                            System.out.println(tmp.toString());
//                        } else{
//                            PostEventStream tmp = (PostEventStream) x;
//                            System.out.println(tmp.toString());
//                        }
//                    }
//                });
        /** Comment Stream **/


            //SUM COMMENTS


//                .map(x->{
//                    if(x instanceof CommentEventStream){
//                        CommentEventStream tmp = (CommentEventStream) x;
//                        System.out.println(tmp.toString());
//                    } else if(x instanceof LikesEventStream){
//                        LikesEventStream tmp = (LikesEventStream) x;
//                        System.out.println(tmp.toString());
//                    } else{
//                        PostEventStream tmp = (PostEventStream) x;
//                        System.out.println(tmp.toString());
//                    }
//                    return x;
//                });
                //.assignTimestampsAndWatermarks(water)
                //.map(persistForComment)
//                .keyBy(
//                        new KeySelector<CommentEventStream, Integer>() {
//                            @Override
//                            public int getKey(CommentEventStream commentEventStream) throws Exception {
//                                return commentEventStream.getReply_to_postId();
//                            }
//                        }
//                )


                //.keyBy(x->{if(x instanceof CommentEventStream){CommentEventStream tmp = (CommentEventStream) x; System.out.println(tmp.toString());return tmp.getReply_to_postId();} return -1;})
                //.window(TumblingEventTimeWindows.of(Time.seconds(10)));
                //.process(new NumberOfCommentsFunction())
                //.sum(x->)
                //.print();

            //SUM REPLY TO COMMENTS
        //commentInputStream.flatMap(new NumberOfReplies()).keyBy(0).timeWindow(Time.seconds(60)).sum(3).print();
        /** Likes Stream **/
        //likesInputStream.map(persistForLikes);

        /** Post Stream **/
        //postInputStream.map(persistForPost);


        /******* EXECUTE THE COOL STUFF ABOVE ********/
        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    /**********************************************     FLINK       ************************************************/

    //TODO: set time to the time when the window slides
    public static class NumberOfComments extends RichFlatMapFunction<CommentEventStream, Tuple5<Integer, Date, Integer, Integer, String>> {

        @Override
        public void flatMap(CommentEventStream commentEventStream, Collector<Tuple5<Integer, Date, Integer, Integer, String>> collector) throws Exception {
            if(commentEventStream.getReply_to_postId()!=-1){
                collector.collect(new Tuple5<>(commentEventStream.getReply_to_postId(),new Date(),commentEventStream.getId(),1, "COMMENTS"));
            }
        }
    }
    public static class NumberOfCommentsFunction extends ProcessWindowFunction<
            CommentEventStream,
            Tuple5<Integer, Date, Integer, Integer, String>,
            Integer,
            TimeWindow> {

        @Override
        public void process(Integer integer, Context context, Iterable<CommentEventStream> iterable, Collector<Tuple5<Integer, Date, Integer, Integer, String>> collector) throws Exception {
            int i=0;
            for(CommentEventStream commentEventStream : iterable){
                i++;
                if(commentEventStream.getReply_to_postId()!=-1){
                    collector.collect(new Tuple5<>(commentEventStream.getReply_to_postId(),new Date(context.window().getEnd()),commentEventStream.getId(),i, "COMMENTS"));
                }
            }
        }
    }
    //TODO: set time to the time when the window slides
    public static class NumberOfReplies extends RichFlatMapFunction<CommentEventStream, Tuple5<Integer, Date, Integer, Integer, String>> {
        private transient PostAndCommentRepository postAndCommentRepository;
        @Override
        public void flatMap(CommentEventStream commentEventStream, Collector<Tuple5<Integer, Date, Integer, Integer, String>> collector) throws Exception {
            if(commentEventStream.getReply_to_commentId()!=-1){
                if(postAndCommentRepository==null){
                    postAndCommentRepository = SpringBeansUtil.getBean(PostAndCommentRepository.class);
                }
                Optional<PostAndComment> postAndCommentOptional = postAndCommentRepository.findById(commentEventStream.getId());

                //TODO: ABSOLUTELY CORRECT HERE
                if(postAndCommentOptional.isPresent()){
                    collector.collect(new Tuple5<>(postAndCommentOptional.get().getPostId(),new Date(),commentEventStream.getReply_to_postId(),1, "REPLIES"));
                }
            }
        }
    }

    public class ActivePost implements FilterFunction<PostAndDate>{
        @Override
        public boolean filter(PostAndDate postAndDate) throws Exception {

            long time_diff = (new Date()).getTime()-postAndDate.getLastUpdate().getTime();
            long hours12 = Time.hours(12).toMilliseconds();

            if(time_diff<hours12){
                return true;
            }else{
                return false;
            }
        }
    }
//    public static class Tokenizer implements FlatMapFunction<CommentEventStream, Tuple2<String, Integer>> {
//        @Override
//        public void flatMap(CommentEventStream commentEventStream, Collector<Tuple2<String, Integer>> collector) throws Exception {
//            collector.collect(new Tuple2<>("comment",1));
//        }
//    }
//
//
//    public static class SumCommentsByPostId implements FlatMapFunction<CommentEventStream, Tuple2<Integer, Integer>> {
//        @Override
//        public void flatMap(CommentEventStream commentEventStream, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
//            int postId = commentEventStream.getReply_to_postId();
//            if(postId != -1){
//                collector.collect(new Tuple2<>(postId,1));
//            }
//        }
//    }
//    public static class SumReplayToCommentsByCommentId implements FlatMapFunction<CommentEventStream, Tuple2<Integer, Integer>> {
//        @Override
//        public void flatMap(CommentEventStream commentEventStream, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
//            int commentId = commentEventStream.getReply_to_commentId();
//            if(commentId != -1){
//                collector.collect(new Tuple2<>(commentId,1));
//            }
//        }
//    }

//        public static void doSomethingComment(){
//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        CommentEventStreamConsumer consume = new CommentEventStreamConsumer();
//        FlinkKafkaConsumer011<CommentEventStream> consumer = consume.createCommentEventStreamConsumer("comment","localhost:9092", "bar"); //TODO: change to correct topic
//        consumer.setStartFromLatest(); //TODO: change this based on what is required
//        consumer.assignTimestampsAndWatermarks(new CommentEventStreamTimestampAssigner(Time.milliseconds(300000))); //TODO: check if it works
//
//        DataStream<CommentEventStream> inputStream = environment.addSource(consumer);
//        inputStream.map(new MapFunction<CommentEventStream, Long>(){
//                            @Override
//                            public Long map(CommentEventStream commentEventStream) throws Exception {
//                                //System.out.println(commentEventStream.toString());
//                                return commentEventStream.getSentAt().getTime();
//                            }
//                        }
//        ).print();
//                .timeWindowAll(Time.hours(24))
//                .addSink(flinkKafkaProducer);  //TODO: producer to send back some aggregated statistics
//
//        inputStream.flatMap(new Tokenizer()).keyBy(0).timeWindow(Time.milliseconds(50)).sum(1).print();
//
//        inputStream.keyBy(new KeySelector<CommentEventStream, Integer>() {
//            @Override
//            public Integer getKey(CommentEventStream commentEventStream) throws Exception {
//                return commentEventStream.getReply_to_postId();
//            }
//        }).timeWindow(Time.milliseconds(1000));
//
//        inputStream.flatMap(new SumCommentsByPostId()).keyBy(0).timeWindow(Time.milliseconds(1000)).sum(1).print(); //sum's up comments by post_id
//
//        inputStream.map(new StorePostId());
//
//
//        try {
//            environment.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

//    public static void doSomethingLikes(){
//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        FlinkKafkaConsumer011<LikesEventStream> consumer = LikesEventStreamConsumer.createLikesEventStreamConsumer("likes","localhost:9092", "bar");
//        consumer.setStartFromEarliest();
//        consumer.assignTimestampsAndWatermarks(new LikesEventStreamTimestampAssigner()); //TODO: check if it works
//
//        DataStream<LikesEventStream> inputStream = environment.addSource(consumer);
//        inputStream.map(new MapFunction<LikesEventStream, String>(){
//                            @Override
//                            public String map(LikesEventStream likesEventStream) throws Exception {
//                                System.out.println(likesEventStream.toString());
//                                return likesEventStream.toString();
//                            }
//                        }
//        );
//                .timeWindowAll(Time.hours(24))
//                .addSink(flinkKafkaProducer);  //TODO: producer to send back some aggregated statistics
//
//
//        try {
//            environment.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

//    public static void doSomethingPost(){
//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        FlinkKafkaConsumer011<PostEventStream> consumer = PostEventStreamConsumer.createPostEventStreamConsumer("post", "localhost:9092", "bar");
//        consumer.setStartFromEarliest();
//        consumer.assignTimestampsAndWatermarks(new PostEventStreamTimestampAssigner()); //TODO: check if it works
//
//        DataStream<PostEventStream> inputStream = environment.addSource(consumer);
//        inputStream.map(new MapFunction<PostEventStream, String>(){
//                            @Override
//                            public String map(PostEventStream postEventStream) throws Exception {
//                                System.out.println(postEventStream.toString()); //This is to test what you read from the topic. Make some statistics and print them like this. In a second moment we will look at how to save the results.
//                                return postEventStream.toString();
//                            }
//                        }
//        );
//                .timeWindowAll(Time.hours(24))
//                .addSink(flinkKafkaProducer);  //TODO: producer to send back some aggregated statistics
//        try {
//            environment.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

}