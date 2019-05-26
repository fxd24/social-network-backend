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
import flink.StreamConsumer;
import flink.StreamTimestampAssigner;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
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

import java.util.Date;
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
        consumeComment.setStartFromEarliest(); //TODO: change this based on what is required
        DataStream<Stream> commentInputStream = environment.addSource(consumeComment);
        PersistForComment persistForComment = new PersistForComment();
        /*******************  LikesEventStream Config *********************/
        FlinkKafkaConsumer011<Stream> consumeLikes = StreamConsumer.createStreamConsumer("likes", "localhost:9092", "bar", new LikesEventStreamDeserializationSchema());
        consumeLikes.setStartFromEarliest();
        DataStream<Stream> likesInputStream = environment.addSource(consumeLikes);
        PersistForLikes persistForLikes = new PersistForLikes();
        /*******************  PostEventStream Config *********************/
        FlinkKafkaConsumer011<Stream> consumePost = StreamConsumer.createStreamConsumer("post", "localhost:9092", "bar", new PostEventStreamDeserializationSchema());
        consumePost.setStartFromEarliest();
        DataStream<Stream> postInputStream = environment.addSource(consumePost);
        PersistForPost persistForPost = new PersistForPost();
        /***********    COOL STUFF FOR ANALYSIS  ****************/
        //TODO: does NOT work in parallel. first a stream is read and then the next, but the first stram is unbounded...
        /** Assign Timestamps and Watermarks on all the streams **/
        DataStream<Stream> connectedStream = commentInputStream.union(likesInputStream,postInputStream).assignTimestampsAndWatermarks(streamTimestampAssigner);;

        /** Persist stream information **/
        DataStream<CommentEventStream> commentEventStream =
        connectedStream
                .filter(new CommentEventStreamFilter())
                .map(x->{
                    return (CommentEventStream) x;
                })
                .map(persistForComment);

//        connectedStream
//                .filter(new LikeEventStreamFilter())
//                .map(x->{
//                    return (LikesEventStream) x;
//                })
//                .map(persistForLikes);
//
//        connectedStream
//                .filter(new PostEventStreamFilter())
//                .map(x->{
//                    return (PostEventStream) x;
//                })
//                .map(persistForPost);

        /** Comment Stats **/
            //SUM COMMENTS
        commentEventStream
                .filter(new CommentFilter())
                .map(x->{
                    //System.out.println("Mapping"+x.toString());
                    return (CommentEventStream) x;
                })
                .keyBy(
                        new KeySelector<CommentEventStream, Integer>() {
                            @Override
                            public Integer getKey(CommentEventStream commentEventStream) throws Exception {
                                //System.out.println("Selecting key");
                                return commentEventStream.getReply_to_postId();
                            }
                        }
                )
                .window(TumblingEventTimeWindows.of(Time.minutes(30)))
                .process(new NumberOfCommentsFunction())
                .print();

        /** Replies Stats **/
//        DataStream<CommentEventStream> replyEventStream =
//                connectedStream
//                        .filter(new ReplyFilter())
//                        .map(x->{
//                            System.out.println(x.toString());
//                            return (CommentEventStream) x;
//                        });


                //.map(persistForComment)
//                .keyBy(
//                        new KeySelector<CommentEventStream, Integer>() {
//                            @Override
//                            public int getKey(CommentEventStream commentEventStream) throws Exception {
//                                return commentEventStream.getReply_to_postId();
//                            }
//                        }
//                )


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
    public class NumberOfCommentsFunction extends ProcessWindowFunction<
            CommentEventStream,
            Tuple5<Integer, Date, Integer, Integer, String>,
            Integer,
            TimeWindow> {

        @Override
        public void process(Integer integer, Context context, Iterable<CommentEventStream> iterable, Collector<Tuple5<Integer, Date, Integer, Integer, String>> collector) throws Exception {
            int i=0;
            //System.out.println("Collecting");
            for(CommentEventStream commentEventStream : iterable){
                i++;
                if(commentEventStream.getReply_to_postId()!=-1){

                    collector.collect(new Tuple5<>(commentEventStream.getReply_to_postId(),new Date(context.window().getEnd()),commentEventStream.getId(),i, "COMMENTS"));
                }
            }
        }
    }
    public class NumberOfRepliesFunction extends ProcessWindowFunction<
            CommentEventStream,
            Tuple5<Integer, Date, Integer, Integer, String>,
            Integer,
            TimeWindow> {
        private transient PostAndCommentRepository postAndCommentRepository;
        @Override
        public void process(Integer integer, Context context, Iterable<CommentEventStream> iterable, Collector<Tuple5<Integer, Date, Integer, Integer, String>> collector) throws Exception {
            int i=0;
            if(postAndCommentRepository==null){
                postAndCommentRepository = SpringBeansUtil.getBean(PostAndCommentRepository.class);
            }
            Optional<PostAndComment> postAndComment;
            for(CommentEventStream commentEventStream : iterable){
                i++;
                postAndComment = postAndCommentRepository.findById(commentEventStream.getReply_to_postId());
                if(postAndComment.isPresent()){
                    collector.collect(new Tuple5<>(postAndComment.get().getPostId(),new Date(context.window().getEnd()),commentEventStream.getId(),i, "REPLIES"));
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

    public class CommentEventStreamFilter implements FilterFunction<Stream> {
        @Override
        public boolean filter(Stream stream) throws Exception {
            boolean bool = (stream instanceof CommentEventStream);
            return bool;
        }
    }
    public class LikeEventStreamFilter implements FilterFunction<Stream> {
        @Override
        public boolean filter(Stream stream) throws Exception {
            return (stream instanceof LikesEventStream);
        }
    }
    public class PostEventStreamFilter implements FilterFunction<Stream> {
        @Override
        public boolean filter(Stream stream) throws Exception {
            return (stream instanceof PostEventStream);
        }
    }
    public class CommentFilter implements FilterFunction<CommentEventStream> {
        @Override
        public boolean filter(CommentEventStream stream) throws Exception {
            return (stream.getReply_to_postId()!=-1);
        }
    }
    public class ReplyFilter implements FilterFunction<Stream> {
        @Override
        public boolean filter(Stream stream) throws Exception {
            return (stream instanceof CommentEventStream) && (((CommentEventStream) stream).getReply_to_commentId()!=-1);
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



}