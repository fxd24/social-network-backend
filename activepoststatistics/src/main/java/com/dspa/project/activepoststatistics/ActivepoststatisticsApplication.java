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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichProcessWindowFunction;
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

import java.util.*;


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
        StreamTimestampAssigner streamTimestampAssigner = new StreamTimestampAssigner(Time.milliseconds(300000)); //TODO: modify to new value
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

        /** Assign Timestamps and Watermarks on all the streams **/
        DataStream<Stream> connectedStream = commentInputStream.union(likesInputStream,postInputStream).assignTimestampsAndWatermarks(streamTimestampAssigner);

        /** Persist stream information **/
        DataStream<CommentEventStream> commentEventStream =
        connectedStream
                .filter(new CommentEventStreamFilter())
                .map(x->{
                    return (CommentEventStream) x;
                })
                .flatMap(new EarlyReplies())
                .map(persistForComment);


        connectedStream
                .filter(new LikeEventStreamFilter())
                .map(x->{
                    return (LikesEventStream) x;
                })
                .map(persistForLikes);


        connectedStream
                .filter(new PostEventStreamFilter())
                .map(x->{
                    return (PostEventStream) x;
                })
                .map(persistForPost);

        /** Comment Stats **/
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
        commentEventStream
                .filter(new ReplyFilter())
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
                .process(new NumberOfRepliesFunction())
                .print();

        /** Post Stream **/
        connectedStream
                .flatMap(new UserEngagedWithPost())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new UserEngagedWithPostFunction())
                .print();



        /******* EXECUTE THE COOL STUFF ABOVE ********/
        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**********************************************     FLINK       ************************************************/

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
                    //System.out.println("Is the post active? "+IsPostActive.isPostActive(context, commentEventStream.getReply_to_postId()));
                    if(IsPostActive.isPostActive(context, commentEventStream.getReply_to_postId())) {
                        collector.collect(new Tuple5<>(commentEventStream.getReply_to_postId(), new Date(context.window().getEnd()), commentEventStream.getId(), i, "COMMENTS"));
                    }
                }
            }
        }
    }

    public class EarlyReplies extends RichFlatMapFunction<CommentEventStream, CommentEventStream>{
        private ArrayList<CommentEventStream> repliesWithNoPostId;
        @Override
        public void open(Configuration parameters) throws Exception {
            //super.open(parameters);
            repliesWithNoPostId = new ArrayList<>();
        }

        @Override
        public void flatMap(CommentEventStream commentEventStream, Collector<CommentEventStream> collector) throws Exception {
            PostAndCommentRepository postAndCommentRepository = SpringBeansUtil.getBean(PostAndCommentRepository.class);

            if(commentEventStream.getReply_to_postId()==-1){ //is a reply
                checkUnmappedReplies(collector, postAndCommentRepository);

                Optional<PostAndComment> postAndComment = postAndCommentRepository.findById(commentEventStream.getReply_to_commentId());
                if(postAndComment.isPresent()){
                    collector.collect(commentEventStream);
                } else {
                    //System.out.println("Reply has no mapping");
                    repliesWithNoPostId.add(commentEventStream);
                }
            } else{ //is a comment
                collector.collect(commentEventStream);
            }

        }

        private void checkUnmappedReplies(Collector<CommentEventStream> collector, PostAndCommentRepository postAndCommentRepository){
            for(CommentEventStream commentEventStream : repliesWithNoPostId){
                Optional<PostAndComment> postAndComment = postAndCommentRepository.findById(commentEventStream.getReply_to_commentId());
                if(postAndComment.isPresent()){
                    System.out.println("YESSSSSS   Reply now has a mapping");
                    collector.collect(commentEventStream);
                    repliesWithNoPostId.remove(commentEventStream);
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
                postAndComment = postAndCommentRepository.findById(commentEventStream.getReply_to_commentId());
                //System.out.println("Maybe collecting. The value of Optional is: "+postAndComment.isPresent());
                //TODO: ABSOLUTELY CHANGE

                if(postAndComment.isPresent()){
                    //System.out.println("Is the post active? "+IsPostActive.isPostActive(context, postAndComment.get().getPostId()));
                    if(IsPostActive.isPostActive(context, postAndComment.get().getPostId())){
                        collector.collect(new Tuple5<>(postAndComment.get().getPostId(),new Date(context.window().getEnd()),commentEventStream.getId(),i, "REPLIES"));
                    }
                }

            }
        }
    }

    public static class IsPostActive{
        public static boolean isPostActive(ProcessWindowFunction.Context context, Integer postId){

            PostAndDateRepository postAndDateRepository = SpringBeansUtil.getBean(PostAndDateRepository.class);

            long currentTime = context.window().maxTimestamp();
            //System.out.println("The current time is: "+currentTime);
            long postTime = postAndDateRepository.findById(postId).get().getLastUpdate().getTime();
            //System.out.println("The post time is: "+postTime);
            long hours12 = Time.hours(12).toMilliseconds();

            if((postTime+hours12)>currentTime){
                return true;
            }else {
                return false;
            }

        }
    }
    public static class UserEngagedWithPost implements FlatMapFunction<Stream, Tuple3<Integer, Integer, Integer>> {
        private transient PostAndCommentRepository postAndCommentRepository;
        @Override
        public void flatMap(Stream stream, Collector<Tuple3<Integer, Integer, Integer>> collector) throws Exception {

            if(stream instanceof CommentEventStream){
                CommentEventStream tmp = (CommentEventStream) stream;
                //System.out.println(tmp.toString());
                int commentId = tmp.getReply_to_commentId();
                if(commentId != -1){
                    if(postAndCommentRepository==null){
                        postAndCommentRepository = SpringBeansUtil.getBean(PostAndCommentRepository.class);
                    }
                    Optional<PostAndComment> postAndCommentOptional = postAndCommentRepository.findById(commentId);
                    if(postAndCommentOptional.isPresent()){
                        collector.collect(new Tuple3<>(postAndCommentOptional.get().getPostId(),tmp.getPersonId(),1));
                    }

                } else {
                    collector.collect(new Tuple3<>(tmp.getReply_to_postId(),tmp.getPersonId(),1));
                }

            } else if(stream instanceof LikesEventStream){
                LikesEventStream tmp = (LikesEventStream) stream;
                collector.collect(new Tuple3<>(tmp.getPostId(),tmp.getPersonId(),1));
            } else{
                //PostEventStream tmp = (PostEventStream) stream;
                //System.out.println(tmp.toString());
                //TODO: probably do nothing here
            }
        }
    }

    public class UserEngagedWithPostFunction extends ProcessWindowFunction<
            Tuple3<Integer, Integer, Integer>,
            Tuple4<Integer, Date, Integer, String>,
            Tuple,
            TimeWindow> {


        @Override
        public void process(Tuple integer, Context context, Iterable<Tuple3<Integer, Integer, Integer>> iterable, Collector<Tuple4<Integer, Date, Integer, String>> collector) throws Exception {
            int i=0;
            Set<Integer> userIds = new HashSet<>();
            for(Tuple3<Integer, Integer, Integer> t : iterable){
                userIds.add(t.f1);
                //System.out.println(t.f0+" should be the same ");
            }
            //if(IsPostActive.isPostActive(context, postAndComment.get().getPostId())){
//            for(int j=0; iterable.iterator().hasNext(); j++){
//TODO: check if it is the correct post id and also that your assumption is correct
//                System.out.println(iterable.iterator().next().f0);
//
//            }
            if(IsPostActive.isPostActive(context, iterable.iterator().next().f0)) {
                collector.collect(new Tuple4<>(iterable.iterator().next().f0, new Date(context.window().getEnd()), userIds.size(), "USER ENGAGED WITH POST"));
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
    public class ReplyFilter implements FilterFunction<CommentEventStream> {
        @Override
        public boolean filter(CommentEventStream stream) throws Exception {
            return stream.getReply_to_commentId()!=-1;
        }
    }

}