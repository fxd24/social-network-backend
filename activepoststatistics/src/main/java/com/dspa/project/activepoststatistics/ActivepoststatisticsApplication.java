package com.dspa.project.activepoststatistics;

import com.dspa.project.model.CommentEventStream;
import com.dspa.project.model.LikesEventStream;
import com.dspa.project.model.PostEventStream;
import flink.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.lang.Nullable;

@SpringBootApplication
public class ActivepoststatisticsApplication {




    public static void main(String[] args) {
        //Spring
        ConfigurableApplicationContext context = SpringApplication.run(ActivepoststatisticsApplication.class, args);

        /**********     FLINK   START    *********************/

        doSomethingComment();
//        doSomethingLikes();
//        doSomethingPost();

        System.out.println("I work :D");
        /**********     FLINK   END    *********************/

        context.close();

    }

    /**********************************************     FLINK       ************************************************/

    /**
     * TODO: Partially task 1. Do most of the code in the classe in the Flink package.
     * understand what groups are for and how to call them.
     * Ideally we should store the results back into another topic and handle there the windowing
     */
    public static void doSomethingComment(){

        //final StreamExecutionEnvironment environment = StreamExecutionEnvironment
        //		.createRemoteEnvironment( "localhost", 8081 );


        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        CommentEventStreamConsumer consume = new CommentEventStreamConsumer();

        FlinkKafkaConsumer011<CommentEventStream> consumer = consume.createCommentEventStreamConsumer("comment","localhost:9092", "bar"); //TODO: change to correct topic
        consumer.setStartFromLatest(); //TODO: change this based on what is required
        consumer.assignTimestampsAndWatermarks(new CommentEventStreamTimestampAssigner()); //TODO: check if it works

 //       2019-05-20 18:24:11.051 ERROR 8376 --- [ Std. Out (2/8)] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-15, groupId=bar] Offset commit failed on partition comment-0 at offset 53: The coordinator is not aware of this member.
 //       2019-05-20 18:24:11.051  WARN 8376 --- [ Std. Out (2/8)] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-15, groupId=bar] Asynchronous auto-commit of offsets {comment-0=OffsetAndMetadata{offset=53, metadata=''}} failed: Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing the session timeout or by reducing the maximum size of batches returned in poll() with max.poll.records.


        DataStream<CommentEventStream> inputStream = environment.addSource( consumer );

        // Step 1: assign keys:
        DataStream<String> keyedInputSteam = inputStream

                // .assignTimestampsAndWatermarks( new  CommentEventStreamTimestampAssigner() )

                .keyBy(new KeySelector<CommentEventStream, Integer>() { // .getReply_to_postId()
                    public Integer getKey(CommentEventStream c) { return new Integer(c.getPersonId()); }
                })


                .window( TumblingEventTimeWindows.of( Time.seconds(10)) )

                // The operator name Window(TumblingEventTimeWindows(600000), EventTimeTrigger, FoldFunction$1, PassThroughWindowFunction) exceeded the 80 characters length limit and was truncated.

                // required: .reduce/aggregate/fold/apply()
                .fold("start", new FoldFunction<CommentEventStream, String>() {

                    public String fold(String value, CommentEventStream current) {
                        return current.getBrowserUsed() + "-" + value;

                    }
                });

        // try offset 1909863

        keyedInputSteam.print();





        // Step 2: filter operation  (no)

        /*
        DataStream<CommentEventStream> filteredInputSteam = keyedInputSteam

                .filter(new FilterFunction<CommentEventStream>() {
                    @Override
                    public boolean filter(CommentEventStream commentEventStream) throws Exception {
                        return commentEventStream.getReply_to_postId() != -1;
                    }
                });*/

        // Step 3: window operation  (no) + aggregation




        // check:

        /*
        keyedInputSteam.
                // all of them are -1
                map(new MapFunction<CommentEventStream, String>(){
                                @Override
                                public String map(CommentEventStream commentEventStream) throws Exception {
                                    String msg = commentEventStream.getReply_to_postId() + " " +
                                            commentEventStream.getReply_to_commentId() + " " +
                                            commentEventStream.getPersonId()  + " " +

                                            commentEventStream.getCreationDate();
                                    System.out.println(msg);
                                    return commentEventStream.toString() ;
                                }
                            } );
        */

        /*
        inputStream.map(new MapFunction<CommentEventStream, String>(){
                            @Override
                            public String map(CommentEventStream commentEventStream) throws Exception {
                                System.out.println(commentEventStream.toString());
                                return commentEventStream.toString();
                            }
                        }
        );*/




                /*
                .keyBy(new KeySelector<CommentEventStream, Integer>() {
                    public Integer getKey(CommentEventStream c) { return c.getReply_to_postId(); }
                })*/

                //.window( TumblingEventTimeWindows.of( Time.minutes(10)) ) // Time.hours(4))

            /*
                .filter(new FilterFunction<CommentEventStream>() {
                        @Override
                        public boolean filter(CommentEventStream commentEventStream) throws Exception {
                            return commentEventStream.getReply_to_postId() != -1;
                        }
                    })

                .fold( 0, new FoldFunction<CommentEventStream, Integer>() {

                    @Override
                    public Integer fold( Integer acc, CommentEventStream comment ) throws Exception {
                        return acc + 1;
                    }
                })

                */
            /*
                .fold("start", new FoldFunction<CommentEventStream, String>() {

                    public String fold(String value, CommentEventStream current) {
                        return current.getBrowserUsed() + "-" + value;

                    }
                })
                .print();
*/

        //.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator() )


        //inputStream.print();

        // take out elements for which we do not know original post_id
                /*
                .filter(new FilterFunction<CommentEventStream>() {
                    @Override
                    public boolean filter(CommentEventStream commentEventStream) throws Exception {
                        return commentEventStream.getReply_to_postId() != -1;
                    }
                })

                // latest elements arrives at most 3500 ms later -> can change in the code.
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());
        */
        //inputStreamWindowed.print();

        //inputStreamWindowed.print();

        // Defining windows:
        // https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/stream/operators/windows.html#allowed-lateness

        /*

        inputStreamWindowed

                // keyBy post id
                .keyBy(new KeySelector<CommentEventStream, Integer>() {
                    public Integer getKey(CommentEventStream c) { return c.getReply_to_postId(); }
                })


                .window( TumblingEventTimeWindows.of( Time.minutes(4)) ) // Time.hours(4))

                // what to do if event time has not passed yet ?
                // see https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/stream/operators/windows.html#allowed-lateness

                .allowedLateness(Time.hours(12))

                // output late events somewhere on the side
                // .sideOutputLateData(lateOutputTag)

                // REDUCE: https://www.slideshare.net/JamieGrier/counting-elements-in-streams
                // List of functions applied on data streams: https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/stream/operators/index.html

                .fold( 0, new FoldFunction<CommentEventStream, Integer>() {

                    @Override
                    public Integer fold( Integer acc, CommentEventStream comment ) throws Exception {
                        return acc + 1;
                    }
                })

                //.add_sink()
                //.setParallelism(1);
                //.addSink( myProducer );

                .print();


    //          .timeWindowAll(Time.hours(24))
    //          .addSink(flinkKafkaProducer);  //TODO: producer to send back some aggregated statistics
    */

        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void doSomethingLikes(){


        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer011<LikesEventStream> consumer = LikesEventStreamConsumer.createLikesEventStreamConsumer("likes","localhost:9092", "bar");
        consumer.setStartFromEarliest();
        consumer.assignTimestampsAndWatermarks(new LikesEventStreamTimestampAssigner()); //TODO: check if it works

        DataStream<LikesEventStream> inputStream = environment.addSource(consumer);
        inputStream.map(new MapFunction<LikesEventStream, String>(){
                            @Override
                            public String map(LikesEventStream likesEventStream) throws Exception {
                                System.out.println(likesEventStream.toString());
                                return likesEventStream.toString();
                            }
                        }
        );
//                .timeWindowAll(Time.hours(24))
//                .addSink(flinkKafkaProducer);  //TODO: producer to send back some aggregated statistics


        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void doSomethingPost(){
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer011<PostEventStream> consumer = PostEventStreamConsumer.createPostEventStreamConsumer("post", "localhost:9092", "bar");
        consumer.setStartFromEarliest();
        consumer.assignTimestampsAndWatermarks(new PostEventStreamTimestampAssigner()); //TODO: check if it works

        DataStream<PostEventStream> inputStream = environment.addSource(consumer);
        inputStream.map(new MapFunction<PostEventStream, String>(){
                            @Override
                            public String map(PostEventStream postEventStream) throws Exception {
                                System.out.println(postEventStream.toString()); //This is to test what you read from the topic. Make some statistics and print them like this. In a second moment we will look at how to save the results.
                                return postEventStream.toString();
                            }
                        }
        );
//                .timeWindowAll(Time.hours(24))
//                .addSink(flinkKafkaProducer);  //TODO: producer to send back some aggregated statistics

        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
