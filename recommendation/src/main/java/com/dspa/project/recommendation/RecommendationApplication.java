package com.dspa.project.recommendation;

import com.dspa.project.common.deserialization.LikesEventStreamDeserializationSchema;
import com.dspa.project.model.LikesEventStream;
import com.dspa.project.model.PersonHasInterestTag;
import com.dspa.project.model.Stream;
import com.dspa.project.recommendation.repository.PersonHasInterestTagRepository;
import flink.StreamConsumer;
import flink.StreamTimestampAssigner;
import javafx.util.Pair;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.*;


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
        StreamTimestampAssigner streamTimestampAssigner = new StreamTimestampAssigner(Time.milliseconds(300000));

        /*******************  LikesEventStream Config *********************/
        FlinkKafkaConsumer011<Stream> consumeLikes = StreamConsumer.createStreamConsumer("likes", "localhost:9092", "recommender", new LikesEventStreamDeserializationSchema());
        consumeLikes.setStartFromLatest();
        //consumeLikes.setStartFromEarliest();
        DataStream<Stream> likesInputStream = environment.addSource(consumeLikes);

        /***********    COOL STUFF FOR RECOMMENDATION  ****************/

        int[] personIds = {47, 192, 265, 395, 434, 581, 650, 724, 838, 913};

        Map<Integer, Map<Integer,Integer>> userAndSimilarUserCount = new HashMap<>(); //<User for recommendation, Tuple2<Possible similar user, count of how many equal likes they have>>
        for(int i : personIds){
            userAndSimilarUserCount.put(i,new HashMap<>());
        }
        likesInputStream
                .assignTimestampsAndWatermarks(streamTimestampAssigner)
                .keyBy(x->{return ((LikesEventStream) x).getPersonId();})
                .window(SlidingEventTimeWindows.of(Time.hours(4),Time.hours(1)))
                .process(new RecommendationFunction(/*userAndSimilarUserCount*/))
                .print()
                ;
        /******* EXECUTE THE COOL STUFF ABOVE ********/
        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public class RecommendationFunction extends ProcessWindowFunction<
            Stream,
            Tuple4<Integer, Date, Map<Integer, Integer>, String>,
            Integer,
            TimeWindow> {
//        private final Map<Integer, Map<Integer,Integer>> userAndSimilarUserCount;

//        public RecommendationFunction(Map<Integer, Map<Integer, Integer>> userAndSimilarUserCount) {
//            this.userAndSimilarUserCount = userAndSimilarUserCount;
//        }

        @Override
        public void process(Integer integer, Context context, Iterable<Stream> iterable, Collector<Tuple4<Integer, Date, Map<Integer, Integer>, String>> collector) throws Exception {
            Map<Integer, List<Integer>> postWithUserLikes = new HashMap<>();
            Map<Integer, List<Integer>> userWithPostHeLikes = new HashMap<>();
            Map<Integer, Map<Integer,Integer>> userAndSimilarUserCount = /*this.userAndSimilarUserCount;//*/new HashMap<>(); //<User for recommendation, Tuple2<Possible similar user, count of how many equal likes they have>>

            Integer postId, userId;
            LikesEventStream likesEventStream;
            /***** BUILD HashMaps for recommentdation ****/
            for(Stream stream : iterable){
                likesEventStream = (LikesEventStream) stream;
                postId = likesEventStream.getPostId();
                userId = likesEventStream.getPersonId();

                if(postWithUserLikes.containsKey(postId)){
                    postWithUserLikes.get(postId).add(userId);
                } else{
                    List<Integer> user_list = new ArrayList<>();
                    user_list.add(userId);
                    postWithUserLikes.put(postId, user_list);
                }

                if(userWithPostHeLikes.containsKey(userId)){
                    userWithPostHeLikes.get(userId).add(postId);
                } else{
                    List<Integer> post_list = new ArrayList<>();
                    post_list.add(postId);
                    userWithPostHeLikes.put(userId, post_list);
                }
            }

            /*** COUNT HOW MANY LIKES BOTH USERS HAVE FOR A COMMON POST ****/
            int[] personIds = {47};//, 192, 265, 395, 406, 581, 650, 724, 838, 913};    //User we are making the recommendation for.
            List<Integer> likedPosts;
            List<Integer> userLikedPost;
            Set<Integer> sameInterest;
            for(Integer user : personIds){
                likedPosts = userWithPostHeLikes.get(user);
                if(likedPosts != null){
                    for(Integer liked_post_id : likedPosts){
                        userLikedPost = postWithUserLikes.get(liked_post_id);
                        for(Integer maybe_similar_user : userLikedPost){
                            updateSimilarUserCount(userAndSimilarUserCount, user, maybe_similar_user);
                        }
                        sameInterest = sameInterestTag(user);
                        for(Integer maybe_similar_user : sameInterest){
                            updateSimilarUserCount(userAndSimilarUserCount, user, maybe_similar_user);
                        }
                    }
                    /****  COLLECT RESULTS *****/
                    collector.collect(new Tuple4(user, new Date(context.window().getEnd()), get5MostSimilarUser(userAndSimilarUserCount.get(user), user), "Recommendation"));
                }
            }
        }

        private void updateSimilarUserCount(Map<Integer, Map<Integer,Integer>> userAndSimilarUserCount, Integer user, Integer maybe_similar_user){
            if(userAndSimilarUserCount.containsKey(user)){
                if(userAndSimilarUserCount.get(user).containsKey(maybe_similar_user)){
                    userAndSimilarUserCount.get(user).put(maybe_similar_user,userAndSimilarUserCount.get(user).get(maybe_similar_user)+1);
                }else{
                    userAndSimilarUserCount.get(user).put(maybe_similar_user,1);
                }
            }else{
                userAndSimilarUserCount.put(user, new HashMap<>());
                userAndSimilarUserCount.get(user).put(maybe_similar_user,1);
            }
        }

        private Map<Integer,Integer> get5MostSimilarUser(Map<Integer,Integer> userAndSimilarUserCount, Integer user){
            Map<Integer,Integer> copy = new HashMap<>(userAndSimilarUserCount);
            Map<Integer,Integer> result = new HashMap<>();

            //System.out.println(result.toString());
            Comparator<Pair<Integer,Integer>> comparator = (Pair<Integer,Integer> p1, Pair<Integer,Integer> p2)-> {
                int val = p1.getValue().compareTo(p2.getValue());
                if (val==0){
                    return 0;
                }else if(val==-1){
                    return 1;
                } else {
                    return -1;
                }

            };

            PriorityQueue<Pair<Integer,Integer>> orderedResult = new PriorityQueue<>(8, comparator);
            for(Map.Entry<Integer, Integer> entry : copy.entrySet()){
                Integer key = entry.getKey();
                //remove if already friends
                if(isFriend(user,key)){
                    result.remove(key);
                }else{
                    orderedResult.add(new Pair<>(key, entry.getValue()));
                }
            }
            //add most similar users
            int size = orderedResult.size();
            if(size>6) size=6;
            for(int i=0; i<size; i++){
                Pair<Integer,Integer> tmp = orderedResult.poll();
                result.put(tmp.getKey(), tmp.getValue());
            }
            if(size<6){//if not enough data to determine 5 similar user, choose randomly the rest
                for(int i=0; i<6-size; i++)
                result.put(i, 0);
            }
            return result;
        }

        //TODO
        private boolean isFriend(Integer userId1, Integer userId2){
            return false;
        }

        private Set<Integer> sameInterestTag(Integer user){
            PersonHasInterestTagRepository personHasInterestTagRepository = SpringBeansUtil.getBean(PersonHasInterestTagRepository.class);

            Iterable<PersonHasInterestTag> iterInterest = personHasInterestTagRepository.findAll();
            List<Integer> userInterests = new ArrayList<>();
            Set<Integer> similarUser = new HashSet<>();
            for(PersonHasInterestTag p : iterInterest){
                if(p.getId().personId==user){
                    userInterests.add(p.getId().tagId);
                }
            }
            for(PersonHasInterestTag p : iterInterest){
                for(Integer interest : userInterests){
                    if(p.getId().personId!=user && (interest==p.getId().tagId)){
                        similarUser.add(p.getId().personId);
                    }
                }
            }

            return similarUser;
        }
    }

}
