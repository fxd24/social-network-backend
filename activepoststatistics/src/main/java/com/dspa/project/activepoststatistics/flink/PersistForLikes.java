package com.dspa.project.activepoststatistics.flink;

import com.dspa.project.activepoststatistics.repo.PostAndDateRepository;
import com.dspa.project.model.LikesEventStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PersistForLikes implements MapFunction<LikesEventStream,LikesEventStream> {

    @Autowired
    public transient PostAndDateRepository postAndDateRepository;

    @Override
    public LikesEventStream map(LikesEventStream likesEventStream) throws Exception {
//        PostAndDate postAndDate = new PostAndDate();
//
//        if(this.postAndDateRepository==null){
//            postAndDateRepository = SpringBeansUtil.getBean(PostAndDateRepository.class);
//        }
//
//        if(postAndDateRepository!=null){
//            Optional<PostAndDate> postAndDateOptional = postAndDateRepository.findById(likesEventStream.getPostId());
//            if(postAndDateOptional.isPresent()){
//                //System.out.println("PRINT updating the value PersistForLikes");
//                //postAndDateRepository.setLastUpdate(likesEventStream.getPostId(),likesEventStream.getSentAt());
//                postAndDate.setId(likesEventStream.getPostId());
//                postAndDate.setLastUpdate(likesEventStream.getSentAt());
//                postAndDateRepository.save(postAndDate);
//            }else{
//                //System.out.println("PRINT SAVING the value PersistForLikes");
//                postAndDate.setId(likesEventStream.getPostId());
//                postAndDate.setLastUpdate(likesEventStream.getSentAt());
//                postAndDateRepository.save(postAndDate);
//            }
//        }
        return likesEventStream;
    }
}
