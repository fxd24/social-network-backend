package com.dspa.project.activepoststatistics.flink;

import com.dspa.project.activepoststatistics.SpringBeansUtil;
import com.dspa.project.activepoststatistics.repo.PostAndCommentRepository;
import com.dspa.project.activepoststatistics.repo.PostAndDateRepository;
import com.dspa.project.model.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class PersistForPost implements MapFunction<Stream,Stream> {
    @Autowired
    public transient PostAndDateRepository postAndDateRepository;
    @Autowired
    public transient PostAndCommentRepository postAndCommentRepository;


    @Override
    public Stream map(Stream stream) throws Exception {
        PostAndDate postAndDate = new PostAndDate();

        if(this.postAndDateRepository==null){
            postAndDateRepository = SpringBeansUtil.getBean(PostAndDateRepository.class);
        }
        if(this.postAndCommentRepository==null){
            postAndCommentRepository = SpringBeansUtil.getBean(PostAndCommentRepository.class);
        }
        postAndDateRepository.flush();
        if(stream instanceof CommentEventStream){
            CommentEventStream commentEventStream = (CommentEventStream) stream;

                if(commentEventStream.getReply_to_postId()!=-1) {
                    Optional<PostAndDate> postAndDateOptional = postAndDateRepository.findById(commentEventStream.getReply_to_postId());
                    if(postAndDateOptional.isPresent()) {
                        if (postAndDateOptional.get().getLastUpdate().getTime() < commentEventStream.getSentAt().getTime()) {
                            postAndDate.setId(commentEventStream.getReply_to_postId());
                            postAndDate.setLastUpdate(commentEventStream.getSentAt());
                            postAndDateRepository.saveAndFlush(postAndDate);
                        }
                    }else{
                        postAndDate.setId(commentEventStream.getReply_to_postId());
                        postAndDate.setLastUpdate(commentEventStream.getSentAt());
                        postAndDateRepository.saveAndFlush(postAndDate);
                    }
                } else if(commentEventStream.getReply_to_commentId()!=-1){
                    Optional<PostAndComment> postAndCommentOptional = postAndCommentRepository.findById(commentEventStream.getReply_to_commentId());
                    //if(!postAndCommentOptional.isPresent()) System.out.println("The value of the post_id of the reply is not yet stored");
                    if(postAndCommentOptional.isPresent()){
                        Optional<PostAndDate> postAndDateOptional = postAndDateRepository.findById(postAndCommentOptional.get().getPostId());
                        if(postAndDateOptional.isPresent()) {
                            if (postAndDateOptional.get().getLastUpdate().getTime() < commentEventStream.getSentAt().getTime()) {
                                postAndDate.setId(postAndCommentOptional.get().getPostId());
                                postAndDate.setLastUpdate(commentEventStream.getSentAt());
                                postAndDateRepository.saveAndFlush(postAndDate);
                            }
                        }else{
                            postAndDate.setId(postAndCommentOptional.get().getPostId());
                            postAndDate.setLastUpdate(commentEventStream.getSentAt());
                            postAndDateRepository.saveAndFlush(postAndDate);
                        }
                    }
                }


        }else if(stream instanceof LikesEventStream){
            LikesEventStream likesEventStream = (LikesEventStream) stream;

            Optional<PostAndDate> postAndDateOptional = postAndDateRepository.findById(likesEventStream.getPostId());
            if(postAndDateOptional.isPresent()){
                //System.out.println("PRINT updating the value PersistForPost");
                //if the timestamp is newer
                if(postAndDateOptional.get().getLastUpdate().getTime()<likesEventStream.getSentAt().getTime()){
                    postAndDate.setId(likesEventStream.getPostId());
                    postAndDate.setLastUpdate(likesEventStream.getSentAt());
                    postAndDateRepository.saveAndFlush(postAndDate);
                }
            }else {
                //System.out.println("PRINT SAVING the value PersistForPost");
                postAndDate.setId(likesEventStream.getPostId());
                postAndDate.setLastUpdate(likesEventStream.getSentAt());
                postAndDateRepository.saveAndFlush(postAndDate);
            }
        }else{
            PostEventStream postEventStream = (PostEventStream) stream;

            Optional<PostAndDate> postAndDateOptional = postAndDateRepository.findById(postEventStream.getId());
            if(postAndDateOptional.isPresent()){
                //System.out.println("PRINT updating the value PersistForPost");
                //if the timestamp is newer
                if(postAndDateOptional.get().getLastUpdate().getTime()<postEventStream.getSentAt().getTime()){
                    postAndDate.setId(postEventStream.getId());
                    postAndDate.setLastUpdate(postEventStream.getSentAt());
                    postAndDateRepository.saveAndFlush(postAndDate);
                }
            }else{
                //System.out.println("PRINT SAVING the value PersistForPost");
                postAndDate.setId(postEventStream.getId());
                postAndDate.setLastUpdate(postEventStream.getSentAt());
                postAndDateRepository.saveAndFlush(postAndDate);
            }

        }
        postAndDate=null;


        return stream;
    }
}
