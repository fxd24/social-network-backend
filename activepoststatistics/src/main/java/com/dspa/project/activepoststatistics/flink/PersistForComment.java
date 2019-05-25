package com.dspa.project.activepoststatistics.flink;

import com.dspa.project.activepoststatistics.SpringBeansUtil;
import com.dspa.project.activepoststatistics.repo.CommentAndReplyRepository;
import com.dspa.project.activepoststatistics.repo.PostAndCommentRepository;
import com.dspa.project.activepoststatistics.repo.PostAndDateRepository;
import com.dspa.project.model.CommentAndReply;
import com.dspa.project.model.CommentEventStream;
import com.dspa.project.model.PostAndComment;
import com.dspa.project.model.PostAndDate;
import org.apache.flink.api.common.functions.MapFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class PersistForComment implements MapFunction<CommentEventStream, CommentEventStream> {
    @Autowired
    public transient PostAndDateRepository postAndDateRepository;
    @Autowired
    public transient PostAndCommentRepository postAndCommentRepository;
    @Autowired
    public transient CommentAndReplyRepository commentAndReplyRepository;

//    @Autowired
//    public PersistForComment(PostAndDateRepository repository) {
//        this.postAndDateRepository = repository;
//        if(this.postAndDateRepository==null){
//            repository = SpringBeansUtil.getBean(PostAndDateRepository.class);
//        }
//
//    }


    @Override
    public CommentEventStream map(CommentEventStream commentEventStream) throws Exception {
//            System.out.println("Saving the value");
        PostAndDate postAndDate = new PostAndDate();

        if(this.postAndDateRepository==null){
            postAndDateRepository = SpringBeansUtil.getBean(PostAndDateRepository.class);
        }
        if(this.postAndCommentRepository==null){
            postAndCommentRepository = SpringBeansUtil.getBean(PostAndCommentRepository.class);
        }
        if(this.commentAndReplyRepository==null){
            commentAndReplyRepository = SpringBeansUtil.getBean(CommentAndReplyRepository.class);
        }

        if(postAndDateRepository != null && postAndCommentRepository!= null && commentAndReplyRepository!=null) {
            if(commentEventStream.getReply_to_postId()!=-1) {
                postAndDate.setId(commentEventStream.getReply_to_postId());
                postAndDate.setLastUpdate(commentEventStream.getSentAt());
                postAndDateRepository.save(postAndDate);

                PostAndComment postAndComment = new PostAndComment();
                postAndComment.setPostId(commentEventStream.getReply_to_postId());
                postAndComment.setCommentId(commentEventStream.getId());

                postAndCommentRepository.save(postAndComment);
            } else if(commentEventStream.getReply_to_commentId()!=-1){

                Optional<PostAndComment> postAndCommentOptional = postAndCommentRepository.findById(commentEventStream.getId());
                if(postAndCommentOptional.isPresent()){
                    postAndDate.setId(postAndCommentOptional.get().getPostId());
                    postAndDate.setLastUpdate(commentEventStream.getSentAt());
                    postAndDateRepository.save(postAndDate);
                }
                //TODO: add the possibility to put into a queue if post is not available yet.

                CommentAndReply commentAndReply = new CommentAndReply();
                commentAndReply.setCommentId(commentEventStream.getId());
                commentAndReply.setReplyId(commentEventStream.getReply_to_commentId());
                commentAndReplyRepository.save(commentAndReply);

            }
        } else{
            System.out.println("CLASS: StorePostAndDate| repository variable is null!");
        }
        return commentEventStream;
    }

}