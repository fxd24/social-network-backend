package com.dspa.project.activepoststatistics.flink;

import com.dspa.project.activepoststatistics.SpringBeansUtil;
import com.dspa.project.activepoststatistics.repo.CommentAndReplyRepository;
import com.dspa.project.activepoststatistics.repo.PostAndCommentRepository;
import com.dspa.project.model.CommentAndReply;
import com.dspa.project.model.CommentEventStream;
import com.dspa.project.model.PostAndComment;
import org.apache.flink.api.common.functions.MapFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class PersistForComment implements MapFunction<CommentEventStream, CommentEventStream> {

    @Autowired
    public transient PostAndCommentRepository postAndCommentRepository;
    @Autowired
    public transient CommentAndReplyRepository commentAndReplyRepository;


    @Override
    public CommentEventStream map(CommentEventStream commentEventStream) throws Exception {

        if(this.postAndCommentRepository==null){
            postAndCommentRepository = SpringBeansUtil.getBean(PostAndCommentRepository.class);
        }
        if(this.commentAndReplyRepository==null){
            commentAndReplyRepository = SpringBeansUtil.getBean(CommentAndReplyRepository.class);
        }

        if(postAndCommentRepository!= null && commentAndReplyRepository!=null) {
            if(commentEventStream.getReply_to_postId()!=-1) {
                //System.out.println("PRINT SAVING the value");
                PostAndComment postAndComment = new PostAndComment();
                postAndComment.setPostId(commentEventStream.getReply_to_postId());
                postAndComment.setCommentId(commentEventStream.getId());

                postAndCommentRepository.save(postAndComment);
            } else if(commentEventStream.getReply_to_commentId()!=-1){
                //System.out.println("PRINT SAVING the value");
                Optional<PostAndComment> postAndCommentOptional = postAndCommentRepository.findById(commentEventStream.getReply_to_commentId());
                //if(!postAndCommentOptional.isPresent()) System.out.println("The value of the post_id of the reply is not yet stored");

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