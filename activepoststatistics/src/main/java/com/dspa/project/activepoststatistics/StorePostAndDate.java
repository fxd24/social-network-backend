package com.dspa.project.activepoststatistics;

import com.dspa.project.activepoststatistics.repo.PostAndDateRepository;
import com.dspa.project.model.CommentEventStream;
import com.dspa.project.model.PostAndDate;
import org.apache.flink.api.common.functions.MapFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StorePostAndDate implements MapFunction<CommentEventStream, CommentEventStream> {
    @Autowired
    public transient PostAndDateRepository repository;

    @Autowired
    public StorePostAndDate(PostAndDateRepository repository) {
        this.repository = repository;
        if(this.repository==null){
            repository = SpringBeansUtil.getBean(PostAndDateRepository.class);
        }

    }


    @Override
    public CommentEventStream map(CommentEventStream commentEventStream) throws Exception {
//            System.out.println("Saving the value");
        PostAndDate postAndDate = new PostAndDate();
        postAndDate.setId(commentEventStream.getReply_to_postId());
        postAndDate.setLastUpdate(commentEventStream.getSentAt());
        if(this.repository==null){
            repository = SpringBeansUtil.getBean(PostAndDateRepository.class);
        }
        if(repository != null) {
            if(commentEventStream.getReply_to_postId()!=-1) {
                repository.save(postAndDate);
            }
        } else{
            System.out.println("CLASS: StorePostAndDate| repository variable is null!");
        }
        return commentEventStream;
    }

}