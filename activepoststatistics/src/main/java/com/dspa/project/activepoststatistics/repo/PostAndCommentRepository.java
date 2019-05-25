package com.dspa.project.activepoststatistics.repo;

import com.dspa.project.model.PostAndComment;
import org.springframework.data.repository.CrudRepository;

public interface PostAndCommentRepository extends CrudRepository<PostAndComment, Integer> {
}
