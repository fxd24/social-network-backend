package com.dspa.project.model;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "post_and_comment")
public class PostAndComment {


    private int postId;
    @Id
    private int commentId;

    public int getPostId() {
        return postId;
    }

    public int getCommentId() {
        return commentId;
    }

    public void setPostId(int postId) {
        this.postId = postId;
    }

    public void setCommentId(int commentId) {
        this.commentId = commentId;
    }
}
