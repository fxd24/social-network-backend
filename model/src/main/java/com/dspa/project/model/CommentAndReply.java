package com.dspa.project.model;

import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "CommentAndReply")
public class CommentAndReply {

    private int commentId;

    private int replyId;

    public int getCommentId() {
        return commentId;
    }

    public int getReplyId() {
        return replyId;
    }

    public void setCommentId(int commentId) {
        this.commentId = commentId;
    }

    public void setReplyId(int replyId) {
        this.replyId = replyId;
    }
}
