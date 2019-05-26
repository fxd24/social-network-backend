package com.dspa.project.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

@JsonDeserialize(builder = CommentEventStream.Builder.class)
public class CommentEventStream extends Stream{
    private final int id;
    private final int personId;
    private final String creationDate;
    private final String locationIP;
    private final String browserUsed;
    private final String content;
    private final int reply_to_postId;
    private final int reply_to_commentId;
    private final int placeId;

    private CommentEventStream(final Builder builder) {
        this.id = builder.id;
        this.personId = builder.personId;
        this.creationDate = builder.creationDate;
        this.locationIP = builder.locationIP;
        this.browserUsed = builder.browserUsed;
        this.content = builder.content;
        this.reply_to_postId = builder.reply_to_postId;
        this.reply_to_commentId = builder.reply_to_commentId;
        this.placeId = builder.placeId;
    }

    public int getId() {
        return id;
    }

    public int getPersonId() {
        return personId;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public String getLocationIP() {
        return locationIP;
    }

    public String getBrowserUsed() {
        return browserUsed;
    }

    public String getContent() {
        return content;
    }

    public int getReply_to_postId() {
        return reply_to_postId;
    }

    public int getReply_to_commentId() {
        return reply_to_commentId;
    }

    public int getPlaceId() {
        return placeId;
    }

    //TODO: duplicated across Stream objects. Clean this mess.
    public Date getSentAt() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        Date d = null;
        try {
            d = sdf.parse(creationDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return d;
    }

    @Override
    public String toString() {
        return "CommentEventStream{" +
                "id=" + id +
                ", personId=" + personId +
                ", creationDate='" + creationDate + '\'' +
                ", locationIP='" + locationIP + '\'' +
                ", browserUsed='" + browserUsed + '\'' +
                ", content='" + content + '\'' +
                ", reply_to_postId=" + reply_to_postId +
                ", reply_to_commentId=" + reply_to_commentId +
                ", placeId=" + placeId +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommentEventStream that = (CommentEventStream) o;
        return getId() == that.getId() &&
                getPersonId() == that.getPersonId() &&
                getReply_to_postId() == that.getReply_to_postId() &&
                getReply_to_commentId() == that.getReply_to_commentId() &&
                getPlaceId() == that.getPlaceId() &&
                Objects.equals(getCreationDate(), that.getCreationDate()) &&
                Objects.equals(getLocationIP(), that.getLocationIP()) &&
                Objects.equals(getBrowserUsed(), that.getBrowserUsed()) &&
                Objects.equals(getContent(), that.getContent());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getPersonId(), getCreationDate(), getLocationIP(), getBrowserUsed(), getContent(), getReply_to_postId(), getReply_to_commentId(), getPlaceId());
    }

    @JsonPOJOBuilder(buildMethodName = "build")
    public static class Builder {
        @JsonProperty("id")
        private int id;
        @JsonProperty("personId")
        private int personId;
        @JsonProperty("creationDate")
        private String creationDate;
        @JsonProperty("locationIP")
        private String locationIP;
        @JsonProperty("browserUsed")
        private String browserUsed;
        @JsonProperty("content")
        private String content;
        @JsonProperty("reply_to_postId")
        private int reply_to_postId;
        @JsonProperty("reply_to_commentId")
        private int reply_to_commentId;
        @JsonProperty("placeId")
        private int placeId;

        public Builder id(final int id) {
            this.id = id;
            return this;
        }

        public Builder personId(final int personId) {
            this.personId = personId;
            return this;
        }

        public Builder creationDate(final String creationDate) {
            this.creationDate = creationDate;
            return this;
        }

        public Builder locationIP(final String locationIP) {
            this.locationIP = locationIP;
            return this;
        }

        public Builder browserUsed(final String browserUsed) {
            this.browserUsed = browserUsed;
            return this;
        }

        public Builder content(final String content) {
            this.content = content;
            return this;
        }

        public Builder reply_to_postId(final int reply_to_postId) {
            this.reply_to_postId = reply_to_postId;
            return this;
        }

        public Builder reply_to_commentId(final int reply_to_commentId) {
            this.reply_to_commentId = reply_to_commentId;
            return this;
        }

        public Builder placeId(final int placeId) {
            this.placeId = placeId;
            return this;
        }

        public CommentEventStream build() {
            return new CommentEventStream(this);
        }
    }

}

