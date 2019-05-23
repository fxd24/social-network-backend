package com.dspa.project.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

@JsonDeserialize(builder = LikesEventStream.Builder.class)
public class LikesEventStream implements Stream {
    private final int personId;
    private final int postId;
    private final String creationDate;

    public LikesEventStream(final Builder builder) {
        this.personId = builder.personId;
        this.postId = builder.postId;
        this.creationDate = builder.creationDate;
    }

    public int getPersonId() {
        return personId;
    }

    public int getPostId() {
        return postId;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Date getSentAt() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        Date d = null;
        try {
            d = sdf.parse(creationDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return d;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LikesEventStream that = (LikesEventStream) o;
        return getPersonId() == that.getPersonId() &&
                getPostId() == that.getPostId() &&
                getCreationDate().equals(that.getCreationDate());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPersonId(), getPostId(), getCreationDate());
    }

    @Override
    public String toString() {
        return "LikesEventStream{" +
                "personId=" + personId +
                ", postId=" + postId +
                ", creationDate='" + creationDate + '\'' +
                '}';
    }

    @JsonPOJOBuilder(buildMethodName = "build")
    public static class Builder {
        @JsonProperty("personId")
        private int personId;
        @JsonProperty("postId")
        private int postId;
        @JsonProperty("creationDate")
        private String creationDate;

        public Builder personId(final int personId) {
            this.personId = personId;
            return this;
        }

        public Builder postId(final int postId) {
            this.postId = postId;
            return this;
        }

        public Builder creationDate(final String creationDate) {
            this.creationDate = creationDate;
            return this;
        }

        public LikesEventStream build() {
            return new LikesEventStream(this);
        }

    }
}


