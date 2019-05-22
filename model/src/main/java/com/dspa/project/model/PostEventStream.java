package com.dspa.project.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

@JsonDeserialize(builder = PostEventStream.Builder.class)
public class PostEventStream {
    private final int id;
    private final int personId;
    private final String creationDate;
    private final String imageFile;
    private final String locationIP;
    private final String browserUsed;
    private final String language;
    private final String content;
    private final String tags;
    private final int forumId;
    private final int placeId;

    public PostEventStream(final Builder builder) {
        this.id = builder.id;
        this.personId = builder.personId;
        this.creationDate = builder.creationDate;
        this.imageFile = builder.imageFile;
        this.locationIP = builder.locationIP;
        this.browserUsed = builder.browserUsed;
        this.language = builder.language;
        this.content = builder.content;
        this.tags = builder.tags;
        this.forumId = builder.forumId;
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

    public String getImageFile() {
        return imageFile;
    }

    public String getLocationIP() {
        return locationIP;
    }

    public String getBrowserUsed() {
        return browserUsed;
    }

    public String getLanguage() {
        return language;
    }

    public String getContent() {
        return content;
    }

    public String getTags() {
        return tags;
    }

    public int getForumId() {
        return forumId;
    }

    public int getPlaceId() {
        return placeId;
    }

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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostEventStream that = (PostEventStream) o;
        return getId() == that.getId() &&
                getPersonId() == that.getPersonId() &&
                getForumId() == that.getForumId() &&
                getPlaceId() == that.getPlaceId() &&
                Objects.equals(getCreationDate(), that.getCreationDate()) &&
                Objects.equals(getImageFile(), that.getImageFile()) &&
                Objects.equals(getLocationIP(), that.getLocationIP()) &&
                Objects.equals(getBrowserUsed(), that.getBrowserUsed()) &&
                Objects.equals(getLanguage(), that.getLanguage()) &&
                Objects.equals(getContent(), that.getContent()) &&
                Objects.equals(getTags(), that.getTags());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getPersonId(), getCreationDate(), getImageFile(), getLocationIP(), getBrowserUsed(), getLanguage(), getContent(), getTags(), getForumId(), getPlaceId());
    }

    @Override
    public String toString() {
        return "PostEventStream{" +
                "id=" + id +
                ", personId=" + personId +
                ", creationDate='" + creationDate + '\'' +
                ", imageFile='" + imageFile + '\'' +
                ", locationIP='" + locationIP + '\'' +
                ", browserUsed='" + browserUsed + '\'' +
                ", language='" + language + '\'' +
                ", content='" + content + '\'' +
                ", tags='" + tags + '\'' +
                ", forumId=" + forumId +
                ", placeId=" + placeId +
                '}';
    }

    @JsonPOJOBuilder(buildMethodName = "build")
    public static class Builder {
        @JsonProperty("id")
        private int id;
        @JsonProperty("personId")
        private int personId;
        @JsonProperty("creationDate")
        private String creationDate;
        @JsonProperty("imageFile")
        private String imageFile;
        @JsonProperty("locationIP")
        private String locationIP;
        @JsonProperty("browserUsed")
        private String browserUsed;
        @JsonProperty("language")
        private String language;
        @JsonProperty("content")
        private String content;
        @JsonProperty("tags")
        private String tags;
        @JsonProperty("forumId")
        private int forumId;
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

        public Builder imageFile(final String imageFile) {
            this.imageFile = imageFile;
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

        public Builder language(final String language) {
            this.language = language;
            return this;
        }

        public Builder content(final String content) {
            this.content = content;
            return this;
        }

        public Builder tags(final String tags) {
            this.tags = tags;
            return this;
        }

        public Builder forumId(final int forumId) {
            this.forumId = forumId;
            return this;
        }

        public Builder placeId(final int placeId) {
            this.placeId = placeId;
            return this;
        }

        public PostEventStream build() {
            return new PostEventStream(this);
        }
    }
}
