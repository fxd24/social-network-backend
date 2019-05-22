package com.dspa.project.model;

// id(Long) | name(String) | url(String)

import java.util.Objects;

public class Tag {

    private final long id;
    private final String name;
    private final String url;

    private Tag(final Builder builder) {
        this.id = builder.id;
        this.name = builder.name;
        this.url = builder.url;

    }

    // Getter

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getUrl() {
        return url;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tag tagTable = (Tag) o;
        return id == tagTable.id &&
                name.equals(tagTable.name) &&
                url.equals(tagTable.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, url);
    }

    @Override
    public String toString() {
        return "Tag{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", url='" + url + '\'' +
                '}';
    }

    // Builder

    public static class Builder {

        private long id;
        private String url;
        private String name;

        public Builder id(final long id) {
            this.id = id;
            return this;
        }

        public Builder url(final String url) {
            this.url = url;
            return this;
        }

        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        public Tag build() {
            return new Tag(this);
        }
    }

}

