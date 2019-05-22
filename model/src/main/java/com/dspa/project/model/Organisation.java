package com.dspa.project.model;

import java.util.Objects;

public class Organisation {

    private final long id;
    private final String type;   // type({"university", "company"}) - need to change
    private final String name;
    private final String url; // is there a better way to store it?


    private Organisation(final Builder builder) {

        this.id = builder.id;
        this.type = builder.type;
        this.name = builder.name;
        this.url = builder.url;

    }

    // Getters and other standard methods

    public long getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getUrl() {
        return url;
    }

    //

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Organisation that = (Organisation) o;
        return id == that.id &&
                type.equals(that.type) &&
                name.equals(that.name) &&
                url.equals(that.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, name, url);
    }

    @Override
    public String toString() {
        return "Organisation{" +
                "id=" + id +
                ", type='" + type + '\'' +
                ", name='" + name + '\'' +
                ", url='" + url + '\'' +
                '}';
    }

    // Builder

    public static class Builder {
        private long id;
        private String type;
        private String name;
        private String url;


        public Builder id(final long id) {
            this.id = id;
            return this;
        }

        public Builder type(final String type) {
            this.type = type;
            return this;
        }

        public Builder creationDate(final String name) {
            this.name = name;
            return this;
        }

        public Builder url(final String url) {
            this.url = url;
            return this;
        }

        public Organisation build() {
            return new Organisation(this);
        }
    }

}
