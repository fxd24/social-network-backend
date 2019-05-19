package com.dspa.project.model;

import java.util.Objects;

public class Forum {

    private final long id;
    private final String creationDate;
    private final String title;


    private Forum(final Builder builder){
        this.id = builder.id;
        this.creationDate = builder.creationDate;
        this.title = builder.title;
    }

    // Getters

    public String getCreationDate() {
        return creationDate;
    }

    public String getTitle() {
        return title;
    }

    public long getId() {
        return id;
    }

    //  OTHER STANDARD METHODS


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Forum that = (Forum) o;
        return id == that.id &&
                creationDate.equals(that.creationDate) &&
                title.equals(that.title);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, creationDate, title);
    }

    @Override
    public String toString() {
        return "Forum{" +
                "id=" + id +
                ", creationDate='" + creationDate + '\'' +
                ", title='" + title + '\'' +
                '}';
    }


    //  BUILDER

    public static class Builder{
        private long id;
        private String creationDate;
        private String title;


        public Builder id(final long id){
            this.id=id;
            return this;
        }
        public Builder creationDate(final String creationDate){
            this.creationDate=creationDate;
            return this;
        }
        public Builder title(final String title){
            this.title=title;
            return this;
        }

        public Forum build(){
            return new Forum(this);
        }
    }


}

