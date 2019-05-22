package com.dspa.project.model;


import javax.persistence.*;
import java.util.Date;
import java.util.Objects;

@Entity
@Table(name = "forum")
public class Forum {

    @Id
    private Long id;

    @Column(name = "creationDate")
    @Temporal(TemporalType.TIMESTAMP)
    private Date creationDate;

    @Column(name = "title")
    private String title;


    private Forum() {
    }

    // Getters

    public Date getCreationDate() {
        return creationDate;
    }

    public String getTitle() {
        return title;
    }

    public Long getId() {
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




}

