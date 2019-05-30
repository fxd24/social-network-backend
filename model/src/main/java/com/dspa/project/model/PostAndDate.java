package com.dspa.project.model;

import javax.persistence.*;
import java.util.Date;

@Entity
@Table(name = "PostAndDate")
public class PostAndDate {
    @Id
    @Column(name = "id")
    private Integer id;

    @Column(name = "lastUpdate")
    @Temporal(TemporalType.TIMESTAMP)
    private Date lastUpdate;

//    public PostAndDate(Integer id, Date lastUpdate) {
//        this.id = id;
//        this.lastUpdate = lastUpdate;
//    }

    public Integer getId() {
        return id;
    }

    public Date getLastUpdate() {
        return lastUpdate;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setLastUpdate(Date lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    @Override
    public String toString() {
        return "PostAndDate{" +
                "id=" + id +
                ", lastUpdate=" + lastUpdate +
                '}';
    }
}
