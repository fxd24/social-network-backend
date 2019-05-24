package com.dspa.project.model;

import javax.persistence.*;
import java.util.Date;

@Entity
@Table(name = "PostAndDate")
public class PostAndDate {
    @Id
    private Long id;

    @Column(name = "lastUpdate")
    @Temporal(TemporalType.TIMESTAMP)
    private Date lastUpdate;

    public Long getId() {
        return id;
    }

    public Date getLastUpdate() {
        return lastUpdate;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setLastUpdate(Date lastUpdate) {
        this.lastUpdate = lastUpdate;
    }
}
