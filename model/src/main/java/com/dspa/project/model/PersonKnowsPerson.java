package com.dspa.project.model;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "person_knows_person")
public class PersonKnowsPerson {
    @EmbeddedId
    PersonKnowsPersonKey id;

    public PersonKnowsPersonKey getId() {
        return id;
    }
}
