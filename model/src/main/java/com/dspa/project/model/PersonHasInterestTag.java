package com.dspa.project.model;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "person_hasInterest_tag")
//@IdClass(PersonHasInterestTagKey.class)
public class PersonHasInterestTag {

    @EmbeddedId
    private PersonHasInterestTagKey id;

    public PersonHasInterestTagKey getId() {
        return id;
    }
}
