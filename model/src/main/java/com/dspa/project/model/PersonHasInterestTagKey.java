package com.dspa.project.model;

import java.io.Serializable;

public class PersonHasInterestTagKey implements Serializable{
        public int personId;
        public int tagId;

    public PersonHasInterestTagKey() {
    }

    public PersonHasInterestTagKey(int personId, int tagId) {
        this.personId = personId;
        this.tagId = tagId;
    }
}

