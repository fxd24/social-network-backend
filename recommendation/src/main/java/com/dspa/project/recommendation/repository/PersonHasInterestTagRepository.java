package com.dspa.project.recommendation.repository;

import com.dspa.project.model.PersonHasInterestTag;
import com.dspa.project.model.PersonHasInterestTagKey;
import org.springframework.data.repository.CrudRepository;

public interface PersonHasInterestTagRepository extends CrudRepository<PersonHasInterestTag, PersonHasInterestTagKey> {
}
