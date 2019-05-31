package com.dspa.project.recommendation.repository;

import com.dspa.project.model.PersonKnowsPerson;
import com.dspa.project.model.PersonKnowsPersonKey;
import org.springframework.data.repository.CrudRepository;

public interface PersonKnowsPersonRepository extends CrudRepository<PersonKnowsPerson, PersonKnowsPersonKey> {
}
