package com.dspa.project.recommendation;


import com.dspa.project.model.PostAndDate;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;


public interface PostAndDateRepository extends CrudRepository<PostAndDate, Integer> {
}
