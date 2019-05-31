package com.dspa.project.activepoststatistics.repo;

import com.dspa.project.model.PostAndDate;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

@Component
@Repository
public interface PostAndDateRepository extends JpaRepository<PostAndDate, Integer> {
}
