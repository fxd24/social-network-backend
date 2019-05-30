package com.dspa.project.activepoststatistics.repo;

import com.dspa.project.model.PostAndDate;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;

@Component
@Repository
public interface PostAndDateRepository extends JpaRepository<PostAndDate, Integer> {
}
