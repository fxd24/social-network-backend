package com.dspa.project.activepoststatistics.repo;

import com.dspa.project.model.PostAndDate;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import java.util.Date;

@Component
@Repository
public interface PostAndDateRepository extends CrudRepository<PostAndDate, Integer> {

//    @Query("update PostAndDate c set c.lastUpdate = :lastUpdate WHERE c.id = :id")
//    public void setLastUpdate(@Param("id") Integer id, @Param("lastUpdate") Date lastUpdate);

}
