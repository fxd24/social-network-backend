package com.dspa.project.recommendation;

import com.dspa.project.recommendation.repository.ForumRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;



@SpringBootApplication
@EntityScan(basePackages = {"com.dspa.project.model"})
public class RecommendationApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(RecommendationApplication.class);

    @Autowired
    ForumRepository repo;

//    @Autowired
//    PostAndDateRepository repo2;

    public static void main(String[] args) {
        SpringApplication.run(RecommendationApplication.class, args);
    }

    @Override
    public void run(String... args) {

        log.info("StartApplication...");

        System.out.println("\nfindAll()");
        //repo.findAll().forEach(x -> System.out.println(x));

        //repo2.findAll().forEach(x -> System.out.println(x));

    }

}
