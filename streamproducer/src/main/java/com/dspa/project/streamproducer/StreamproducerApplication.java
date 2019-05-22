package com.dspa.project.streamproducer;

import com.dspa.project.model.CommentEventStream;
import com.dspa.project.streamproducer.kafka.ProduceCommentStream;
import com.dspa.project.streamproducer.kafka.ProduceLikesStream;
import com.dspa.project.streamproducer.kafka.ProducePostStream;
import com.dspa.project.streamproducer.util.CSVReader;
import com.dspa.project.streamproducer.util.StreamWaitSimulation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import static com.dspa.project.streamproducer.util.Util.handleFileNotFoundException;


@SpringBootApplication
public class StreamproducerApplication {


    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(StreamproducerApplication.class, args);

        StreamProducer producer = context.getBean(StreamProducer.class);

        Runnable runComment = new ProduceCommentStream(producer);
        Runnable runLikes = new ProduceLikesStream(producer);
        Runnable runPost = new ProducePostStream(producer);

        Thread commentThread = new Thread(runComment);
        Thread likesThread = new Thread(runLikes);
        Thread postThread = new Thread(runPost);

//        commentThread.start();
//        likesThread.start();
//        postThread.start();

        //comment first: 2012-02-02T02:45:14Z
        //likes first: 2012-02-02T01:09:00.000Z
        //post first: 2012-02-02T02:46:56Z


//        CSVReader csvReader = new CSVReader("[|]");
//        try {
//            HashMap<Long, CommentEventStream> test = csvReader.readCommentEventStreamCSVtoMap(producer);
//        }catch (IOException e){
//            e.printStackTrace();
//        }




    }

    @Bean
    public StreamProducer messageProducer() {
        return new StreamProducer();
    }


    public static class StreamProducer {

        @Autowired
        private KafkaTemplate<String, String> kafkaTemplate;

        public void sendMessage(String message, String topicName) {
            (new StreamWaitSimulation()).maySleepRandomAmountOfTime();
            ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
            future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
                @Override
                public void onSuccess(SendResult<String, String> result) {
                    System.out.println("Sent message to "+ topicName +"=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
                }

                @Override
                public void onFailure(Throwable ex) {
                    System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
                }
            });
        }
    }
}
