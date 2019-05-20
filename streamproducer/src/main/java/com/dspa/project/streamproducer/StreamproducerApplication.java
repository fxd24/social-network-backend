package com.dspa.project.streamproducer;

import com.dspa.project.streamproducer.util.CSVReader;
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


import java.io.*;

@SpringBootApplication
public class StreamproducerApplication {


    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(StreamproducerApplication.class, args);

        StreamProducer producer = context.getBean(StreamProducer.class);

        readCommentEventStreamCsvAndSendToTopic(producer);
        //readLikesEventStreamCsvAndSendToTopic(producer);
        //readPostEventStreamCsvAndSendToTopic(producer);

//        StreamWaitSimulation test = new StreamWaitSimulation();
//        test.randomSleep();


    }

    //TODO: clean this duplicated mess
    public static void readCommentEventStreamCsvAndSendToTopic(StreamProducer producer) {
        CSVReader reader = new CSVReader("[|]");
        final String FILE_PATH="../../1k-users-sorted/streams/comment_event_stream.csv";

        final File csvFile = new File(FILE_PATH);
        if (!csvFile.exists()) {
            try {
                throw new FileNotFoundException("File not found");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        try {
            reader.readCommentEventStreamCSV(new BufferedReader(new FileReader(csvFile)), producer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readLikesEventStreamCsvAndSendToTopic(StreamProducer producer) {
        CSVReader reader = new CSVReader("[|]");
        final String FILE_PATH="../../1k-users-sorted/streams/likes_event_stream.csv";

        final File csvFile = new File(FILE_PATH);
        if (!csvFile.exists()) {
            try {
                throw new FileNotFoundException("File not found");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        try {
            reader.readLikesEventStreamCSV(new BufferedReader(new FileReader(csvFile)), producer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readPostEventStreamCsvAndSendToTopic(StreamProducer producer) {
        CSVReader reader = new CSVReader("[|]");
        final String FILE_PATH="../../1k-users-sorted/streams/post_event_stream.csv";

        final File csvFile = new File(FILE_PATH);
        if (!csvFile.exists()) {
            try {
                throw new FileNotFoundException("File not found");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        try {
            reader.readPostEventStreamCSV(new BufferedReader(new FileReader(csvFile)), producer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Bean
    public StreamProducer messageProducer() {
        return new StreamProducer();
    }


    public static class StreamProducer {

        @Autowired
        private KafkaTemplate<String, String> kafkaTemplate;

        @Value(value = "${comment.topic.name}")
        private String commentTopicName;

        public void sendMessage(String message, String topicName) {
            ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
            future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
                @Override
                public void onSuccess(SendResult<String, String> result) {
                    System.out.println("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
                }
                @Override
                public void onFailure(Throwable ex) {
                    System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
                }
            });
        }

    }

}
