package com.dspa.project.streamproducer;


import com.dspa.project.model.Stream;
import com.dspa.project.streamproducer.kafka.ProduceCommentStream;
import com.dspa.project.streamproducer.kafka.ProduceLikesStream;
import com.dspa.project.streamproducer.kafka.ProducePostStream;
import javafx.util.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import util.PairComparator;

import java.util.concurrent.PriorityBlockingQueue;

@SpringBootApplication
public class StreamproducerApplication {



    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(StreamproducerApplication.class, args);

        StreamProducer producer = context.getBean(StreamProducer.class);

        PriorityBlockingQueue<Pair<Long,Stream>> queue = new PriorityBlockingQueue<>(50, new PairComparator());

        Runnable runComment = new ProduceCommentStream(queue);
        Runnable runLikes = new ProduceLikesStream(queue);
        Runnable runPost = new ProducePostStream(queue);

        Thread commentThread = new Thread(runComment);
        Thread likesThread = new Thread(runLikes);
        Thread postThread = new Thread(runPost);

        Runnable consumer = new QueueConsumer(queue,producer);
        Thread consThread = new Thread(consumer);


        commentThread.start();
        likesThread.start();
        postThread.start();

        consThread.start();

    }



    @Bean
    public StreamProducer messageProducer() {
        return new StreamProducer();
    }


    public static class StreamProducer {

        @Autowired
        private KafkaTemplate<String, String> kafkaTemplate;

        public void sendMessage(String message, String topicName) {

            ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
            future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
                @Override
                public void onSuccess(SendResult<String, String> result) {
                    //System.out.println("Sent message to "+ topicName +"=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
                }

                @Override
                public void onFailure(Throwable ex) {
                    System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
                }
            });
        }
    }
}
