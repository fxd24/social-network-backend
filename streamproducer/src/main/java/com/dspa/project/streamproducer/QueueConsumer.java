package com.dspa.project.streamproducer;

import com.dspa.project.model.CommentEventStream;
import com.dspa.project.model.LikesEventStream;
import com.dspa.project.model.PostEventStream;
import com.dspa.project.model.Stream;
import com.dspa.project.streamproducer.util.StreamWaitSimulation;
import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.util.Pair;

import java.io.IOException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueueConsumer implements Runnable{
    PriorityBlockingQueue<Pair<Long,Stream>> queue;
    StreamproducerApplication.StreamProducer producer;
    StreamWaitSimulation w = new StreamWaitSimulation();
    Long speedup_factor = 1000L;


    QueueConsumer(PriorityBlockingQueue<Pair<Long,Stream>> queue,StreamproducerApplication.StreamProducer producer){
        this.queue = queue;
        this.producer = producer;

    }
    @Override
    //TODO: cleanup
    public void run() {
        Long last_timestamp = 1328141589837L;
        // System.out.println("CONSUMING: "+queue.poll().getValue().toString());
        //if(likesQueue.size()!=0) System.out.println("CONSUMING LIKES: "+likesQueue.poll().getCreationDate());
        //if(postQueue.size()!=0) System.out.println("CONSUMING POST: "+postQueue.poll().getCreationDate());
        while(true) if (queue.size() != 0) {
            Pair<Long, Stream> obj = queue.poll();
            Long new_timestamp = obj.getKey();
            //System.out.println(new_timestamp);


            Long diff = new_timestamp - last_timestamp;
            Long waiting_time = diff / speedup_factor;
            //TODO: beautify
            if(waiting_time>300000L){
                waiting_time = 300000L/speedup_factor;
            }
            try {
                //System.out.println("Waiting for " + waiting_time + " milliseconds"); //TODO: remove when tested
                TimeUnit.MILLISECONDS.sleep(waiting_time);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (obj.getValue() instanceof CommentEventStream) {
                send(obj.getValue(), producer, "comment");
                last_timestamp = new_timestamp;
            } else if (obj.getValue() instanceof LikesEventStream) {
                send(obj.getValue(), producer, "likes");
                last_timestamp = new_timestamp;
            } else {
                send(obj.getValue(), producer, "post");
                last_timestamp = new_timestamp;
            }
        }

    }

    private void send(Stream value, StreamproducerApplication.StreamProducer producer, String topicName) {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            String msg = mapper.writeValueAsString(value);
            //System.out.println(msg);
            producer.sendMessage(msg, topicName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
