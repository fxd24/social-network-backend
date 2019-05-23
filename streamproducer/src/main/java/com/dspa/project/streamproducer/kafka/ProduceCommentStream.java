package com.dspa.project.streamproducer.kafka;

import com.dspa.project.model.CommentEventStream;
import com.dspa.project.model.Stream;
import com.dspa.project.streamproducer.StreamproducerApplication;
import com.dspa.project.streamproducer.util.CSVReader;
import javafx.util.Pair;

import java.io.IOException;

import java.util.concurrent.PriorityBlockingQueue;


public class ProduceCommentStream implements Runnable {

    PriorityBlockingQueue<Pair<Long,Stream>> queue;
    public ProduceCommentStream(PriorityBlockingQueue<Pair<Long,Stream>> queue) {
        this.queue = queue;
    }

    //TODO: clean this duplicated mess
    @Override
    public void run() {
        CSVReader reader = new CSVReader("[|]");
        final String FILE_PATH = "../../1k-users-sorted/streams/comment_event_stream.csv";

        try {
            reader.readCommentEventStreamCSV(FILE_PATH, queue);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
