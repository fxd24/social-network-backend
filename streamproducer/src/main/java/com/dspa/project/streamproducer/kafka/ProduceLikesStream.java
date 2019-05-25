package com.dspa.project.streamproducer.kafka;

import com.dspa.project.model.Stream;
import com.dspa.project.streamproducer.util.CSVReader;
import javafx.util.Pair;

import java.io.IOException;
import java.util.concurrent.PriorityBlockingQueue;

public class ProduceLikesStream implements Runnable{

    PriorityBlockingQueue<Pair<Long,Stream>> queue;
    public ProduceLikesStream(PriorityBlockingQueue<Pair<Long,Stream>> queue) {
        this.queue = queue;
    }

    public static void readLikesEventStreamCsvAndSendToTopic(PriorityBlockingQueue<Pair<Long,Stream>> queue) {
        CSVReader reader = new CSVReader("[|]");
        final String FILE_PATH = "../../1k-users-sorted/streams/likes_event_stream.csv";

        try {
            reader.readLikesEventStreamCSV(FILE_PATH, queue);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        readLikesEventStreamCsvAndSendToTopic(queue);
    }
}
