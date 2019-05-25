package com.dspa.project.streamproducer.kafka;

import com.dspa.project.model.Stream;
import com.dspa.project.streamproducer.util.CSVReader;
import javafx.util.Pair;

import java.io.IOException;
import java.util.concurrent.PriorityBlockingQueue;

public class ProducePostStream implements Runnable{

    PriorityBlockingQueue<Pair<Long,Stream>> queue;
    public ProducePostStream(PriorityBlockingQueue<Pair<Long,Stream>> queue) {
        this.queue = queue;
    }


    public static void readPostEventStreamCsvAndSendToTopic(PriorityBlockingQueue<Pair<Long,Stream>> queue) {
        CSVReader reader = new CSVReader("[|]");
        final String FILE_PATH = "../../1k-users-sorted/streams/post_event_stream.csv";

        try {
            reader.readPostEventStreamCSV(FILE_PATH, queue);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        readPostEventStreamCsvAndSendToTopic(queue);
    }
}
