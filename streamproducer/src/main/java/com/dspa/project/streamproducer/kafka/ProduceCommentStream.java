package com.dspa.project.streamproducer.kafka;

import com.dspa.project.streamproducer.StreamproducerApplication;
import com.dspa.project.streamproducer.util.CSVReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static com.dspa.project.streamproducer.util.Util.handleFileNotFoundException;

public class ProduceCommentStream implements Runnable {
    StreamproducerApplication.StreamProducer prod;

    public ProduceCommentStream(StreamproducerApplication.StreamProducer producer) {
        prod = producer;
    }

    //TODO: clean this duplicated mess
    @Override
    public void run() {
        CSVReader reader = new CSVReader("[|]");
        final String FILE_PATH = "../../1k-users-sorted/streams/comment_event_stream.csv";

        final File csvFile = new File(FILE_PATH);
        handleFileNotFoundException(csvFile);
        try {
            reader.readCommentEventStreamCSV(new BufferedReader(new FileReader(csvFile)), prod);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
