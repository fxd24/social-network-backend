package com.dspa.project.streamproducer.kafka;

import com.dspa.project.streamproducer.StreamproducerApplication;
import com.dspa.project.streamproducer.util.CSVReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static com.dspa.project.streamproducer.util.Util.handleFileNotFoundException;

public class ProduceLikesStream implements Runnable{

    StreamproducerApplication.StreamProducer prod;

    public ProduceLikesStream(StreamproducerApplication.StreamProducer producer) {
        prod = producer;
    }

    public static void readLikesEventStreamCsvAndSendToTopic(StreamproducerApplication.StreamProducer producer) {
        CSVReader reader = new CSVReader("[|]");
        final String FILE_PATH = "../../1k-users-sorted/streams/likes_event_stream.csv";

        final File csvFile = new File(FILE_PATH);
        handleFileNotFoundException(csvFile);
        try {
            reader.readLikesEventStreamCSV(new BufferedReader(new FileReader(csvFile)), producer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        readLikesEventStreamCsvAndSendToTopic(prod);
    }
}
