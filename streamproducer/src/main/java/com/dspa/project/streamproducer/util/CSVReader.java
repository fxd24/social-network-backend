package com.dspa.project.streamproducer.util;


import com.dspa.project.model.CommentEventStream;
import com.dspa.project.model.LikesEventStream;
import com.dspa.project.model.PostEventStream;
import com.dspa.project.streamproducer.StreamproducerApplication;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.regex.Pattern;

import static com.dspa.project.streamproducer.util.Util.handleFileNotFoundException;


public class CSVReader {

    //TODO: fix problem that does not recognize values from app properties
    //@Value(value = "${comment.topic.name}")
    String commentTopicName = "baeldung";
    //@Value(value = "${likes.topic.name}")
    String likesTopicName = "likes";
    //@Value(value = "${post.topic.name}")
    String postTopicName = "post";

    private final Pattern pattern;

    public CSVReader(final String separator) {
        this.pattern = Pattern.compile(separator);
    }

    public HashMap<Long,CommentEventStream> readCommentEventStreamCSVtoMap(PriorityQueue queue) throws IOException {
        final String FILE_PATH = "../../1k-users-sorted/streams/comment_event_stream.csv";
        final File csvFile = new File(FILE_PATH);
        handleFileNotFoundException(csvFile);
        FileReader fileReader = new FileReader(csvFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        HashMap<Long,CommentEventStream> map = new HashMap<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        try {
            String line;
            line = bufferedReader.readLine(); //read the first line. we do nothing with it.
            while ((line = bufferedReader.readLine()) != null) {
                final String[] lineArray = pattern.split(line);

                CommentEventStream value = new CommentEventStream
                        .Builder()
                        .id(Integer.parseInt(lineArray[0]))
                        .personId(Integer.parseInt(lineArray[1]))
                        .creationDate(lineArray[2])
                        .locationIP(lineArray[3])
                        .browserUsed(lineArray[4])
                        .content(lineArray[5])
                        .reply_to_postId(Integer.parseInt(lineArray[6].equals("")  ?  "-1":lineArray[6])) //TODO: handle this empty string problem in a cleaner way.
                        .reply_to_commentId(Integer.parseInt(lineArray[7].equals("") ? "-1":lineArray[7] ))//TODO
                        .placeId(Integer.parseInt(lineArray[8].equals("") ? "-1": lineArray[8]))//TODO
                        .build();
                ;
                map.put(sdf.parse(value.getCreationDate()).getTime(),value);
            }

        } finally {
            bufferedReader.close();
            return map;
        }
    }


    //Read the CSV file line by line, serialize into object and put to sleep fo
    public void readCommentEventStreamCSV(
            final BufferedReader bufferedReader, StreamproducerApplication.StreamProducer producer) throws IOException {
        String last_timestamp = "2012-02-02T01:09:00Z";
        StreamWaitSimulation sleep = new StreamWaitSimulation();
        try {
            String line;
            line = bufferedReader.readLine(); //read the first line. we do nothing with it.
            while ((line = bufferedReader.readLine()) != null) {
                final String[] lineArray = pattern.split(line);

                CommentEventStream value = new CommentEventStream
                        .Builder()
                        .id(Integer.parseInt(lineArray[0]))
                        .personId(Integer.parseInt(lineArray[1]))
                        .creationDate(lineArray[2])
                        .locationIP(lineArray[3])
                        .browserUsed(lineArray[4])
                        .content(lineArray[5])
                        .reply_to_postId(Integer.parseInt(lineArray[6].equals("")  ?  "-1":lineArray[6])) //TODO: handle this empty string problem in a cleaner way.
                        .reply_to_commentId(Integer.parseInt(lineArray[7].equals("") ? "-1":lineArray[7] ))//TODO
                        .placeId(Integer.parseInt(lineArray[8].equals("") ? "-1": lineArray[8]))//TODO
                        .build();
                sleep.wait(last_timestamp, lineArray[2]);
                last_timestamp = lineArray[2];

                //This sends the object to a topic in Kafka
                send(value, producer, commentTopicName);
            }

        } finally {
            bufferedReader.close();
        }
    }

    //Read the CSV file line by line, serialize into object and put to sleep fo
    public void readLikesEventStreamCSV(
            final BufferedReader bufferedReader, StreamproducerApplication.StreamProducer producer) throws IOException {
        String last_timestamp = "2012-02-02T01:09:00.000Z";
        StreamWaitSimulation sleep = new StreamWaitSimulation();
        try {
            String line;
            line = bufferedReader.readLine(); //read the first line. we do nothing with it.
            while ((line = bufferedReader.readLine()) != null) {
                final String[] lineArray = pattern.split(line);

                LikesEventStream value = new LikesEventStream
                        .Builder()
                        .personId(Integer.parseInt(lineArray[0]))
                        .postId(Integer.parseInt(lineArray[1].equals("") ? "-1":lineArray[1] )) //TODO: handle this empty string problem in a cleaner way.
                        .creationDate(lineArray[2])
                        .build();
                //Here the code will wait before sending the LikesEventStream value created above
                sleep.wait(last_timestamp, lineArray[2]);
                last_timestamp = lineArray[2];

                //This sends the object to a topic in Kafka
                send(value, producer, likesTopicName);
            }

        } finally {
            bufferedReader.close();
        }
    }

    public void readPostEventStreamCSV(
            final BufferedReader bufferedReader, StreamproducerApplication.StreamProducer producer) throws IOException {
        try {
            String line;
            line = bufferedReader.readLine(); //read the first line. we do nothing with it.
            String last_timestamp = "2012-02-02T01:09:00Z";
            StreamWaitSimulation sleep = new StreamWaitSimulation();
            while ((line = bufferedReader.readLine()) != null) {
                final String[] lineArray = pattern.split(line);

                PostEventStream value = new PostEventStream
                        .Builder()
                        .id(Integer.parseInt(lineArray[0]))
                        .personId(Integer.parseInt(lineArray[1]))
                        .creationDate(lineArray[2])
                        .imageFile(lineArray[3])
                        .locationIP(lineArray[4])
                        .browserUsed(lineArray[5])
                        .language(lineArray[6])
                        .content(lineArray[7])
                        .tags(lineArray[8])
                        .forumId(Integer.parseInt(lineArray[9])) //TODO
                        .placeId(Integer.parseInt(lineArray[10].equals("") ? "-1":lineArray[10].replaceAll("\\D+","")))//TODO
                        .build();
                sleep.wait(last_timestamp, lineArray[2]);
                last_timestamp = lineArray[2];
                send(value, producer, postTopicName);
            }

        } finally {
            bufferedReader.close();
        }
    }

    //TODO: look at the type Object to make the function cleaner
    private void send(Object value, StreamproducerApplication.StreamProducer producer, String topicName) {
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

