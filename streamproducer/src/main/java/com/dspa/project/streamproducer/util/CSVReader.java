package com.dspa.project.streamproducer.util;


import com.dspa.project.model.CommentEventStream;
import com.dspa.project.model.LikesEventStream;
import com.dspa.project.model.PostEventStream;
import com.dspa.project.streamproducer.StreamproducerApplication;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;


import java.io.BufferedReader;
import java.io.IOException;
import java.util.regex.Pattern;


public class CSVReader {

    @Value(value = "${comment.topic.name}")
    String commentTopicName;
    @Value(value = "${likes.topic.name}")
    String likesTopicName;
    @Value(value = "${post.topic.name}")
    String postTopicName;

    private final Pattern pattern;

    public CSVReader(final String separator) {
        this.pattern = Pattern.compile(separator);
    }

    public void readCommentEventStreamCSV(
            final BufferedReader bufferedReader, StreamproducerApplication.StreamProducer producer) throws IOException {
        String last_timestamp = "";
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
                        .reply_to_postId(Integer.parseInt(lineArray[6] == "" ? lineArray[6] : "-1")) //TODO: handle this empty string problem in a cleaner way.
                        .reply_to_commentId(Integer.parseInt(lineArray[7] == "" ? lineArray[7] : "-1"))//TODO
                        .placeId(Integer.parseInt(lineArray[8] == "" ? lineArray[8] : "-1"))//TODO
                        .build();
                sleep.wait(last_timestamp, lineArray[2]);
                last_timestamp = lineArray[2];
                send(value, producer,commentTopicName);
            }

        } finally {
            bufferedReader.close();
        }
    }

    public void readLikesEventStreamCSV(
            final BufferedReader bufferedReader, StreamproducerApplication.StreamProducer producer) throws IOException {
        String last_timestamp = "";
        StreamWaitSimulation sleep = new StreamWaitSimulation();
        try {
            String line;
            line = bufferedReader.readLine(); //read the first line. we do nothing with it.
            while ((line = bufferedReader.readLine()) != null) {
                final String[] lineArray = pattern.split(line);

                LikesEventStream value = new LikesEventStream
                        .Builder()
                        .personId(Integer.parseInt(lineArray[0]))
                        .postId(Integer.parseInt(lineArray[1] == "" ? lineArray[1] : "-1")) //TODO: handle this empty string problem in a cleaner way.
                        .creationDate(lineArray[2])
                        .build();
                sleep.wait(last_timestamp, lineArray[2]);
                last_timestamp = lineArray[2];
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
            String last_timestamp = "";
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
                        .placeId(Integer.parseInt(lineArray[10] == "" ? lineArray[10] : "-1"))//TODO
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
    private void send(Object value, StreamproducerApplication.StreamProducer producer, String topicName){
        final ObjectMapper mapper = new ObjectMapper();
        try {
            String msg = mapper.writeValueAsString(value);
            //System.out.println(msg);
            producer.sendMessage(msg,topicName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

