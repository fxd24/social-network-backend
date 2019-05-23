package com.dspa.project.streamproducer.util;


import com.dspa.project.model.CommentEventStream;
import com.dspa.project.model.LikesEventStream;
import com.dspa.project.model.PostEventStream;
import com.dspa.project.model.Stream;
import com.dspa.project.streamproducer.StreamproducerApplication;
import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.util.Pair;
import org.springframework.beans.factory.annotation.Value;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.regex.Pattern;

import static com.dspa.project.streamproducer.util.Util.handleFileNotFoundException;


public class CSVReader {

    //TODO: fix problem that does not recognize values from app properties
    //@Value(value = "${comment.topic.name}")
    String commentTopicName = "comment";
    //@Value(value = "${likes.topic.name}")
    String likesTopicName = "likes";
    //@Value(value = "${post.topic.name}")
    String postTopicName = "post";

    private final Pattern pattern;
    private StreamWaitSimulation delay = new StreamWaitSimulation();

    public CSVReader(final String separator) {
        this.pattern = Pattern.compile(separator);
    }


    //Read the CSV file line by line, serialize into object and put to sleep fo
    public void readCommentEventStreamCSV(String FILE_PATH, PriorityBlockingQueue<Pair<Long,Stream>> queue) throws IOException {

        final File csvFile = new File(FILE_PATH);
        handleFileNotFoundException(csvFile);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(csvFile));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        Long timestamp;
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

                //This sends the object to a topic in Kafka
                //send(value, producer, commentTopicName);
                timestamp = sdf.parse(lineArray[2]).getTime() + delay.maybeRandomDelayNumber();

                queue.add(new Pair<Long,Stream>(timestamp,value));
            }

        } catch (ParseException e) {
            e.printStackTrace();
        } finally {
            bufferedReader.close();
        }
    }

    //Read the CSV file line by line, serialize into object and put to sleep fo
    public void readLikesEventStreamCSV(String FILE_PATH, PriorityBlockingQueue<Pair<Long,Stream>> queue) throws IOException {
        final File csvFile = new File(FILE_PATH);
        handleFileNotFoundException(csvFile);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(csvFile));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        Long timestamp;
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
//                sleep.wait(last_timestamp, lineArray[2]);
//                last_timestamp = lineArray[2];
//
//                //This sends the object to a topic in Kafka
//                send(value, producer, likesTopicName);
                timestamp = sdf.parse(lineArray[2]).getTime() + delay.maybeRandomDelayNumber();
                queue.add(new Pair<Long,Stream>(timestamp,value));
            }

        } catch (ParseException e) {
            e.printStackTrace();
        } finally {
            bufferedReader.close();
        }
    }

    public void readPostEventStreamCSV(String FILE_PATH, PriorityBlockingQueue<Pair<Long,Stream>> queue) throws IOException {
        final File csvFile = new File(FILE_PATH);
        handleFileNotFoundException(csvFile);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(csvFile));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        Long timestamp;
        try {
            String line;
            line = bufferedReader.readLine(); //read the first line. we do nothing with it.


            while ((line = bufferedReader.readLine()) != null) {
                final String[] lineArray = pattern.split(line);

                PostEventStream value = new PostEventStream
                        .Builder()
                        .id(Integer.parseInt(lineArray[0].replaceAll("\\D+","")))
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
//                sleep.wait(last_timestamp, lineArray[2]);
//                last_timestamp = lineArray[2];
//                send(value, producer, postTopicName);
                timestamp = sdf.parse(lineArray[2]).getTime() + delay.maybeRandomDelayNumber();
                queue.add(new Pair<Long,Stream>(timestamp,value));
            }

        } catch (ParseException e) {
            e.printStackTrace();
        } finally {
            bufferedReader.close();
        }
    }



}

