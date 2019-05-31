package com.dspa.project.streamproducer.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class StreamWaitSimulation {

    private long speedup_factor = 10000L;


    public void wait(String last_timestamp, String new_timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        Date t_last = null;
        Date t_new = null;
        try {
            //System.out.println("Class:StreamWaitSimulation. Function:wait {last_timestamp:" + last_timestamp + "},{new_timestamp:" + new_timestamp + "}");
            t_last = sdf.parse(last_timestamp);
            t_new = sdf.parse(new_timestamp);
        } catch (ParseException e) {
            t_last = new Date(0);
            t_new = new Date(100);
            //e.printStackTrace();
        }
        long waiting_time = 0;
        long diff = t_new.getTime() - t_last.getTime();
        waiting_time = diff / speedup_factor;
        try {
            //System.out.println("Waiting for " + waiting_time + " milliseconds");
            TimeUnit.MILLISECONDS.sleep(waiting_time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    public void wait(Long last_timestamp, Long new_timestamp) {

        Long waiting_time = 0L;
        Long diff = last_timestamp - new_timestamp;
        waiting_time = diff / speedup_factor;
        try {
            //System.out.println("Waiting for " + waiting_time + " milliseconds");
            TimeUnit.MILLISECONDS.sleep(waiting_time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    public Long randomDelayNumber() {
        Random random = new Random();
        return (long) random.nextInt(300000); //this are 5 minutes max delay


    }

    public Long maybeRandomDelayNumber(){
        Random random = new Random();
        int randomInt = random.nextInt(10);
        //sleeps for a random amount of time when randomInt is 1
        if(randomInt==1){
            return randomDelayNumber();
        }
        return 0L;

    }
}

