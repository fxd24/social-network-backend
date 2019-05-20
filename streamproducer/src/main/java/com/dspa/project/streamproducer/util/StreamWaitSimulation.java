package com.dspa.project.streamproducer.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class StreamWaitSimulation {


    //@Value(value = "${speedup.factor}") //TODO: doesn't take the value
    private int speedup_factor = 10000; //TODO: automate the speedup factor


    public void wait(String last_timestamp, String new_timestamp){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        Date t_last = null;
        Date t_new = null;
        try {
            System.out.println("Class:StreamWaitSimulation. Function:wait {last_timestamp:"+last_timestamp+"},{new_timestamp:"+new_timestamp+"}"); //TODO: remove when tested
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
            System.out.println("Waiting for "+ waiting_time+" milliseconds"); //TODO: remove when tested
            TimeUnit.MILLISECONDS.sleep(waiting_time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    public void randomSleep() { //TODO: add more properties in the randomness range
        Random random = new Random();
        long time =  (long) random.nextInt(300000); //this are 5 minutes max delay
        System.out.println("Random time choosen is "+time+" |"); //TODO:remove eventually
        try {
            TimeUnit.MILLISECONDS.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}

