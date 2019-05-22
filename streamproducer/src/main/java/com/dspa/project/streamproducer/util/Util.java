package com.dspa.project.streamproducer.util;

import java.io.File;
import java.io.FileNotFoundException;

public class Util {

    public static void handleFileNotFoundException(File csvFile){
        if (!csvFile.exists()) {
            try {
                throw new FileNotFoundException("File not found");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
