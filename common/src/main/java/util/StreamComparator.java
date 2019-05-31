package util;

import com.dspa.project.model.CommentEventStream;
import com.dspa.project.model.LikesEventStream;
import com.dspa.project.model.PostEventStream;
import com.dspa.project.model.Stream;

import java.text.SimpleDateFormat;
import java.util.Comparator;

public class StreamComparator implements Comparator<Stream> {
    @Override
    public int compare(Stream o1, Stream o2) {
        SimpleDateFormat sdf1, sdf2;
        if((o1 instanceof CommentEventStream || o1 instanceof PostEventStream) && (o2 instanceof CommentEventStream || o2 instanceof PostEventStream)){

             sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
             sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        }else if ((o1 instanceof CommentEventStream || o1 instanceof PostEventStream) && (o2 instanceof LikesEventStream)){
             sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
             sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        }else if((o1 instanceof LikesEventStream) && (o2 instanceof CommentEventStream || o2 instanceof PostEventStream)){
             sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
             sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        }else{
             sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
             sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        }

        return 0;
    }
}

