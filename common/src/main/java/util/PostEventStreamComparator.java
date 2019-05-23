package util;

import com.dspa.project.model.PostEventStream;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;

public class PostEventStreamComparator implements Comparator<PostEventStream> {
    @Override
    public int compare(PostEventStream o1, PostEventStream o2) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        try {
            Long l1 = sdf.parse(o1.getCreationDate()).getTime();
            Long l2 = sdf.parse(o2.getCreationDate()).getTime();
            return l1.compareTo(l2);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
