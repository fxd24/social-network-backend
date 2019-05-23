package util;

import com.dspa.project.model.LikesEventStream;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;

public class LikesEventStreamComparator implements Comparator<LikesEventStream> {

    @Override
    public int compare(LikesEventStream o1, LikesEventStream o2) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

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
