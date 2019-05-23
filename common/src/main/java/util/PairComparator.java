package util;

import com.dspa.project.model.Stream;
import javafx.util.Pair;

import java.util.Comparator;

public class PairComparator implements Comparator<Pair<Long, Stream>> {
    @Override
    public int compare(Pair<Long, Stream> o1, Pair<Long, Stream> o2) {
        return o1.getKey().compareTo(o2.getKey());
    }
}
