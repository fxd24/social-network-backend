package flink;


import com.dspa.project.model.CommentEventStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;


public class CommentEventStreamTimestampAssigner extends BoundedOutOfOrdernessTimestampExtractor<CommentEventStream> {
    public CommentEventStreamTimestampAssigner(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(CommentEventStream commentEventStream) {
        //System.out.println("Time of commentEventStream element is: "+commentEventStream.getSentAt().getTime());
        //System.out.println((new Date(this.getCurrentWatermark().getTimestamp()).toString()));
        return commentEventStream.getSentAt().getTime();
    }
}
