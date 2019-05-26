package flink;

import com.dspa.project.model.CommentEventStream;
import com.dspa.project.model.LikesEventStream;
import com.dspa.project.model.PostEventStream;
import com.dspa.project.model.Stream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamTimestampAssigner extends BoundedOutOfOrdernessTimestampExtractor<Stream> {
    public StreamTimestampAssigner(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Stream stream) {
        long result = 0;
        if(stream instanceof CommentEventStream){
            CommentEventStream tmp = (CommentEventStream) stream;
            result = tmp.getSentAt().getTime();
        } else if(stream instanceof LikesEventStream){
            LikesEventStream tmp = (LikesEventStream) stream;
            result = tmp.getSentAt().getTime();
        } else {
            PostEventStream tmp = (PostEventStream) stream;
            result = tmp.getSentAt().getTime();
        }
        return result;
    }

}
