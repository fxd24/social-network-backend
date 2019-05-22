package flink;

import com.dspa.project.model.PostEventStream;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class PostEventStreamTimestampAssigner implements AssignerWithPunctuatedWatermarks<PostEventStream> {

    @Override
    public long extractTimestamp(PostEventStream element, long previousElementTimestamp) {
        return element.getSentAt().getTime();
    }

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(PostEventStream lastElement, long extractedTimestamp) {
        return new Watermark(extractedTimestamp - 15);
    }
}