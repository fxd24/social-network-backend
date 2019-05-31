package flink;

import com.dspa.project.model.LikesEventStream;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class LikesEventStreamTimestampAssigner implements AssignerWithPunctuatedWatermarks<LikesEventStream> {

    @Override
    public long extractTimestamp(LikesEventStream element, long previousElementTimestamp) {
        return element.getSentAt().getTime();
    }

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(LikesEventStream lastElement, long extractedTimestamp) {
        return new Watermark(extractedTimestamp - 300000);
    }
}