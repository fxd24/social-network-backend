package flink;


import com.dspa.project.model.CommentEventStream;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;


public class CommentEventStreamTimestampAssigner implements AssignerWithPunctuatedWatermarks<CommentEventStream> {

    @Override
    public long extractTimestamp(CommentEventStream element, long previousElementTimestamp) {
        return element.getSentAt().getTime();
    }

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(CommentEventStream lastElement, long extractedTimestamp) {
        return new Watermark(extractedTimestamp - 300000); //TODO: this has to be equal to the random amount of delay a message can have
    }
}
