package com.dspa.project.common.deserialization;

import com.dspa.project.model.CommentEventStream;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class CommentStreamDeserializationSchema implements DeserializationSchema<CommentEventStream> {

    static ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);


    @Override
    public CommentEventStream deserialize(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, CommentEventStream.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    //{"id":1786380,"personId":437,"creationDate":"2012-03-11T09:00:13Z","locationIP":"27.112.104.5","browserUsed":"Firefox","content":"About Sigmund Freud, are invested), developed therapeutic techniques such as the use of free association (in which patients report their thoughts without.","reply_to_postId":-1,"reply_to_commentId":-1,"placeId":-1}
    @Override
    public boolean isEndOfStream(CommentEventStream comment) {
        return false;
    }

    @Override
    public TypeInformation<CommentEventStream> getProducedType() {
        return TypeInformation.of(CommentEventStream.class);
    }
}
