package com.dspa.project.common.deserialization;

import com.dspa.project.model.LikesEventStream;
import com.dspa.project.model.Stream;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class LikesEventStreamDeserializationSchema implements DeserializationSchema<Stream> {

    static ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);


    @Override
    public Stream deserialize(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, LikesEventStream.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Stream comment) {
        return false;
    }

    @Override
    public TypeInformation<Stream> getProducedType() {
        return TypeInformation.of(Stream.class);
    }
}
