package part4;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class PayloadDeserializationSchema implements DeserializationSchema<RtPayload>  {

    private static final long serialVersionUID = 1L;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public RtPayload deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, RtPayload.class);
    }

    @Override
    public boolean isEndOfStream(RtPayload nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RtPayload> getProducedType() {
        return TypeInformation.of(RtPayload.class);
    }

}
