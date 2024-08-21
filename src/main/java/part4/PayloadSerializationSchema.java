package part4;

import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.charset.StandardCharsets;

public class PayloadSerializationSchema implements SerializationSchema<RtPayload> {

    @Override
    public byte[] serialize(RtPayload element) {
        return element.toString().getBytes(StandardCharsets.UTF_8);
    }
}
