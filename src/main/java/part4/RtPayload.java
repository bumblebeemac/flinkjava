package part4;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class RtPayload {
    RtPayloadHeader headers;
    String data;
    String kafka_timestamp;

    // Constructor
    public RtPayload(
            @JsonProperty("headers")
            RtPayloadHeader headers,
            @JsonProperty("data")
            String data,
            @JsonProperty("kafka_timestamp")
            String kafka_timestamp) {
        this.headers = headers;
        this.data = data;
        this.kafka_timestamp = kafka_timestamp;
    }

    @Override
    public String toString() {
        return "RtPayload{" +
                "headers=" + headers +
                ", data='" + data + '\'' +
                ", kafka_timestamp='" + kafka_timestamp + '\'' +
                '}';
    }
}
