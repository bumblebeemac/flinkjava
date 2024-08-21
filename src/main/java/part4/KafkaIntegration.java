package part4;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaIntegration {

    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // read simple data (strings) from a Kafka topic
    public static void readStrings() throws Exception {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("iro-test-topic")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStrings = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // use the DataStream
        kafkaStrings.print();
        env.execute();
    }

    // read custom data
    public static void readIroPayload() throws Exception {
        KafkaSource<RtPayload> iroSource = KafkaSource.<RtPayload>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("iro-test-topic")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new PayloadDeserializationSchema())
                .build();

        DataStream<RtPayload> iroDataStream = env.fromSource(iroSource, WatermarkStrategy.noWatermarks(), "IRO Source");

        iroDataStream.print();
        env.execute();
    }

    // write custom data
    public static void writeCustomData() throws Exception {
        KafkaSink<RtPayload> kafkaSink = KafkaSink.<RtPayload>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("iro-topic-delete")
                        .setValueSerializationSchema(new PayloadSerializationSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSource<RtPayload> iroSource = KafkaSource.<RtPayload>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("iro-test-topic")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new PayloadDeserializationSchema())
                .build();

        DataStream<RtPayload> iroDataStream = env.fromSource(iroSource, WatermarkStrategy.noWatermarks(), "IRO Source");

        iroDataStream.sinkTo(kafkaSink);
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        writeCustomData();
    }
}
