package part2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class EssentialStreams {

    public static void applicationTemplate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> numbers = env.fromElements(1,2,3,4,5);
        numbers.print();
        env.execute();
    }

    // Transformations
    public static void demoTransformations() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> numbers = env.fromSequence(1, 100);

        // checking parallelism
        System.out.println("Current parallelism: " + env.getParallelism());
        // set different parallelism
        env.setParallelism(2);
        System.out.println("New parallelism: " + env.getParallelism());
        // map
        DataStream<Long> doubleNumbers = numbers.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {
                return aLong * 2;
            }
        });

        // flatMap
        // Java Lambda function as opposed to traditional declaration above
        DataStream<Long> expandedNumbers = numbers.flatMap(new FlatMapFunction<Long, Long>() {
            @Override
            public void flatMap(Long aLong, Collector<Long> collector) throws Exception {
                // you can push as many items as you want
                collector.collect(aLong);
                collector.collect(aLong * 1000);
            }
        });

        // filter
        DataStream<Long> filteredNumbers = numbers.filter(e -> e * 2 == 0).setParallelism(4); /*you can set parallelism here*/

        // consume the DSs as you like
        DataStreamSink<Long> finalData = expandedNumbers.sinkTo(
                FileSink.forRowFormat(
                        new Path("output/streaming_sink"),
                        new SimpleStringEncoder<Long>("UTF-8")
                ).build()
        );
        // set parallelism in the sink
        finalData.setParallelism(3);

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        demoTransformations();
    }

}
