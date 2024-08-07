package part2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.stream.LongStream;

public class ExplicitTransformations {

    // Explicit transformation
    public static void demoExplicitTransformations() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> numbers = env.fromSequence(1, 100);

        // map
        DataStream<Long> doubleNumbers = numbers.map((MapFunction<Long, Long>) aLong -> aLong * 2);

        // flatMap
        DataStream<Long> expandedNumbers = numbers.flatMap(new FlatMapFunction<Long, Long>() {
            @Override
            public void flatMap(Long aLong, Collector<Long> collector) throws Exception {
                LongStream.rangeClosed(1, aLong).forEach(collector::collect);
//                collector.collect(aLong);
            }
        });

        // Process method
        // Process Function is THE MOST GENERAL function to process elements in Flink
        DataStream<Long> expandedNumbers_v2 = numbers.process(new ProcessFunction<Long, Long>() {
            @Override
            public void processElement(Long aLong, ProcessFunction<Long, Long>.Context context, Collector<Long> collector) throws Exception {
                LongStream.rangeClosed(1, aLong).forEach(collector::collect);
            }
        });

        // Reduce
        // happens on keyed streams
        // Type signature: For every Long attach a Boolean as key
        /*
            [1, false
             2, true

             100, true]

             true => 2, 6, 12, 20, ...
             false => 1, 4, 9, 16, ...
         */
        KeyedStream<Long, Boolean> keyedNumbers = numbers.keyBy(n -> n % 2 == 0);
        SingleOutputStreamOperator<Long> sumByKey = keyedNumbers.reduce(Long::sum); // sum up all the elements by KEY

        sumByKey.print();

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        demoExplicitTransformations();
    }
}
