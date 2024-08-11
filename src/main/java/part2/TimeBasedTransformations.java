package part2;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import shopping.ShoppingCartEvent;
import shopping.ShoppingCartEventsGenerator;

import java.time.Duration;
import java.time.Instant;

public class TimeBasedTransformations {

    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    static DataStream<ShoppingCartEvent> shoppingcartEvents = env.addSource(new ShoppingCartEventsGenerator(
            100, 5, Instant.parse("2024-02-15T00:00:00.000Z")));

    // 1. Event time = the moment event was created
    // 2. Processing time = the event ARRIVES AT FLINK

    /*
      Group by window, every 3s, tumbling (non-overlapping), PROCESSING TIME
     */
    /*
        With processing time
        - we don't care when the event was created
        - multiple runs generate different results
     */
    static class CountByWindowV2 extends ProcessAllWindowFunction<ShoppingCartEvent, String, TimeWindow> {
        @Override
        public void process(ProcessAllWindowFunction<ShoppingCartEvent, String, TimeWindow>.Context context, Iterable<ShoppingCartEvent> iterable, Collector<String> collector) throws Exception {
            // Get time window from context
            TimeWindow window = context.window();

            collector.collect(
                "Window [" + window.getStart() + " - " + window.getEnd() + "] - " + Iterables.size(iterable)
            );
        }
    }

    public static void demoProcessingTime() throws Exception {

        AllWindowedStream<ShoppingCartEvent, TimeWindow> groupedEventsByWindow = shoppingcartEvents
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)));

        DataStream<String> countEventsByWindow = groupedEventsByWindow.process(new CountByWindowV2());
        countEventsByWindow.print();
        env.execute();
    }

    public static void demoEventTime() throws Exception {
        AllWindowedStream<ShoppingCartEvent, TimeWindow> groupedEventsByWindow = shoppingcartEvents.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ShoppingCartEvent>forBoundedOutOfOrderness(Duration.ofMillis(500))
                    .withTimestampAssigner(new SerializableTimestampAssigner<ShoppingCartEvent>() {
                        @Override
                        public long extractTimestamp(ShoppingCartEvent element, long recordTimestamp) {
                            return element.getTime().toEpochMilli();
                        }
                    })
        ).windowAll(TumblingEventTimeWindows.of(Time.seconds(3)));

        DataStream<String> countEventsByWindow = groupedEventsByWindow.process(new CountByWindowV2());
        countEventsByWindow.print();
        env.execute();
    }

    // custom watermarks
    static class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<ShoppingCartEvent> {
        private long maxDelay;
        private long currentMaxTimestamp = 0L;

        public BoundedOutOfOrdernessGenerator(long maxDelay) {
            this.maxDelay = maxDelay;
        }

        @Override
        public void onEvent(ShoppingCartEvent event, long eventTimestamp, WatermarkOutput output) {
            if (event.getTime().toEpochMilli() > currentMaxTimestamp) {
                currentMaxTimestamp = event.getTime().toEpochMilli();
                output.emitWatermark(new Watermark(event.getTime().toEpochMilli() - maxDelay));
            }
            // we may or may not emit watermarks
        }

        // 200ms
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(currentMaxTimestamp - maxDelay - 1));
        }
    }

    public static void demoEventTimeCustom() throws Exception {
        //  control how often Flink calls onPeriodicEmit
        env.getConfig().setAutoWatermarkInterval(100L); // call onPeriodicEmit every 100 milliseconds

        DataStream<ShoppingCartEvent> shoppingCartEventsET = shoppingcartEvents
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartEvent>forGenerator(ctx ->
                                new BoundedOutOfOrdernessGenerator(500)
                        ).withTimestampAssigner(
                                new SerializableTimestampAssigner<ShoppingCartEvent>() {
                                    @Override
                                    public long extractTimestamp(ShoppingCartEvent element, long recordTimestamp) {
                                        return element.getTime().toEpochMilli();
                                    }
                                }
                        )
                );

        shoppingCartEventsET.print();
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        demoEventTimeCustom();
    }
}
