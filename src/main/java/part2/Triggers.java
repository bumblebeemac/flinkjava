package part2;

import com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import shopping.ShoppingCartEvent;
import shopping.ShoppingCartEventsGenerator;

public class Triggers {

    // Triggers -> WHEN a window function is executed
    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // copied from TimeBasedWindowTransformations
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

    public static void demoFirstTrigger() throws Exception {
        DataStream<String> shoppingCarEvents = env
                .addSource(new ShoppingCartEventsGenerator(500, 2)) // 2 events per second
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events per window
                .trigger(CountTrigger.of(5)) // the window function runs every 5 elements
                .process(new CountByWindowV2()); // runs twice for the same window

        shoppingCarEvents.print();
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        demoFirstTrigger();
    }

}
