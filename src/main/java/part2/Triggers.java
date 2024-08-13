package part2;

import com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
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

    public static void demoCountTrigger() throws Exception {
        DataStream<String> shoppingCarEvents = env
                .addSource(new ShoppingCartEventsGenerator(500, 2)) // 2 events per second
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events per window
                .trigger(CountTrigger.of(5)) // the window function runs every 5 elements
                .process(new CountByWindowV2()); // runs twice for the same window

        shoppingCarEvents.print();
        env.execute();
    }

    // purging trigger - clear the window when it fires
    public static void demoPurgingTrigger() throws Exception {
        DataStream<String> shoppingCarEvents = env
                .addSource(new ShoppingCartEventsGenerator(500, 2)) // 2 events per second
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events per window
                .trigger(PurgingTrigger.of(CountTrigger.of(5))) // the window function runs every 5 elements
                .process(new CountByWindowV2()); // runs twice for the same window

        shoppingCarEvents.print();
        env.execute();
    }

    /*
      Other triggers:
      - EventTimeTrigger - happens by default when watermark is larger >  window end time (automatic for event time windows)
      - ProcessingTimeTrigger - fires when the current system time > window end time (automatic for processing time windows)
      - CustomTriggers - powerful APIs for custom firing behavior
     */

    public static void main(String[] args) throws Exception {
        demoPurgingTrigger();
    }

}
