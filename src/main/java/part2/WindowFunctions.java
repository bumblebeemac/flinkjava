package part2;

import com.google.common.collect.Iterables;
import gaming.Player;
import gaming.PlayerRegistered;
import gaming.ServerEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class WindowFunctions {

    // use-case : stream of events for a gaming session
    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    static Player alice = new Player(UUID.randomUUID(), "Alice");
    static Player bob = new Player(UUID.randomUUID(), "Bob");
    static Player charlie = new Player(UUID.randomUUID(), "Charlie");
    static Player diana = new Player(UUID.randomUUID(), "Diana");
    static Player emily = new Player(UUID.randomUUID(), "Emily");
    static Player fred = new Player(UUID.randomUUID(), "Fred");

    static Instant serverStartTime = Instant.parse("2023-11-06T00:00:00.000Z");

    static List<ServerEvent> events = List.of(
            bob.register(serverStartTime, Duration.ofSeconds(2)),
            bob.online(serverStartTime, Duration.ofSeconds(2)),
            diana.register(serverStartTime, Duration.ofSeconds(3)),
            diana.online(serverStartTime, Duration.ofSeconds(4)),
            emily.register(serverStartTime, Duration.ofSeconds(4)),
            alice.register(serverStartTime, Duration.ofSeconds(4)),
            fred.register(serverStartTime, Duration.ofSeconds(6)),
            bob.offline(serverStartTime, Duration.ofSeconds(6)),
            fred.online(serverStartTime, Duration.ofSeconds(6)),
            diana.offline(serverStartTime, Duration.ofSeconds(7)),
            charlie.register(serverStartTime, Duration.ofSeconds(8)),
            fred.offline(serverStartTime, Duration.ofSeconds(9)),
            alice.online(serverStartTime, Duration.ofSeconds(10)),
            emily.online(serverStartTime, Duration.ofSeconds(10)),
            charlie.online(serverStartTime, Duration.ofSeconds(10)),
            emily.offline(serverStartTime, Duration.ofSeconds(11)),
            charlie.offline(serverStartTime, Duration.ofSeconds(12)),
            alice.offline(serverStartTime, Duration.ofSeconds(12))
    );

    static DataStream<ServerEvent> eventStream = env.fromCollection(events, TypeInformation.of(ServerEvent.class))
            .assignTimestampsAndWatermarks(WatermarkStrategy.<ServerEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner(new SerializableTimestampAssigner<ServerEvent>() {
                        @Override
                        public long extractTimestamp(ServerEvent element, long recordTimestamp) {
                            return element.getEventTime().toEpochMilli();
                        }
                    })
            );

    // how many players were registered every 3 seconds
    /*
      |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
      |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    |              |
      |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
      |         |         |                 |                 | alice registered  |       |                 |       |                 |     | carl online   |              |
      ^|------------ window one ----------- + -------------- window two ----------------- + ------------- window three -------------- + ----------- window four ----------|^
      |                                     |                                             |                                           |                                    |
      |            1 registrations          |               3 registrations               |              2 registration               |            0 registrations         |
      |     1643760000000 - 1643760003000   |        1643760005000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |
   */

    // count by windowAll
    static class CountByWindowAll implements AllWindowFunction<ServerEvent, String, TimeWindow> {
    //                                                        ^ input      ^ output ^ window type
        @Override
        public void apply(TimeWindow timeWindow, Iterable<ServerEvent> iterable, Collector<String> collector) throws Exception {
            int eventCount = 0;
            for (ServerEvent event : iterable) if (event instanceof PlayerRegistered)
                eventCount += 1;

            collector.collect(
                "[" + timeWindow.getStart() + " - " + timeWindow.getEnd() + "] - " + eventCount + " players registered"
            );
        }
    }

    public static void demoCountByWindow() throws Exception {
        AllWindowedStream<ServerEvent, TimeWindow> threeSecondsTumblingWindow = eventStream.windowAll(
                TumblingEventTimeWindows.of(Time.seconds(3))
        );

        DataStream<String> registrationEvery3Secs = threeSecondsTumblingWindow.apply(new CountByWindowAll());

        registrationEvery3Secs.print();
        env.execute();
    }

    static class CountByWindowV2 extends ProcessAllWindowFunction<ServerEvent, String, TimeWindow> {
        @Override
        public void process(ProcessAllWindowFunction<ServerEvent, String, TimeWindow>.Context context, Iterable<ServerEvent> iterable, Collector<String> collector) throws Exception {
            // Get time window from context
            TimeWindow window = context.window();

            int eventCount = 0;
            for (ServerEvent event : iterable) if (event instanceof PlayerRegistered)
                eventCount += 1;

            collector.collect(
                    "[" + window.getStart() + " - " + window.getEnd() + "] - " + eventCount + " players registered"
            );
        }
    }

    public static void demoCountByWindowV2() throws Exception {
        AllWindowedStream<ServerEvent, TimeWindow> threeSecondsTumblingWindow = eventStream.windowAll(
                TumblingEventTimeWindows.of(Time.seconds(3))
        );

        DataStream<String> registrationEvery3Secs = threeSecondsTumblingWindow.process(new CountByWindowV2());

        registrationEvery3Secs.print();
        env.execute();
    }

    // alternative #2: aggregate function
    static class CountByWindowV3 implements AggregateFunction<ServerEvent, Long, Long> {
        //                                                    ^input       ^acc  ^output
        // start counting at 0
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        // every element increases accumulator by 1
        @Override
        public Long add(ServerEvent value, Long accumulator) {
            if (value instanceof PlayerRegistered)
                return accumulator + 1;
            return accumulator;
        }

        // push a final output out of the final accumulator
        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        // accum1 + accum2 = a bigger accumulator
        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static void demoCountByWindowV3() throws Exception {
        AllWindowedStream<ServerEvent, TimeWindow> threeSecondsTumblingWindow = eventStream.windowAll(
                TumblingEventTimeWindows.of(Time.seconds(3))
        );

        DataStream<Long> registrationEvery3Secs = threeSecondsTumblingWindow.aggregate(new CountByWindowV3());

        registrationEvery3Secs.print();
        env.execute();
    }

    /**
     * Keyed streams and window functions
     */

    // each element will be assigned to a "mini-stream" for its own key
    static KeyedStream<ServerEvent, String> streamByType = eventStream.keyBy(e -> e.getClass().getSimpleName());

    // for every key, we'll have a separate window allocation
    static WindowedStream<ServerEvent, String, TimeWindow> threeSecondsTumblingWindowsByType = streamByType
            .window(TumblingEventTimeWindows.of(Time.seconds(3)));

    static class CountInWindow implements WindowFunction<ServerEvent, String, String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<ServerEvent> iterable, Collector<String> collector) throws Exception {
            collector.collect(s + ": " + timeWindow + ", " + Iterables.size(iterable));
        }

    }

    public static void demoCountByTypeByWindow() throws Exception {
        DataStream<String> finalStream = threeSecondsTumblingWindowsByType.apply(new CountInWindow());
        finalStream.print();
        env.execute();
    }

    // alternative: process function for windows
    static class CountInWindowV2 extends ProcessWindowFunction<ServerEvent, String, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<ServerEvent, String, String, TimeWindow>.Context context, Iterable<ServerEvent> iterable, Collector<String> collector) throws Exception {
            collector.collect(s + ": " + context.window() + ", " + Iterables.size(iterable));
        }
    }

    public static void demoCountByTypeByWindowV2() throws Exception {
        DataStream<String> finalStream = threeSecondsTumblingWindowsByType.process(new CountInWindowV2());
        finalStream.print();
        env.execute();
    }

    // one task processes all the data for a particular key

    /**
     * Sliding windows
     */

    // how many players were registered every 3 seconds, UPDATED EVERY 1s?
    // [0..3s] [1..4s] [2..5s]

    public static void demoSlidingAllWindows() throws Exception {
        Time windowSize = Time.seconds(3);
        Time slidingTime = Time.seconds(1);

        AllWindowedStream<ServerEvent, TimeWindow> slidingWindowsAll = eventStream
                .windowAll(SlidingEventTimeWindows.of(windowSize, slidingTime));

        // process the windowed streams with similar window function
        DataStream<String> registrationCountByWindow = slidingWindowsAll.apply(new CountByWindowAll());
        registrationCountByWindow.print();
        env.execute();
    }

    /**
     * Session windows = group of events with NO MORE THAN a certain time gap in between them
     */
    // how many registration events do we have no more than 1 second apart
    public static void demoSessionWindows() throws Exception {
        AllWindowedStream<ServerEvent, TimeWindow> groupBySessionWindows = eventStream
                .windowAll(EventTimeSessionWindows.withGap(Time.seconds(1)));

        DataStream<String> countBySessionWindows = groupBySessionWindows.apply(new CountByWindowAll());
        countBySessionWindows.print();
        env.execute();
    }

    /**
     * Global window
     */
    // how many registration events do we have every 10 events

    static class CountByGlobalWindow implements AllWindowFunction<ServerEvent, String, GlobalWindow> {
        //                                                            ^ input      ^ output  ^ window type
        @Override
        public void apply(GlobalWindow window, Iterable<ServerEvent> values, Collector<String> out) throws Exception {
            int eventCount = 0;
            for (ServerEvent e : values) if (e instanceof PlayerRegistered)
                eventCount += 1;

            out.collect(
                    eventCount + " players registered"
            );
        }
    }

    public static void demoGlobalWindow() throws Exception {
        DataStream<String> globalWindowEvents = eventStream.windowAll(GlobalWindows.create())
                .trigger(CountTrigger.of(10L))
                .apply(new CountByGlobalWindow());

        globalWindowEvents.print();
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        demoGlobalWindow();
    }

}
