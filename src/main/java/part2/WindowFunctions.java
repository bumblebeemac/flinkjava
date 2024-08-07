package part2;

import gaming.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
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
            .assignTimestampsAndWatermarks( // extract timestamps for events (event time) + watermarks
                WatermarkStrategy<ServerEvent>.forBoundedOutOfOrderness(
                        Duration.ofMillis(500) // once you get an event with times T, you will NOT accept further events with time < T - 500
                )
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


    public static void main(String[] args) throws Exception {
        demoCountByWindow();
    }

}
